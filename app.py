"""
Telegram Channel Collector: multi-user Streamlit app.

Each browser session gets its own Telethon client (in-memory StringSession),
its own settings, and its own results. All Telethon work runs on one shared
background asyncio loop, so Streamlit reruns never touch the connection and
concurrent users never share state.
"""

import asyncio
import io
import re
import threading
import time
import uuid
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.sessions import StringSession

MAX_CONCURRENT_JOBS = 20          # collections allowed to run at once
IDLE_DISCONNECT_SECONDS = 2 * 3600
FLOOD_SLEEP_THRESHOLD = 120       # Telethon auto-sleeps flood waits up to this
LOG_LINES_SHOWN = 15


# ═══════════════════════════════════════════════════════════════════
# Shared background event loop (one per server process)
# ═══════════════════════════════════════════════════════════════════
class LoopRunner:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        self.slots = self.run(self._make_semaphore())
        self.registry = {}   # session key -> {"client", "last_seen", "busy"}
        self.lock = threading.Lock()
        self.submit(self._reaper())

    async def _make_semaphore(self):
        return asyncio.Semaphore(MAX_CONCURRENT_JOBS)

    def run(self, coro, timeout=120):
        """Run a coroutine on the loop and wait for the result."""
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def submit(self, coro):
        """Start a coroutine on the loop; returns a concurrent.futures.Future."""
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def register(self, key, client):
        with self.lock:
            self.registry[key] = {"client": client, "last_seen": time.time(), "busy": False}

    def touch(self, key, busy=None):
        with self.lock:
            entry = self.registry.get(key)
            if entry:
                entry["last_seen"] = time.time()
                if busy is not None:
                    entry["busy"] = busy

    def unregister(self, key):
        with self.lock:
            entry = self.registry.pop(key, None)
        if entry:
            self.submit(entry["client"].disconnect())

    async def _reaper(self):
        """Disconnect clients whose browser sessions went away."""
        while True:
            await asyncio.sleep(300)
            cutoff = time.time() - IDLE_DISCONNECT_SECONDS
            with self.lock:
                stale = [k for k, v in self.registry.items()
                         if v["last_seen"] < cutoff and not v["busy"]]
                clients = [self.registry.pop(k)["client"] for k in stale]
            for c in clients:
                try:
                    await c.disconnect()
                except Exception:
                    pass


@st.cache_resource
def get_runner():
    return LoopRunner()


# ═══════════════════════════════════════════════════════════════════
# Telethon helpers (all run on the background loop)
# ═══════════════════════════════════════════════════════════════════
async def new_client(api_id, api_hash, session_str=""):
    client = TelegramClient(
        StringSession(session_str), int(api_id), api_hash,
        device_model="Channel Collector",
        flood_sleep_threshold=FLOOD_SLEEP_THRESHOLD,
    )
    await client.connect()
    return client


async def describe_me(client):
    me = await client.get_me()
    handle = f"@{me.username}" if me.username else (me.first_name or str(me.id))
    return handle


def clean_channel(raw):
    raw = raw.strip()
    raw = re.sub(r"^https?://(t|telegram)\.me/(s/)?", "", raw, flags=re.I)
    return raw.lstrip("@").split("/")[0].split("?")[0]


def parse_lines(text):
    seen, out = set(), []
    for line in text.replace(",", "\n").splitlines():
        item = line.strip()
        if item and item.lower() not in seen:
            seen.add(item.lower())
            out.append(item)
    return out


async def with_flood_retry(coro_fn, log):
    while True:
        try:
            return await coro_fn()
        except FloodWaitError as e:
            log(f"  rate limited, waiting {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)


async def iter_messages_retry(client, entity, log, **kwargs):
    """iter_messages that resumes after a flood wait instead of abandoning the channel."""
    offset_id = 0
    while True:
        try:
            async for m in client.iter_messages(entity, offset_id=offset_id, **kwargs):
                offset_id = m.id
                yield m
            return
        except FloodWaitError as e:
            log(f"  rate limited, waiting {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)


async def collect(runner, key, client, cfg, progress):
    def log(line):
        progress["log"].append(f"{datetime.now():%H:%M:%S} {line}")

    runner.touch(key, busy=True)
    try:
        if runner.slots.locked():
            log("Waiting for a free collection slot...")
        async with runner.slots:
            return await _collect(runner, key, client, cfg, progress, log)
    finally:
        runner.touch(key, busy=False)


async def _collect(runner, key, client, cfg, progress, log):
    start, end = cfg["start"], cfg["end"]
    search_terms = cfg["search_terms"]
    scan_terms = [t.lower() for t in cfg["scan_terms"]]
    text_limit = cfg["text_limit"]
    all_terms_lower = [t.lower() for t in search_terms]

    messages, scan_hits = [], []
    fwd_cache = {}

    async def resolve_forward(msg):
        fwd = msg.forward
        if fwd.from_name:
            return fwd.from_name
        cache_key = str(fwd.from_id)
        if cache_key in fwd_cache:
            return fwd_cache[cache_key]
        ent = fwd.chat or fwd.sender
        if ent is None:
            for getter in (fwd.get_chat, fwd.get_sender):
                try:
                    ent = await getter()
                except Exception:
                    ent = None
                if ent is not None:
                    break
        if ent is not None:
            uname = getattr(ent, "username", None)
            name = f"@{uname}" if uname else (getattr(ent, "title", None)
                                             or getattr(ent, "first_name", None))
        else:
            name = None
        name = name or f"[unresolved: {fwd.from_id}]"
        fwd_cache[cache_key] = name
        return name

    async def to_row(msg, channel, username, title, term):
        text = msg.text or ""
        lower = text.lower()
        is_fwd = msg.fwd_from is not None
        media_type = type(msg.media).__name__.replace("MessageMedia", "") if msg.media else ""
        return {
            "channel": channel,
            "channel_title": title,
            "message_id": msg.id,
            "link": f"https://t.me/{username}/{msg.id}" if username else "",
            "date": msg.date.isoformat(),
            "edit_date": msg.edit_date.isoformat() if msg.edit_date else None,
            "post_author": msg.post_author,
            "views": msg.views or 0,
            "forwards": msg.forwards or 0,
            "replies": msg.replies.replies if msg.replies else 0,
            "is_forward": is_fwd,
            "forwarded_from": await resolve_forward(msg) if is_fwd else None,
            "original_fwd_date": msg.fwd_from.date.isoformat()
                if is_fwd and msg.fwd_from.date else None,
            "search_term": term,
            "matched_keywords": ", ".join(k for k, kl in zip(search_terms, all_terms_lower)
                                          if kl in lower),
            "reply_to_msg_id": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
            "grouped_id": msg.grouped_id,
            "media_type": media_type,
            "text": text[:text_limit] if text_limit else text,
        }

    channels = cfg["channels"]
    progress["total"] = len(channels)

    for channel in channels:
        runner.touch(key, busy=True)
        log(f"Scanning {channel}")
        try:
            entity = await with_flood_retry(lambda: client.get_entity(channel), log)
        except Exception as e:
            log(f"  skipped: {type(e).__name__}: {e}")
            progress["skipped"].append(channel)
            progress["done"] += 1
            continue

        username = getattr(entity, "username", None)
        title = getattr(entity, "title", None) or channel
        seen_ids, count = set(), 0

        try:
            for term in search_terms:
                async for msg in iter_messages_retry(client, entity, log,
                                                     search=term, offset_date=end):
                    if msg.date < start:
                        break
                    if msg.date >= end or msg.id in seen_ids:
                        continue
                    seen_ids.add(msg.id)
                    messages.append(await to_row(msg, channel, username, title, term))
                    count += 1
                await asyncio.sleep(1)
            log(f"  {count} keyword matches")

            if scan_terms:
                hits = 0
                async for msg in iter_messages_retry(client, entity, log,
                                                     offset_date=end):
                    if msg.date < start:
                        break
                    if msg.date >= end:
                        continue
                    lower = (msg.text or "").lower()
                    matched = [t for t in cfg["scan_terms"] if t.lower() in lower]
                    if matched:
                        row = await to_row(msg, channel, username, title, None)
                        row["matched_keywords"] = ", ".join(matched)
                        scan_hits.append(row)
                        hits += 1
                log(f"  {hits} full-scan matches")
        except Exception as e:
            log(f"  error: {type(e).__name__}: {e}")
            progress["skipped"].append(channel)

        progress["done"] += 1
        await asyncio.sleep(2)

    log(f"Finished: {len(messages)} keyword matches, {len(scan_hits)} full-scan matches")
    return build_outputs(messages, scan_hits, cfg["min_cascade_chars"])


# ═══════════════════════════════════════════════════════════════════
# Analysis (pure functions, no Telegram calls)
# ═══════════════════════════════════════════════════════════════════
AT_RE = re.compile(r"(?<!\w)@([A-Za-z0-9_]{5,})")
TG_RE = re.compile(r"https?://(?:t\.me|telegram\.me)/(?:s/)?([A-Za-z0-9_]{5,})", re.I)
URL_RE = re.compile(r"(https?://[^\s<>\"]+)")


def normalize_text(text):
    return re.sub(r"\s+", " ", (text or "")[:200].lower().strip())


def build_outputs(messages, scan_hits, min_cascade_chars):
    df = pd.DataFrame(messages)
    out = {"messages": df, "full_scan_hits": pd.DataFrame(scan_hits)}

    if df.empty:
        for name in ("forwarding_network", "channel_mentions", "urls", "cascades"):
            out[name] = pd.DataFrame()
        return out

    out["forwarding_network"] = df[df["is_forward"]][
        ["channel", "forwarded_from", "date", "original_fwd_date", "views", "forwards", "link"]
    ].reset_index(drop=True)

    mentions, urls = [], []
    for m in messages:
        text = m["text"] or ""
        for handle in set(AT_RE.findall(text)) | set(TG_RE.findall(text)):
            mentions.append({"source_channel": m["channel"], "mentioned_channel": f"@{handle}",
                             "message_id": m["message_id"], "message_date": m["date"],
                             "link": m["link"], "text_snippet": text[:300]})
        for url in set(URL_RE.findall(text)):
            try:
                domain = urlparse(url).netloc
            except Exception:
                domain = "unknown"
            urls.append({"source_channel": m["channel"], "full_url": url, "domain": domain,
                         "message_id": m["message_id"], "message_date": m["date"],
                         "link": m["link"]})
    out["channel_mentions"] = pd.DataFrame(mentions)
    out["urls"] = pd.DataFrame(urls)

    groups = defaultdict(list)
    for m in messages:
        k = normalize_text(m["text"])
        if len(k) > min_cascade_chars:
            groups[k].append(m)

    cascades = []
    for msgs in groups.values():
        if len({m["channel"] for m in msgs}) < 2:
            continue
        msgs = sorted(msgs, key=lambda m: m["date"])
        cascades.append(msgs)
    cascades.sort(key=lambda ms: len({m["channel"] for m in ms}), reverse=True)

    rows = []
    for i, msgs in enumerate(cascades, start=1):
        first_dt = datetime.fromisoformat(msgs[0]["date"])
        n_channels = len({m["channel"] for m in msgs})
        for m in msgs:
            rows.append({
                "cascade_id": i,
                "channels_count": n_channels,
                "origin_channel": msgs[0]["channel"],
                "forwarded_from": msgs[0]["forwarded_from"],
                "channel": m["channel"],
                "date": m["date"],
                "delay_hours": round((datetime.fromisoformat(m["date"]) - first_dt)
                                     .total_seconds() / 3600, 2),
                "views": m["views"],
                "link": m["link"],
                "text_snippet": msgs[0]["text"][:300],
            })
    out["cascades"] = pd.DataFrame(rows)
    return out


def zip_outputs(outputs, prefix):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, df in outputs.items():
            if df.empty:
                continue
            if name == "messages":
                name = "keyword_posts"
                for channel, part in df.groupby("channel"):
                    zf.writestr(f"posts_by_channel/{prefix}_{channel}.csv",
                                part.to_csv(index=False))
            zf.writestr(f"{prefix}_{name}.csv", df.to_csv(index=False))
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════
# UI
# ═══════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Telegram Channel Collector", layout="wide")
runner = get_runner()
ss = st.session_state
ss.setdefault("key", uuid.uuid4().hex)
ss.setdefault("client", None)
ss.setdefault("pending", None)       # client mid-login
ss.setdefault("login_stage", "phone")
ss.setdefault("me", None)
ss.setdefault("job", None)
ss.setdefault("results", None)

if ss.client is not None:
    runner.touch(ss.key)


def disconnect():
    runner.unregister(ss.key)
    for k in ("client", "pending", "me", "job", "results", "session_string"):
        ss[k] = None
    ss.login_stage = "phone"


def finish_login(client):
    ss.client = client
    ss.pending = None
    ss.me = runner.run(describe_me(client))
    ss.session_string = client.session.save()
    runner.register(ss.key, client)


with st.sidebar:
    st.header("Connection")
    if ss.client:
        st.success(f"Connected as {ss.me}")
        st.download_button("Download session string", ss.session_string or "",
                           file_name="telegram_session.txt",
                           help="Paste this next time to skip the login code. "
                                "Anyone holding it has access to your account.")
        if st.button("Disconnect"):
            disconnect()
            st.rerun()
    else:
        st.info("Not connected")

st.title("Telegram Channel Collector")

# ── Step 1: connect ──────────────────────────────────────────────
if not ss.client:
    st.subheader("Connect your Telegram account")
    st.caption("Get an API ID and hash at my.telegram.org → API development tools. "
               "Credentials stay in this browser session's memory and are never written to disk.")
    c1, c2 = st.columns(2)
    api_id = c1.text_input("API ID")
    api_hash = c2.text_input("API hash", type="password")

    tab_phone, tab_session = st.tabs(["Log in with phone", "Use a saved session string"])

    with tab_phone:
        if ss.login_stage == "phone":
            phone = st.text_input("Phone number (international format, e.g. +61...)")
            if st.button("Send login code", disabled=not (api_id and api_hash and phone)):
                try:
                    client = runner.run(new_client(api_id, api_hash))
                    runner.run(client.send_code_request(phone))
                    ss.pending, ss.phone, ss.login_stage = client, phone, "code"
                    st.rerun()
                except Exception as e:
                    st.error(f"Could not send code: {type(e).__name__}: {e}")

        elif ss.login_stage == "code":
            st.write(f"Code sent to the Telegram app for {ss.phone}.")
            code = st.text_input("Login code")
            b1, b2 = st.columns([1, 5])
            if b1.button("Sign in", disabled=not code):
                try:
                    runner.run(ss.pending.sign_in(phone=ss.phone, code=code.strip()))
                    finish_login(ss.pending)
                    st.rerun()
                except SessionPasswordNeededError:
                    ss.login_stage = "password"
                    st.rerun()
                except Exception as e:
                    st.error(f"Sign-in failed: {type(e).__name__}: {e}")
            if b2.button("Start over"):
                if ss.pending:
                    runner.submit(ss.pending.disconnect())
                ss.pending, ss.login_stage = None, "phone"
                st.rerun()

        elif ss.login_stage == "password":
            pw = st.text_input("Two-step verification password", type="password")
            if st.button("Submit password", disabled=not pw):
                try:
                    runner.run(ss.pending.sign_in(password=pw))
                    finish_login(ss.pending)
                    st.rerun()
                except Exception as e:
                    st.error(f"Password rejected: {type(e).__name__}: {e}")

    with tab_session:
        sess = st.text_area("Session string", height=100)
        if st.button("Connect with session", disabled=not (api_id and api_hash and sess)):
            try:
                client = runner.run(new_client(api_id, api_hash, sess.strip()))
                if not runner.run(client.is_user_authorized()):
                    runner.submit(client.disconnect())
                    st.error("That session string is no longer authorized. Log in with your phone.")
                else:
                    finish_login(client)
                    st.rerun()
            except Exception as e:
                st.error(f"Could not connect: {type(e).__name__}: {e}")
    st.stop()

# ── Step 2: configure ────────────────────────────────────────────
st.subheader("Collection settings")
running = ss.job is not None
with st.form("config"):
    c1, c2 = st.columns(2)
    channels_raw = c1.text_area(
        "Target channels (one per line; @handle, handle, or t.me link)", height=220)
    search_raw = c2.text_area(
        "Search keywords (Telegram server-side search, one per line)", height=100)
    scan_raw = c2.text_area(
        "Full-scan keywords (optional; reads every message in range, much slower)",
        height=90)
    d1, d2, d3, d4 = st.columns(4)
    start_d = d1.date_input("Start date", value=date.today() - timedelta(days=30))
    end_d = d2.date_input("End date (inclusive)", value=date.today())
    text_limit = d3.number_input("Max characters per message (0 = full text)", 0, 20000, 0, step=500)
    min_cascade = d4.number_input("Min text length for cascade matching", 10, 200, 50)
    prefix = st.text_input("File name prefix", value="telegram")
    submitted = st.form_submit_button("Start collection", disabled=running)

if submitted:
    cfg = {
        "channels": [clean_channel(c) for c in parse_lines(channels_raw)],
        "search_terms": parse_lines(search_raw),
        "scan_terms": parse_lines(scan_raw),
        "start": datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc),
        "end": datetime.combine(end_d + timedelta(days=1), datetime.min.time(),
                                tzinfo=timezone.utc),
        "text_limit": int(text_limit),
        "min_cascade_chars": int(min_cascade),
    }
    problems = []
    if not cfg["channels"]:
        problems.append("Add at least one channel.")
    if not cfg["search_terms"] and not cfg["scan_terms"]:
        problems.append("Add at least one search or full-scan keyword.")
    if cfg["start"] >= cfg["end"]:
        problems.append("Start date must be on or before end date.")
    if problems:
        for p in problems:
            st.error(p)
    else:
        progress = {"log": [], "done": 0, "total": len(cfg["channels"]), "skipped": []}
        future = runner.submit(collect(runner, ss.key, ss.client, cfg, progress))
        ss.job = {"future": future, "progress": progress, "prefix": prefix or "telegram"}
        ss.results = None
        st.rerun()


# ── Step 3: progress and results ─────────────────────────────────
@st.fragment(run_every=2)
def job_panel():
    job = ss.job
    if not job:
        return
    prog, fut = job["progress"], job["future"]
    frac = prog["done"] / max(prog["total"], 1)
    st.progress(frac, text=f"{prog['done']} of {prog['total']} channels")
    st.code("\n".join(prog["log"][-LOG_LINES_SHOWN:]) or "Starting...")
    if st.button("Cancel collection"):
        fut.cancel()
    if fut.done():
        if fut.cancelled():
            st.warning("Collection cancelled.")
        elif fut.exception():
            st.error(f"Collection failed: {fut.exception()}")
        else:
            ss.results = {"data": fut.result(), "prefix": job["prefix"],
                          "skipped": prog["skipped"], "log": prog["log"]}
        ss.job = None
        st.rerun()


if ss.job:
    st.subheader("Progress")
    job_panel()

if ss.results:
    data = ss.results["data"]
    msgs = data["messages"]
    st.subheader("Results")
    st.download_button("Download all CSVs (.zip)",
                       zip_outputs(data, ss.results["prefix"]),
                       file_name=f"{ss.results['prefix']}_{date.today():%Y%m%d}.zip",
                       mime="application/zip", type="primary")
    if ss.results["skipped"]:
        st.warning("Skipped or partially collected: " + ", ".join(ss.results["skipped"]))

    m1, m2, m3, m4, m5 = st.columns(5)
    n_fwd = int(msgs["is_forward"].sum()) if not msgs.empty else 0
    m1.metric("Keyword matches", len(msgs))
    m2.metric("Forwards", n_fwd)
    m3.metric("Original posts", len(msgs) - n_fwd)
    m4.metric("Cascades", data["cascades"]["cascade_id"].nunique()
              if not data["cascades"].empty else 0)
    m5.metric("Full-scan matches", len(data["full_scan_hits"]))

    tabs = st.tabs(["Posts", "Per channel", "Forward sources", "Cascades",
                    "Mentions", "Domains", "Full-scan hits", "Run log"])
    with tabs[0]:
        st.dataframe(msgs, use_container_width=True)
    with tabs[1]:
        if not msgs.empty:
            per = (msgs.groupby("channel")
                   .agg(total=("message_id", "size"), forwards=("is_forward", "sum"))
                   .assign(original=lambda d: d.total - d.forwards)
                   .sort_values("total", ascending=False))
            st.dataframe(per, use_container_width=True)
    with tabs[2]:
        fn = data["forwarding_network"]
        if not fn.empty:
            st.dataframe(fn["forwarded_from"].value_counts().rename("count"),
                         use_container_width=True)
    with tabs[3]:
        cas = data["cascades"]
        if not cas.empty:
            firsts = cas.groupby("cascade_id")
            fast = sum((g["delay_hours"].iloc[1:] < 1).any() for _, g in firsts)
            slow = sum((g["delay_hours"].iloc[1:] >= 24).all() for _, g in firsts)
            st.write(f"Cascades with a repost under 1 hour: {fast}. "
                     f"Cascades with every repost 24+ hours later: {slow}.")
            st.dataframe(cas, use_container_width=True)
    with tabs[4]:
        cm = data["channel_mentions"]
        if not cm.empty:
            st.dataframe(cm["mentioned_channel"].value_counts().rename("count"),
                         use_container_width=True)
            st.dataframe(cm, use_container_width=True)
    with tabs[5]:
        u = data["urls"]
        if not u.empty:
            st.dataframe(u["domain"].value_counts().rename("count"), use_container_width=True)
    with tabs[6]:
        st.dataframe(data["full_scan_hits"], use_container_width=True)
    with tabs[7]:
        st.code("\n".join(ss.results["log"]))

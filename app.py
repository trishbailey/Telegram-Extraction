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
from html import escape
from math import sqrt
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

import pandas as pd
import plotly.graph_objects as go
import resvg_py
import streamlit as st
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Alignment, Font
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
                        row["full_scan_keywords"] = ", ".join(matched)
                        scan_hits.append(row)
                        hits += 1
                log(f"  {hits} full-scan matches")
        except Exception as e:
            log(f"  error: {type(e).__name__}: {e}")
            progress["skipped"].append(channel)

        progress["done"] += 1
        await asyncio.sleep(2)

    log(f"Finished: {len(messages)} keyword matches, {len(scan_hits)} full-scan matches")
    return build_posts(messages, scan_hits, cfg["min_cascade_chars"])


# ═══════════════════════════════════════════════════════════════════
# Analysis (pure functions, no Telegram calls)
# ═══════════════════════════════════════════════════════════════════
# Telegram usernames: 5-32 characters, start with a letter
AT_RE = re.compile(r"(?<![\w@./])@([A-Za-z][A-Za-z0-9_]{4,31})(?![A-Za-z0-9_])")
TG_RE = re.compile(
    r"(?<![\w.])(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me|telegram\.dog)/(?:s/)?"
    r"([A-Za-z][A-Za-z0-9_]{4,31})(?![A-Za-z0-9_])", re.I)
TME_RESERVED = {"joinchat", "addstickers", "addemoji", "addlist", "addtheme", "share",
                "proxy", "socks", "setlanguage", "contact", "boost", "login",
                "confirmphone", "invoice"}
URL_RE = re.compile(r"https?://[^\s<>\"'\[\]]+", re.I)
URL_TRAILING = ".,;:!?*`'\"»”’"
LINK_USER_RE = re.compile(r"t\.me/(?:s/)?([A-Za-z0-9_]+)/", re.I)


def clean_url(url):
    """Trim punctuation and the closing parenthesis of Markdown links."""
    url = url.rstrip(URL_TRAILING)
    while url.endswith(")") and url.count(")") > url.count("("):
        url = url[:-1].rstrip(URL_TRAILING)
    return url


def handle_key(value):
    return str(value or "").strip().lstrip("@").lower()


def self_keys(channel, link="", title=""):
    """Identifiers a channel uses for itself, to drop self-mentions and self-forwards."""
    keys = {handle_key(channel)}
    m = LINK_USER_RE.search(str(link or ""))
    if m:
        keys.add(m.group(1).lower())
    if title and isinstance(title, str):
        keys.add(title.strip().lower())
    keys.discard("")
    return keys


def extract_mentions(text, exclude=()):
    text = text or ""
    found = {}
    for handle in AT_RE.findall(text) + TG_RE.findall(text):
        key = handle.lower()
        if key in TME_RESERVED or key in exclude or key in found:
            continue
        found[key] = f"@{handle}"
    return list(found.values())


def extract_urls(text):
    return list(dict.fromkeys(clean_url(u) for u in URL_RE.findall(text or "")))

POST_COLUMNS = [
    "channel", "date", "text", "link", "views", "forwards", "replies",
    "is_forward", "forwarded_from", "original_fwd_date",
    "found_by", "search_term", "matched_keywords", "full_scan_keywords",
    "mentioned_channels", "domains", "urls",
    "cascade_id", "cascade_channel_count", "cascade_first_channel",
    "cascade_first_date", "hours_after_first",
    "media_type", "channel_title", "message_id", "edit_date", "post_author",
    "reply_to_msg_id", "grouped_id",
]


def normalize_text(text):
    return re.sub(r"\s+", " ", (text or "")[:200].lower().strip())


def unique_in_order(items):
    return list(dict.fromkeys(items))


def build_posts(messages, scan_hits, min_cascade_chars):
    """Merge keyword and full-scan results into one row per post, with derived columns."""
    posts = {}
    for m in messages:
        row = dict(m, found_by="keyword search", full_scan_keywords="")
        posts[(row["channel"], row["message_id"])] = row
    for h in scan_hits:
        k = (h["channel"], h["message_id"])
        if k in posts:
            posts[k]["found_by"] = "both"
            posts[k]["full_scan_keywords"] = h["full_scan_keywords"]
        else:
            posts[k] = dict(h, found_by="full scan")
    rows = list(posts.values())
    if not rows:
        return pd.DataFrame(columns=POST_COLUMNS)

    for r in rows:
        text = r["text"] or ""
        mentions = extract_mentions(
            text, self_keys(r["channel"], r.get("link"), r.get("channel_title")))
        urls = extract_urls(text)
        domains = []
        for u in urls:
            try:
                domains.append(urlparse(u).netloc)
            except Exception:
                pass
        r["mentioned_channels"] = ", ".join(mentions)
        r["urls"] = "\n".join(urls)
        r["domains"] = ", ".join(unique_in_order(d for d in domains if d))
        r.update(cascade_id=None, cascade_channel_count=None, cascade_first_channel=None,
                 cascade_first_date=None, hours_after_first=None)

    groups = defaultdict(list)
    for r in rows:
        key = normalize_text(r["text"])
        if len(key) > min_cascade_chars:
            groups[key].append(r)
    cascades = [sorted(g, key=lambda r: r["date"]) for g in groups.values()
                if len({r["channel"] for r in g}) >= 2]
    cascades.sort(key=lambda g: (-len({r["channel"] for r in g}), g[0]["date"]))
    for cid, group in enumerate(cascades, start=1):
        first = group[0]
        first_dt = datetime.fromisoformat(first["date"])
        n = len({r["channel"] for r in group})
        for r in group:
            r.update(
                cascade_id=cid, cascade_channel_count=n,
                cascade_first_channel=first["channel"], cascade_first_date=first["date"],
                hours_after_first=round(
                    (datetime.fromisoformat(r["date"]) - first_dt).total_seconds() / 3600, 2),
            )

    df = pd.DataFrame(rows).sort_values(["channel", "date"], ascending=[True, False])
    return df[[c for c in POST_COLUMNS if c in df.columns]].reset_index(drop=True)


WRAP_WIDTHS = {"text": 90, "urls": 50}
EXCEL_CELL_LIMIT = 32767


def xlsx_bytes(df, sheet="Posts", tall_rows=True):
    def clean(v):
        if isinstance(v, str):
            return ILLEGAL_CHARACTERS_RE.sub("", v)[:EXCEL_CELL_LIMIT]
        return v

    base_font = Font(name="Arial", size=10)
    head_font = Font(name="Arial", size=10, bold=True)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        df.map(clean).to_excel(xw, sheet_name=sheet, index=False)
        ws = xw.sheets[sheet]
        ws.freeze_panes = "D2" if tall_rows else "A2"
        ws.auto_filter.ref = ws.dimensions
        wrap_letters = set()
        for cell in ws[1]:
            cell.font = head_font
            header = str(cell.value)
            if header in WRAP_WIDTHS:
                wrap_letters.add(cell.column_letter)
                width = WRAP_WIDTHS[header]
            else:
                width = min(max(len(header) + 4, 12), 30)
            ws.column_dimensions[cell.column_letter].width = width
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.font = base_font
                cell.alignment = Alignment(vertical="top",
                                           wrap_text=cell.column_letter in wrap_letters)
            if tall_rows:
                ws.row_dimensions[row[0].row].height = 150
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════
# Referral analysis
# ═══════════════════════════════════════════════════════════════════
UNRESOLVED_RE = re.compile(r"(?:channel_id|user_id|chat_id)=(\d+)")
TYPES = ["mention", "forward", "website"]
TYPE_NAMES = {"mention": "mention", "forward": "forward", "website": "website link"}
TYPE_HEX = {"mention": "#2F80ED", "forward": "#F2994A", "website": "#27AE60"}
TYPE_COLORS = {"mention": "rgba(47, 128, 237, 0.5)", "forward": "rgba(242, 153, 74, 0.6)",
               "website": "rgba(39, 174, 96, 0.5)"}
NODE_COLOR = "rgba(120, 120, 120, 0.85)"
TELEGRAM_DOMAINS = {"t.me", "telegram.me", "telegram.dog"}


def domain_of(url):
    try:
        host = urlparse(url).netloc.lower().split("@")[-1].split(":")[0]
    except Exception:
        return ""
    return host[4:] if host.startswith("www.") else host


def load_posts_file(uploaded):
    name = uploaded.name.lower()
    if name.endswith((".xlsx", ".xlsm")):
        df = pd.read_excel(uploaded, sheet_name=0)
    elif name.endswith(".csv"):
        df = pd.read_csv(uploaded, encoding="utf-8-sig")
    else:
        raise ValueError("Upload an .xlsx or .csv file produced by the collector.")
    missing = {"channel", "text"} - set(df.columns)
    if missing:
        raise ValueError(f"The file is missing required columns: {', '.join(sorted(missing))}.")
    return df


def referral_edges(posts):
    """One row per (post, referred account, type). Mentions and forwards both count."""
    posts = posts.copy()
    for col in ("link", "channel_title", "forwarded_from", "views", "date"):
        if col not in posts.columns:
            posts[col] = None
    posts["views"] = pd.to_numeric(posts["views"], errors="coerce").fillna(0)
    posts["date"] = pd.to_datetime(posts["date"], errors="coerce", utc=True)

    labels = {}
    title_to_key = {}
    for r in posts.itertuples(index=False):
        key = handle_key(r.channel)
        m = LINK_USER_RE.search(str(r.link or ""))
        labels.setdefault(key, f"@{m.group(1)}" if m else f"@{r.channel}")
        if isinstance(r.channel_title, str) and r.channel_title.strip():
            title_to_key.setdefault(r.channel_title.strip().lower(), key)

    def source_node(value):
        value = str(value).strip()
        if value.startswith("@"):
            return handle_key(value), value
        unresolved = UNRESOLVED_RE.search(value)
        if unresolved or value.startswith("[unresolved"):
            ident = unresolved.group(1) if unresolved else value
            return f"id:{ident}", f"Unresolved account {ident}"
        key = title_to_key.get(value.lower(), f"name:{value.lower()}")
        return key, labels.get(key, value)

    rows = []
    for uid, r in enumerate(posts.itertuples(index=False)):
        src = handle_key(r.channel)
        mine = self_keys(r.channel, r.link, r.channel_title)
        base = {"post_uid": uid, "referrer_key": src, "date": r.date, "views": r.views,
                "post_link": r.link if isinstance(r.link, str) else ""}
        for mention in extract_mentions(r.text if isinstance(r.text, str) else "", mine):
            key = handle_key(mention)
            labels.setdefault(key, mention)
            rows.append({**base, "referred_key": key, "type": "mention"})
        text = r.text if isinstance(r.text, str) else ""
        for domain in dict.fromkeys(domain_of(u) for u in extract_urls(text)):
            if domain and domain not in TELEGRAM_DOMAINS:
                key = f"web:{domain}"
                labels.setdefault(key, domain)
                rows.append({**base, "referred_key": key, "type": "website"})
        fwd = r.forwarded_from
        if isinstance(fwd, str) and fwd.strip():
            key, label = source_node(fwd)
            if key not in mine and key.removeprefix("name:") not in mine:
                labels.setdefault(key, label)
                rows.append({**base, "referred_key": key, "type": "forward"})

    edges = pd.DataFrame(rows, columns=["post_uid", "referrer_key", "referred_key", "type",
                                        "date", "views", "post_link"])
    edges["referrer"] = edges["referrer_key"].map(labels)
    edges["referred"] = edges["referred_key"].map(labels)
    return edges


def summarize_edges(edges):
    if edges.empty:
        return pd.DataFrame()
    g = edges.groupby(["referrer_key", "referrer", "referred_key", "referred", "type"])
    out = g.agg(posts=("type", "size"), views=("views", "sum"),
                first_seen=("date", "min"), last_seen=("date", "max"),
                example_post=("post_link", "first")).reset_index()
    return out


def counterparty_table(summary, side_key, other_key, other_label, focus):
    part = summary[summary[side_key] == focus]
    if part.empty:
        return pd.DataFrame()
    wide = part.pivot_table(index=[other_key, other_label], columns="type",
                            values="posts", aggfunc="sum", fill_value=0)
    for t in TYPES:
        if t not in wide.columns:
            wide[t] = 0
    extra = part.groupby([other_key, other_label]).agg(
        views=("views", "sum"), first_seen=("first_seen", "min"),
        last_seen=("last_seen", "max"), example_post=("example_post", "first"))
    table = wide[TYPES].join(extra).reset_index()
    table = table.rename(columns={other_label: "account", "mention": "mentions",
                                  "forward": "forwards", "website": "website_links"})
    table["total"] = table["mentions"] + table["forwards"] + table["website_links"]
    cols = ["account", "total", "mentions", "forwards", "website_links", "views",
            "first_seen", "last_seen", "example_post"]
    if not table["website_links"].any():
        cols.remove("website_links")
    table = table[cols]
    return table.sort_values(["total", "views"], ascending=False).reset_index(drop=True)


def other_label(count, noun):
    return f"Other ({count} {noun}{'' if count == 1 else 's'})"


def top_n(part, key_col, label_col, weight, n):
    """Keep the n heaviest counterparties; fold the rest into one 'Other' node."""
    totals = part.groupby(key_col)[weight].sum().sort_values(ascending=False)
    keep = set(totals.index[:n])
    rest = totals.index[n:]
    part = part.copy()
    if len(rest):
        mask = ~part[key_col].isin(keep)
        part.loc[mask, key_col] = "__other__"
        part.loc[mask, label_col] = other_label(len(rest), "account")
    return (part.groupby([key_col, label_col, "type"], as_index=False)
            .agg(posts=("posts", "sum"), views=("views", "sum")))


def short(label, limit=40):
    return label if len(label) <= limit else label[: limit - 1] + "…"


def build_sankey(nodes, links, weight, height):
    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(label=[short(n[0]) for n in nodes], color=[n[1] for n in nodes],
                  pad=12, thickness=16, customdata=[n[0] for n in nodes],
                  hovertemplate="%{customdata}<br>%{value:,.0f} " + weight + "<extra></extra>"),
        link=dict(
            source=[l["source"] for l in links], target=[l["target"] for l in links],
            value=[l["value"] for l in links], color=[TYPE_COLORS[l["type"]] for l in links],
            customdata=[[l["type"], l["posts"], l["views"]] for l in links],
            hovertemplate=("%{source.customdata} → %{target.customdata}<br>"
                           "%{customdata[0]}: %{customdata[1]:,} posts, "
                           "%{customdata[2]:,.0f} views<extra></extra>")),
    ))
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=10, b=10), font_size=12)
    return fig


def compact(n):
    n = float(n)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= div:
            return f"{n / div:.1f}".rstrip("0").rstrip(".") + suffix
    return f"{n:,.0f}"


def plural(n, word):
    return f"{n:,} {word}{'' if n == 1 else 's'}"


def ego_cards(summary, edges, focus, side, weight, n_accounts, n_sites):
    if side == "in":
        part, key, label = summary[summary["referred_key"] == focus], "referrer_key", "referrer"
        post_ids = edges.loc[edges["referred_key"] == focus, "post_uid"]
    else:
        part, key, label = summary[summary["referrer_key"] == focus], "referred_key", "referred"
        post_ids = edges.loc[edges["referrer_key"] == focus, "post_uid"]
    if part.empty:
        return [], {"accounts": 0, "sites": 0, "posts": 0, "ids": set()}
    per = part.groupby([key, label, "type"], as_index=False)[["posts", "views"]].sum()
    per["is_site"] = per[key].str.startswith("web:")
    totals = {"accounts": int(per.loc[~per["is_site"], key].nunique()),
              "sites": int(per.loc[per["is_site"], key].nunique()),
              "posts": int(post_ids.nunique()), "ids": set(post_ids)}

    def card(rows, name, is_site, other=False):
        types = rows.groupby("type")[["posts", "views"]].sum()
        return {"label": name, "site": is_site, "other": other,
                "value": float(types[weight].sum()),
                "types": {t: (int(types.at[t, "posts"]), float(types.at[t, "views"]))
                          for t in TYPES if t in types.index}}

    cards, others = [], []
    for is_site, n, noun in ((False, n_accounts, "account"), (True, n_sites, "website")):
        grp = per[per["is_site"] == is_site]
        if grp.empty or n == 0:
            continue
        order = grp.groupby([key, label])[weight].sum().sort_values(ascending=False)
        for k, name in order.index[:n]:
            cards.append(card(grp[grp[key] == k], name, is_site))
        rest = [k for k, _ in order.index[n:]]
        if rest:
            others.append(card(grp[grp[key].isin(rest)], other_label(len(rest), noun),
                               is_site, other=True))
    cards.sort(key=lambda c: -c["value"])
    return cards + others, totals


def fit(text, px, size, bold=False):
    limit = max(4, int(px / (size * (0.68 if bold else 0.58))))
    return text if len(text) <= limit else text[: limit - 1] + "…"


def ego_svg(summary, edges, focus, focus_label, weight, n_accounts, n_sites, collected=True):
    left, tin = ego_cards(summary, edges, focus, "in", weight, n_accounts, n_sites)
    right, tout = ego_cards(summary, edges, focus, "out", weight, n_accounts, n_sites)

    W, CW, CH, GAP, R = 1100, 300, 64, 14, 74
    LX, RX, HX = 12, W - 12 - CW, W / 2
    TOP = 92
    rows = max(len(left), len(right), 3)
    body = rows * (CH + GAP) - GAP
    HY = TOP + body / 2
    BOX_H, BOX_GAP = 58, 10
    box_top = TOP + body + 34
    H = box_top + 3 * (BOX_H + BOX_GAP) + 6

    values = [v[0 if weight == "posts" else 1] for c in left + right for v in c["types"].values()]
    vmax = max(values) if values else 1

    def stroke(v):
        return 1.5 + 22 * sqrt(v / vmax) if vmax > 0 else 1.5

    curves, cards_svg = [], []
    for side, cards, x0 in (("in", left, LX), ("out", right, RX)):
        lead = (body - (len(cards) * (CH + GAP) - GAP)) / 2
        for i, c in enumerate(cards):
            top = TOP + lead + i * (CH + GAP)
            mid = top + CH / 2
            present = [t for t in TYPES if t in c["types"]]
            widths = [stroke(c["types"][t][0 if weight == "posts" else 1]) for t in present]
            span = sum(widths) + 2 * (len(widths) - 1)
            offset = mid - span / 2
            for t, w in zip(present, widths):
                ya = offset + w / 2
                offset += w + 2
                yb = HY + (ya - mid) * 0.35
                if side == "in":
                    xa, xb = x0 + CW, HX - R + 4
                else:
                    xa, xb = x0, HX + R - 4
                xc = (xa + xb) / 2
                posts, views = c["types"][t]
                tip = f"{c['label']}: {plural(posts, TYPE_NAMES[t])}, {views:,.0f} views"
                curves.append(
                    f'<path d="M{xa:.1f},{ya:.1f} C{xc:.1f},{ya:.1f} {xc:.1f},{yb:.1f} '
                    f'{xb:.1f},{yb:.1f}" fill="none" stroke="{TYPE_HEX[t]}" '
                    f'stroke-opacity="0.78" stroke-width="{w:.1f}" stroke-linecap="round">'
                    f'<title>{escape(tip)}</title></path>')

            detail = " · ".join(plural(c["types"][t][0], TYPE_NAMES[t]) for t in present)
            if weight == "views":
                stats = (f"{compact(sum(v[1] for v in c['types'].values()))} views · "
                         f"{plural(sum(v[0] for v in c['types'].values()), 'post')}")
            else:
                stats = detail
            fill = "#EAF7EF" if c["site"] else ("#F3F5F8" if c["other"] else "#FFFFFF")
            badge_x = x0 + CW - 34 if side == "in" else x0 + 34
            text_x = x0 + 16 if side == "in" else x0 + 66
            initial = "…" if c["other"] else (c["label"].lstrip("@")[:1] or "?").upper()
            badge = "#27AE60" if c["site"] else "#8A96A8"
            tip = f"{c['label']}\n{detail}\n{sum(v[1] for v in c['types'].values()):,.0f} views"
            cards_svg.append(
                f'<g><title>{escape(tip)}</title>'
                f'<rect x="{x0}" y="{top}" width="{CW}" height="{CH}" rx="6" fill="{fill}" '
                f'stroke="#B8C2CE" stroke-width="1"/>'
                f'<circle cx="{badge_x}" cy="{mid}" r="20" fill="{badge}" fill-opacity="0.9"/>'
                f'<text x="{badge_x}" y="{mid + 6}" font-size="17" font-weight="bold" '
                f'fill="#FFFFFF" text-anchor="middle">{escape(initial)}</text>'
                f'<text x="{text_x}" y="{top + 27}" font-size="15.5" fill="#2D3748">'
                f'{escape(fit(c["label"], CW - 70, 15.5))}</text>'
                f'<text x="{text_x}" y="{top + 48}" font-size="12.5" fill="#718096">'
                f'{escape(fit(stats, CW - 70, 12.5))}</text></g>')

    is_site = focus.startswith("web:")
    if is_site:
        right_msg = ["Websites do not post,", "so they have no outgoing referrals"]
    elif not collected:
        right_msg = ["Not a collected channel,", "so outgoing referrals are unknown"]
    else:
        right_msg = ["No outgoing referrals", "in this data"]
    show_right_boxes = collected and not is_site
    for cards, x0, msg in ((left, LX, ["No incoming referrals", "in this data"]),
                           (right, RX, right_msg)):
        if not cards:
            for j, line in enumerate(msg):
                cards_svg.append(f'<text x="{x0 + CW / 2}" y="{HY - 8 + j * 20}" font-size="14" '
                                 f'fill="#94A0B0" text-anchor="middle">{escape(line)}</text>')

    hub_color = "#27AE60" if focus.startswith("web:") else "#1F4FD1"
    name = fit(focus_label, 2 * R - 20, 14, bold=True)
    n_hub = len(tin["ids"] | tout["ids"])
    hub_posts = f"{compact(n_hub)} post{'' if n_hub == 1 else 's'}"
    hub = (f'<circle cx="{HX}" cy="{HY}" r="{R + 6}" fill="#FFFFFF" stroke="#D5DBE3"/>'
           f'<circle cx="{HX}" cy="{HY}" r="{R}" fill="{hub_color}"/>'
           f'<text x="{HX}" y="{HY + 2}" font-size="14" font-weight="bold" fill="#FFFFFF" '
           f'text-anchor="middle">{escape(name)}</text>'
           f'<text x="{HX}" y="{HY + 22}" font-size="11.5" fill="#E8EEFF" '
           f'text-anchor="middle">{escape(hub_posts)}</text>'
           f'<title>{escape(focus_label)}</title>')

    def box(x, y, label, value, align):
        tx = x + CW - 16 if align == "end" else x + 16
        return (f'<rect x="{x}" y="{y}" width="{CW}" height="{BOX_H}" rx="4" fill="#EEF1F5" '
                f'stroke="#8C96A3"/>'
                f'<text x="{tx}" y="{y + 22}" font-size="13.5" fill="#4A5568" '
                f'text-anchor="{align}">{escape(label)}</text>'
                f'<text x="{tx}" y="{y + 47}" font-size="22" fill="#2D3748" '
                f'text-anchor="{align}">{value:,}</text>')

    boxes = [box(LX, box_top, "Accounts referring", tin["accounts"], "end"),
             box(LX, box_top + BOX_H + BOX_GAP, "Posts with referrals", tin["posts"], "end")]
    if show_right_boxes:
        boxes += [box(RX, box_top, "Telegram accounts referred to", tout["accounts"], "start"),
                  box(RX, box_top + BOX_H + BOX_GAP, "Websites linked", tout["sites"], "start"),
                  box(RX, box_top + 2 * (BOX_H + BOX_GAP), "Posts with referrals",
                      tout["posts"], "start")]

    legend, lx = [], HX - 190
    for t in TYPES:
        legend.append(f'<line x1="{lx}" y1="30" x2="{lx + 28}" y2="30" stroke="{TYPE_HEX[t]}" '
                      f'stroke-width="6" stroke-linecap="round"/>'
                      f'<text x="{lx + 36}" y="35" font-size="13" fill="#4A5568">'
                      f'{TYPE_NAMES[t]}</text>')
        lx += 130
    width_note = "Line width: number of posts" if weight == "posts" else "Line width: views"
    headers = (f'<text x="{LX}" y="72" font-size="14" font-weight="bold" fill="#4A5568">'
               f'Incoming referrals</text>'
               f'<text x="{RX + CW}" y="72" font-size="14" font-weight="bold" fill="#4A5568" '
               f'text-anchor="end">Outgoing referrals</text>'
               f'<text x="{HX}" y="58" font-size="11.5" fill="#94A0B0" text-anchor="middle">'
               f'{width_note}</text>')

    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H:.0f}" '
            f'width="{W}" height="{round(H)}" '
            f'font-family="DejaVu Sans, Helvetica, Arial, sans-serif">'
            f'<rect width="{W}" height="{H:.0f}" fill="#FFFFFF"/>'
            + "".join(legend) + headers + "".join(curves) + "".join(cards_svg) + hub
            + "".join(boxes) + "</svg>"), H


def svg_to_png(svg):
    return bytes(resvg_py.svg_to_bytes(svg_string=svg, zoom=2, background="#ffffff"))


def overview_sankey(summary, weight, n_sources, n_targets):
    """Collected channels on the left, the accounts they refer to on the right."""
    cols = ["referrer_key", "referrer", "referred_key", "referred", "type"]
    agg = summary.groupby(cols, as_index=False)[["posts", "views"]].sum()
    for key, label, n, noun in (("referred_key", "referred", n_targets, "account"),
                                ("referrer_key", "referrer", n_sources, "channel")):
        totals = agg.groupby(key)[weight].sum().sort_values(ascending=False)
        mask = ~agg[key].isin(set(totals.index[:n]))
        if mask.any():
            agg.loc[mask, key] = f"__other_{key}__"
            agg.loc[mask, label] = other_label(len(totals) - n, noun)
    agg = agg.groupby(cols, as_index=False)[["posts", "views"]].sum()

    left = agg.groupby(["referrer_key", "referrer"])[weight].sum().sort_values(ascending=False)
    right = agg.groupby(["referred_key", "referred"])[weight].sum().sort_values(ascending=False)
    nodes = [(lbl, NODE_COLOR) for _, lbl in left.index]
    nodes += [(lbl, NODE_COLOR) for _, lbl in right.index]
    li = {k: i for i, (k, _) in enumerate(left.index)}
    ri = {k: len(li) + i for i, (k, _) in enumerate(right.index)}
    links = [{"source": li[r.referrer_key], "target": ri[r.referred_key],
              "value": getattr(r, weight), "type": r.type, "posts": r.posts, "views": r.views}
             for r in agg.itertuples(index=False)]
    height = max(480, 28 * max(len(li), len(ri)) + 80)
    return build_sankey(nodes, links, weight, height)


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



def collector_page():
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


    if ss.job:
        st.subheader("Progress")
        job_panel()

    if ss.results:
        posts = ss.results["data"]
        st.subheader("Results")
        st.download_button("Download results (.xlsx)",
                           xlsx_bytes(posts),
                           file_name=f"{ss.results['prefix']}_{date.today():%Y%m%d}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           type="primary")
        if ss.results["skipped"]:
            st.warning("Skipped or partially collected: " + ", ".join(ss.results["skipped"]))
        st.page_link(REFERRAL_PAGE, label="Map referrals for these results", icon="🔀")

        if posts.empty:
            st.info("No posts matched. Check the keywords, channels and date range.")
        else:
            n_fwd = int(posts["is_forward"].sum())
            n_cas = int(posts["cascade_id"].nunique())
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Posts", len(posts))
            m2.metric("Forwards", n_fwd)
            m3.metric("Original posts", len(posts) - n_fwd)
            m4.metric("Cascades", n_cas)

            def counts(series, sep, label):
                items = (series.fillna("").str.split(sep).explode().str.strip())
                return items[items != ""].value_counts().rename_axis(label).rename("posts")

            tabs = st.tabs(["Posts", "Per channel", "Forward sources", "Cascades",
                            "Mentioned channels", "Domains", "Run log"])
            with tabs[0]:
                st.dataframe(posts, width="stretch")
            with tabs[1]:
                per = (posts.groupby("channel")
                       .agg(posts=("message_id", "size"), forwards=("is_forward", "sum"))
                       .assign(original=lambda d: d.posts - d.forwards)
                       .sort_values("posts", ascending=False))
                st.dataframe(per, width="stretch")
            with tabs[2]:
                st.dataframe(posts["forwarded_from"].dropna().value_counts()
                             .rename_axis("source").rename("forwards"),
                             width="stretch")
            with tabs[3]:
                cas = posts[posts["cascade_id"].notna()].sort_values(["cascade_id", "date"])
                if cas.empty:
                    st.write("No post text appeared in more than one channel.")
                else:
                    later = cas[cas.groupby("cascade_id").cumcount() > 0]
                    fast = later[later["hours_after_first"] < 1]["cascade_id"].nunique()
                    slow = n_cas - later[later["hours_after_first"] < 24]["cascade_id"].nunique()
                    st.write(f"Cascades with a repost under 1 hour: {fast}. "
                             f"Cascades with every repost 24+ hours later: {slow}.")
                    st.dataframe(cas[["cascade_id", "cascade_channel_count", "channel", "date",
                                      "hours_after_first", "views", "link", "text"]],
                                 width="stretch")
            with tabs[4]:
                st.dataframe(counts(posts["mentioned_channels"], ",", "channel"),
                             width="stretch")
            with tabs[5]:
                st.dataframe(counts(posts["domains"], ",", "domain"), width="stretch")
            with tabs[6]:
                st.code("\n".join(ss.results["log"]))

def referral_page():
    st.title("Referral map")
    st.caption("A referral is a post that points its audience somewhere else: a mention of "
               "another Telegram account (@handle or t.me link), a forward of another "
               "account's post, or a link to a website. Self-mentions and self-forwards "
               "are excluded.")

    sources = ["Upload a spreadsheet"]
    if ss.results is not None and not ss.results["data"].empty:
        sources.insert(0, "Results from this session")
    source = st.radio("Data", sources, horizontal=True)

    if source == "Upload a spreadsheet":
        uploaded = st.file_uploader("Collector output (.xlsx or .csv)", type=["xlsx", "csv"])
        if uploaded is None:
            st.info("Upload a file downloaded from the Collect posts page.")
            return
        try:
            posts = load_posts_file(uploaded)
        except Exception as e:
            st.error(str(e))
            return
    else:
        posts = ss.results["data"]

    edges = referral_edges(posts)
    if edges.empty:
        st.warning("No mentions of other accounts and no forwards were found in these posts.")
        return

    c1, c2, c3 = st.columns([2, 2, 1])
    types = c1.multiselect("Referral types", TYPES, default=TYPES,
                           format_func=lambda t: TYPE_NAMES[t])
    dated = edges["date"].dropna()
    if not dated.empty:
        lo, hi = dated.min().date(), dated.max().date()
        picked = c2.date_input("Date range", value=(lo, hi), min_value=lo, max_value=hi)
        if isinstance(picked, tuple) and len(picked) == 2:
            start = pd.Timestamp(picked[0], tz="UTC")
            end = pd.Timestamp(picked[1], tz="UTC") + pd.Timedelta(days=1)
            edges = edges[edges["date"].isna() | ((edges["date"] >= start) & (edges["date"] < end))]
    weight_label = c3.radio("Line width", ["Posts", "Views"])
    weight = weight_label.lower()

    edges = edges[edges["type"].isin(types)]
    summary = summarize_edges(edges)
    if summary.empty:
        st.warning("No referrals match these filters.")
        return

    png = {"toImageButtonOptions": {"format": "png", "scale": 2, "filename": "referral_map"}}
    collected = set(posts["channel"].map(handle_key))

    tab_focus, tab_all = st.tabs(["One channel", "All collected channels"])

    with tab_focus:
        totals = pd.concat([
            summary.groupby(["referrer_key", "referrer"])["posts"].sum()
                   .rename_axis(["key", "label"]),
            summary.groupby(["referred_key", "referred"])["posts"].sum()
                   .rename_axis(["key", "label"]),
        ]).groupby(level=[0, 1]).sum().sort_values(ascending=False)
        options = list(totals.index)
        choice = st.selectbox(
            "Channel or website", options,
            format_func=lambda kl: f"{kl[1]}  ({totals[kl]:,} referrals)")
        g1, g2 = st.columns(2)
        n_acc = g1.slider("Telegram accounts shown per side", 3, 30, 10)
        n_web = g2.slider("Websites shown", 0, 20, 5,
                          help="Set to 0 to hide websites from the diagram.")
        focus, focus_label = choice
        svg, height = ego_svg(summary, edges, focus, focus_label, weight, n_acc, n_web,
                              collected=focus in collected)
        size_attr = 'width="1100" height="%d"' % round(height)
        responsive = svg.replace(size_attr, 'width="100%"', 1)
        # All text inside the SVG is escaped with html.escape before it reaches the page.
        st.iframe('<div style="max-width:1100px;margin:0 auto">' + responsive + "</div>",
                  height="content")
        stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", focus_label.lstrip("@")) or "channel"
        d1, d2, _ = st.columns([1, 1, 2])
        d1.download_button("Download diagram (.png)", svg_to_png(svg),
                           file_name=f"referrals_{stem}.png", mime="image/png")
        d2.download_button("Download diagram (.svg)", svg.encode("utf-8"),
                           file_name=f"referrals_{stem}.svg", mime="image/svg+xml")
        st.caption("Hover over a card or line for exact counts. The SVG file scales "
                   "without blurring in PowerPoint and Word.")
        inc = counterparty_table(summary, "referred_key", "referrer_key", "referrer", focus)
        out = counterparty_table(summary, "referrer_key", "referred_key", "referred", focus)
        t1, t2 = st.columns(2)
        t1.markdown(f"**Incoming to {focus_label}**")
        t1.dataframe(inc, width="stretch", hide_index=True)
        t2.markdown(f"**Outgoing from {focus_label}**")
        t2.dataframe(out, width="stretch", hide_index=True)

    with tab_all:
        a1, a2 = st.columns(2)
        n_src = a1.slider("Collected channels shown", 3, 50, 20)
        n_tgt = a2.slider("Referred accounts shown", 5, 60, 25)
        st.caption("Left: collected channels. Right: the accounts they refer their audience to.")
        st.plotly_chart(overview_sankey(summary, weight, n_src, n_tgt),
                        width="stretch", config=png)

    table = summary.rename(columns={"referrer": "from_channel", "referred": "to_account"}) \
        .drop(columns=["referrer_key", "referred_key"]) \
        .sort_values(["posts", "views"], ascending=False)
    for col in ("first_seen", "last_seen"):
        table[col] = table[col].dt.strftime("%Y-%m-%d %H:%M").fillna("")
    st.download_button("Download referral table (.xlsx)",
                       xlsx_bytes(table, sheet="Referrals", tall_rows=False),
                       file_name=f"referrals_{date.today():%Y%m%d}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.caption("To save the all-channels diagram as an image, hover over it and click "
               "the camera icon.")


COLLECT_PAGE = st.Page(collector_page, title="Collect posts", icon="📥", default=True)
REFERRAL_PAGE = st.Page(referral_page, title="Referral map", icon="🔀")
st.navigation([COLLECT_PAGE, REFERRAL_PAGE]).run()

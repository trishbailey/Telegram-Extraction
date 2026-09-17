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
import json
import urllib.request
import uuid
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from html import escape
from math import sqrt
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qs, quote, unquote, urlparse

import networkx as nx
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import resvg_py
import streamlit as st
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Alignment, Font
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.sessions import StringSession

MAX_CONCURRENT_JOBS = 20
MAX_LOOKUPS = 3000                # web lookups per collection          # collections allowed to run at once
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
    query = parse_query(cfg["query"])
    search_terms = cfg["search_terms"]
    text_limit = cfg["text_limit"]

    messages = []
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
        """Returns a row if the post matches the query, otherwise None."""
        text = msg.text or ""
        is_fwd = msg.fwd_from is not None
        link = f"https://t.me/{username}/{msg.id}" if username else ""
        # Channel, text and link are enough for every field except from:, which needs
        # the forward source; resolve it only for forwards.
        fwd_name = await resolve_forward(msg) if is_fwd else None
        rec = PostRecord(text, channel, title, link, fwd_name)
        if not query_matches(query, rec):
            return None
        media_type = type(msg.media).__name__.replace("MessageMedia", "") if msg.media else ""
        return {
            "channel": channel,
            "channel_title": title,
            "message_id": msg.id,
            "link": link,
            "date": msg.date.isoformat(),
            "edit_date": msg.edit_date.isoformat() if msg.edit_date else None,
            "post_author": msg.post_author,
            "views": msg.views or 0,
            "forwards": msg.forwards or 0,
            "replies": msg.replies.replies if msg.replies else 0,
            "is_forward": is_fwd,
            "forwarded_from": fwd_name,
            "original_fwd_date": msg.fwd_from.date.isoformat()
                if is_fwd and msg.fwd_from.date else None,
            "search_term": term,
            "query_matches": ", ".join(matched_terms(query, rec)),
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
        seen_ids, checked, count = set(), 0, 0

        async def consider(msg, term):
            nonlocal checked, count
            if msg.id in seen_ids:
                return
            seen_ids.add(msg.id)
            checked += 1
            row = await to_row(msg, channel, username, title, term)
            if row is not None:
                messages.append(row)
                count += 1
            if checked % 500 == 0:
                log(f"  {checked:,} posts checked, {count:,} matches so far")

        try:
            if cfg["method"] == "search":
                for term in search_terms:
                    async for msg in iter_messages_retry(client, entity, log,
                                                         search=term, offset_date=end):
                        if msg.date < start:
                            break
                        if msg.date < end:
                            await consider(msg, term)
                    await asyncio.sleep(1)
            else:
                try:
                    newest = await client.get_messages(entity, limit=1, offset_date=end)
                    before = await client.get_messages(entity, limit=1, offset_date=start)
                    approx = max((newest[0].id if newest else 0)
                                 - (before[0].id if before else 0), 0)
                    log(f"  about {approx:,} posts in range")
                except Exception:
                    pass
                async for msg in iter_messages_retry(client, entity, log, offset_date=end):
                    if msg.date < start:
                        break
                    if msg.date < end:
                        await consider(msg, None)
            log(f"  {plural(count, 'match')} ({plural(checked, 'post')} checked)")
        except Exception as e:
            log(f"  error: {type(e).__name__}: {e}")
            progress["skipped"].append(channel)

        progress["done"] += 1
        await asyncio.sleep(2)

    log(f"Finished: {plural(len(messages), 'matching post')}")
    resolved = {}
    if cfg.get("resolve_links") and messages:
        urls = resolvable_urls(m["text"] for m in messages)
        if len(urls) > MAX_LOOKUPS:
            log(f"Looking up the first {MAX_LOOKUPS:,} of {len(urls):,} video and short links")
            urls = urls[:MAX_LOOKUPS]
        elif urls:
            log(f"Looking up {plural(len(urls), 'video and short link')}")
        if urls:
            progress["phase"] = "Identifying accounts behind links"

            def tick(i, n):
                progress["lookups"] = (i, n)
                if i % 100 == 0 or i == n:
                    progress["log"].append(f"{datetime.now():%H:%M:%S}   {i:,} of {n:,} links checked")

            loop = asyncio.get_running_loop()
            resolved = await loop.run_in_executor(None, resolve_links, urls, tick)
            log(f"Identified {plural(len(resolved), 'account')} from {plural(len(urls), 'link')}")
    return build_posts(messages, cfg["min_cascade_chars"], resolved)


# ── Size estimates and method comparison ───────────────────────────
# Telethon pauses about a second between history requests of 100 posts each.
THOROUGH_POSTS_PER_MIN = (3000, 5000)


async def estimate_volume(client, channels, start, end):
    """Approximate posts per channel in the date range from message IDs (2 requests each)."""
    rows = []
    for ch in channels:
        try:
            entity = await with_flood_retry(lambda: client.get_entity(ch), lambda _: None)
            newest = await client.get_messages(entity, limit=1, offset_date=end)
            before = await client.get_messages(entity, limit=1, offset_date=start)
            hi = newest[0].id if newest else 0
            lo = before[0].id if before else 0
            rows.append({"channel": ch, "posts": max(hi - lo, 0), "note": ""})
        except Exception as e:
            rows.append({"channel": ch, "posts": None, "note": f"not available ({type(e).__name__})"})
        await asyncio.sleep(0.5)
    return rows


def estimate_minutes(total_posts, n_channels, n_terms):
    lo = total_posts / THOROUGH_POSTS_PER_MIN[1] + n_channels * 2 / 60
    hi = total_posts / THOROUGH_POSTS_PER_MIN[0] + n_channels * 4 / 60
    fast = n_channels * max(n_terms, 1) * 2.5 / 60
    return lo, hi, fast


def minutes_text(m):
    if m < 1:
        return "under a minute"
    if m < 90:
        return f"{m:.0f} minute{'s' if round(m) != 1 else ''}"
    return f"{m / 60:.1f} hours"


def range_text(lo, hi):
    if hi < 1:
        return "under a minute"
    if minutes_text(lo) == minutes_text(hi):
        return f"about {minutes_text(hi)}"
    if lo < 1:
        return f"up to {minutes_text(hi)}"
    if lo >= 90 and hi >= 90:
        return f"{lo / 60:.1f} to {hi / 60:.1f} hours"
    if hi < 90:
        return f"{lo:.0f} to {hi:.0f} minutes"
    return f"{minutes_text(lo)} to {minutes_text(hi)}"


async def compare_methods(runner, key, client, cfg, progress):
    """Run the thorough and fast methods on the same sample and compare what each found."""
    def log(line):
        progress["log"].append(f"{datetime.now():%H:%M:%S} {line}")

    runner.touch(key, busy=True)
    try:
        if runner.slots.locked():
            log("Waiting for a free collection slot...")
        async with runner.slots:
            base = {**cfg, "resolve_links": False}
            progress["phase"] = "Thorough pass"
            log("Thorough pass: reading every post")
            thorough = await _collect(runner, key, client, {**base, "method": "scan"},
                                      progress, log)
            progress["done"] = 0
            progress["phase"] = "Fast pass"
            log("Fast pass: Telegram search")
            fast = await _collect(runner, key, client, {**base, "method": "search"},
                                  progress, log)
    finally:
        runner.touch(key, busy=False)

    def ids(df):
        return set() if df.empty else set(zip(df["channel"], df["message_id"]))

    t_ids, f_ids = ids(thorough), ids(fast)
    patterns = [(term, _term_regex("phrase", term)) for term in cfg["search_terms"]]

    def reason(row):
        norm = normalize_text(row["text"])
        present = [term for term, rx in patterns if rx.search(norm)]
        if present:
            return f"Contains {', '.join(present)}, but Telegram's search did not return it"
        matched = [m.strip() for m in str(row["query_matches"] or "").split(",") if m.strip()]
        wild = [m for m in matched if "*" in m or "?" in m]
        if wild:
            return (f"Matched through {', '.join(wild)}; Telegram's search looks only for the "
                    "fixed letters and does not expand wildcards")
        return ("None of the searched words appear as whole words; the post matched through "
                "a phrase, field filter or regular expression")

    cols = ["channel", "date", "text", "link", "query_matches"]
    missed = thorough[[k not in f_ids for k in zip(thorough["channel"],
                                                   thorough["message_id"])]] \
        if not thorough.empty else pd.DataFrame(columns=cols)
    missed = missed[cols].copy()
    missed["why_fast_missed_it"] = missed.apply(reason, axis=1) if not missed.empty else []
    missed["text"] = missed["text"].map(lambda t: (t or "")[:300])
    return {"thorough": len(t_ids), "fast": len(f_ids), "both": len(t_ids & f_ids),
            "fast_only": len(f_ids - t_ids), "missed": missed.reset_index(drop=True),
            "search_terms": cfg["search_terms"], "channels": cfg["channels"],
            "days": (cfg["end"] - cfg["start"]).days}


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
    "query_matches", "search_term",
    "mentioned_channels", "social_accounts", "domains", "forwarded_content_refs",
    "urls", "resolved_links",
    "cascade_id", "cascade_channel_count", "cascade_first_channel",
    "cascade_first_date", "hours_after_first",
    "media_type", "channel_title", "message_id", "edit_date", "post_author",
    "reply_to_msg_id", "grouped_id",
]


def normalize_text(text):
    return re.sub(r"\s+", " ", (text or "")[:200].lower().strip())


def unique_in_order(items):
    return list(dict.fromkeys(items))


def build_posts(messages, min_cascade_chars, resolved=None):
    """One row per post, with derived referral, link and cascade columns.

    Referral columns describe what the posting channel itself points to, so they are
    empty for forwards. A forward's embedded references go in forwarded_content_refs.
    """
    resolved = resolved or {}
    posts = {}
    for m in messages:
        posts.setdefault((m["channel"], m["message_id"]), dict(m))
    rows = list(posts.values())
    if not rows:
        return pd.DataFrame(columns=POST_COLUMNS)

    for r in rows:
        text = r["text"] or ""
        urls = extract_urls(text)
        refs = post_references(text, r["channel"], r.get("link"), r.get("channel_title"),
                               resolved)
        if r.get("forwarded_from"):
            fwd_key = handle_key(r["forwarded_from"])
            refs = [x for x in refs if x["key"] != fwd_key]
        own = [] if r.get("is_forward") else refs
        r["mentioned_channels"] = ", ".join(x["label"] for x in own if x["type"] == "telegram")
        r["social_accounts"] = "; ".join(ref_text(x) for x in own if x["type"] == "social")
        r["domains"] = ", ".join(x["label"] for x in own if x["type"] == "website")
        r["forwarded_content_refs"] = ("; ".join(ref_text(x) for x in refs)
                                       if r.get("is_forward") else "")
        r["urls"] = "\n".join(urls)
        r["resolved_links"] = format_resolved({u: resolved[u] for u in urls if u in resolved})
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
# Boolean query engine
# ═══════════════════════════════════════════════════════════════════
class QueryError(ValueError):
    pass


QUERY_FIELDS = {"text", "channel", "from", "mention", "domain"}
DEFAULT_NEAR = 10
QUERY_TOKEN_RE = re.compile(r"""
    (?P<ws>\s+)
  | (?P<lparen>\()
  | (?P<rparen>\))
  | (?P<near>NEAR(?:/(?P<n>\d+))?(?=[\s()"]|$))
  | (?P<phrase>(?:(?P<pfield>[A-Za-z]+):)?"(?P<ptext>[^"]*)(?P<pend>"?))
  | (?P<regex>(?:(?P<rfield>[A-Za-z]+):)?/(?P<rtext>(?:\\.|[^/\\])+)/)
  | (?P<sym>&&|\|\||[&|!])
  | (?P<word>[^\s()"]+)
""", re.X)
OPERATOR_WORDS = {"AND": "and", "OR": "or", "NOT": "not"}
SYMBOLS = {"&&": "and", "&": "and", "||": "or", "|": "or", "!": "not"}


def normalize_text(text):
    """Case-fold and treat ё as е, applied to both queries and posts."""
    return (text or "").casefold().replace("ё", "е")


def _split_field(raw):
    m = re.match(r"([A-Za-z]{2,12}):(.+)", raw)
    if m and not raw.lower().startswith(("http:", "https:")):
        field, rest = m.group(1).lower(), m.group(2)
        if field in QUERY_FIELDS:
            return field, rest
        raise QueryError(f'Unknown field "{m.group(1)}:". Fields are '
                         f"{', '.join(f + ':' for f in sorted(QUERY_FIELDS))}. "
                         f'To search for the text itself, put it in quotes: "{raw}".')
    return "text", raw


def tokenize_query(query):
    tokens, pos = [], 0
    while pos < len(query):
        m = QUERY_TOKEN_RE.match(query, pos)
        if not m:
            raise QueryError(f"Could not read the query at position {pos + 1}.")
        pos = m.end()
        kind = m.lastgroup
        if m.group("ws"):
            continue
        if m.group("lparen"):
            tokens.append(("(", None))
        elif m.group("rparen"):
            tokens.append((")", None))
        elif m.group("near"):
            tokens.append(("near", int(m.group("n") or DEFAULT_NEAR)))
        elif m.group("phrase") is not None and m.group("phrase").find('"') >= 0 and kind != "word":
            if not m.group("pend"):
                raise QueryError("A quotation mark is not closed.")
            field = (m.group("pfield") or "text").lower()
            if field not in QUERY_FIELDS:
                raise QueryError(f'Unknown field "{m.group("pfield")}:". '
                                 f"Use one of: {', '.join(sorted(QUERY_FIELDS))}.")
            if not m.group("ptext").strip():
                raise QueryError("A quoted phrase is empty.")
            tokens.append(("term", (field, "phrase", m.group("ptext").strip())))
        elif m.group("regex"):
            field = (m.group("rfield") or "text").lower()
            if field not in QUERY_FIELDS:
                raise QueryError(f'Unknown field "{m.group("rfield")}:".')
            try:
                re.compile(m.group("rtext"))
            except re.error as e:
                raise QueryError(f"Invalid regular expression /{m.group('rtext')}/: {e}")
            tokens.append(("term", (field, "regex", m.group("rtext"))))
        elif m.group("sym"):
            tokens.append((SYMBOLS[m.group("sym")], None))
        else:
            word = m.group("word")
            if word in OPERATOR_WORDS:
                tokens.append((OPERATOR_WORDS[word], None))
                continue
            if word.startswith("-"):
                tokens.append(("not", None))
                word = word[1:]
                if not word:
                    continue
            word = word.lstrip("+")
            if not word:
                continue
            if word.startswith("/"):
                raise QueryError(f"The regular expression starting at {word} is not closed with /.")
            field, value = _split_field(word)
            if not value.replace("*", "").replace("?", ""):
                raise QueryError(f'"{word}" needs at least one letter or number besides wildcards.')
            tokens.append(("term", (field, "word", value)))
    return tokens


class _Parser:
    STARTS = {"term", "(", "not"}

    def __init__(self, tokens):
        self.t, self.i = tokens, 0

    def peek(self):
        return self.t[self.i][0] if self.i < len(self.t) else None

    def take(self):
        tok = self.t[self.i]
        self.i += 1
        return tok

    def parse(self):
        if not self.t:
            raise QueryError("The query is empty.")
        node = self.or_()
        if self.peek() is not None:
            kind = self.peek()
            raise QueryError("There is an unmatched closing parenthesis." if kind == ")"
                             else f"Unexpected {kind.upper()} in the query.")
        return node

    def or_(self):
        kids = [self.and_()]
        while self.peek() == "or":
            self.take()
            kids.append(self.and_())
        return kids[0] if len(kids) == 1 else ("or", kids)

    def and_(self):
        kids = [self.near()]
        while True:
            if self.peek() == "and":
                self.take()
                kids.append(self.near())
            elif self.peek() in self.STARTS:
                kids.append(self.near())
            else:
                break
        return kids[0] if len(kids) == 1 else ("and", kids)

    def near(self):
        left = self.unary()
        while self.peek() == "near":
            n = self.take()[1]
            right = self.unary()
            for side in (left, right):
                if not _span_capable(side):
                    raise QueryError("NEAR works only between terms, phrases, or OR groups "
                                     "of terms in the post text.")
            left = ("near", n, left, right)
        return left

    def unary(self):
        kind = self.peek()
        if kind == "not":
            self.take()
            if self.peek() is None:
                raise QueryError("NOT must be followed by a term.")
            return ("not", self.unary())
        if kind == "(":
            self.take()
            if self.peek() is None:
                raise QueryError("A parenthesis is not closed.")
            if self.peek() == ")":
                raise QueryError("Empty parentheses.")
            node = self.or_()
            if self.peek() != ")":
                raise QueryError("A parenthesis is not closed.")
            self.take()
            return node
        if kind == "term":
            field, tkind, value = self.take()[1]
            return ("term", field, tkind, value)
        if kind is None:
            raise QueryError("The query ends with an operator. Add a term after it.")
        raise QueryError(f"Expected a term but found {kind.upper() if kind != ')' else ')'}.")


def _span_capable(node):
    if node[0] == "term":
        return node[1] == "text"
    if node[0] == "or":
        return all(_span_capable(k) for k in node[1])
    return False


def _term_regex(tkind, value):
    if tkind == "regex":
        return re.compile(value, re.I)
    words = normalize_text(value).split()
    parts = ["".join(r"\w*" if ch == "*" else r"\w" if ch == "?" else re.escape(ch)
                     for ch in w) for w in words]
    return re.compile(r"(?<!\w)" + r"\W+".join(parts) + r"(?!\w)")


def _compile(node):
    kind = node[0]
    if kind == "term":
        _, field, tkind, value = node
        return ("term", field, tkind, value, _term_regex(tkind, value))
    if kind in ("and", "or"):
        return (kind, [_compile(k) for k in node[1]])
    if kind == "not":
        return ("not", _compile(node[1]))
    return ("near", node[1], _compile(node[2]), _compile(node[3]))


def parse_query(query):
    """Parse a Boolean query into a compiled tree. Raises QueryError with a readable message."""
    tokens = tokenize_query((query or "").strip())
    return _compile(_Parser(tokens).parse())


def term_label(node):
    _, field, tkind, value, _ = node
    shown = f'"{value}"' if tkind == "phrase" else f"/{value}/" if tkind == "regex" else value
    return shown if field == "text" else f"{field}:{shown}"


def describe_query(node, top=True):
    kind = node[0]
    if kind == "term":
        return term_label(node)
    if kind == "not":
        return "NOT " + describe_query(node[1], False)
    if kind == "near":
        return (f"{describe_query(node[2], False)} NEAR/{node[1]} "
                f"{describe_query(node[3], False)}")
    inner = f" {kind.upper()} ".join(describe_query(k, False) for k in node[1])
    return inner if top else f"({inner})"


class PostRecord:
    """A post as the query engine sees it. Fields are built only when a query asks for them."""

    def __init__(self, text="", channel="", channel_title="", link="", forwarded_from=""):
        self.raw = text if isinstance(text, str) else ""
        self.meta = {"channel": channel, "channel_title": channel_title,
                     "link": link, "forwarded_from": forwarded_from}
        self._fields = {"text": normalize_text(self.raw)}
        self._word_starts = None

    def field(self, name):
        if name not in self._fields:
            m = self.meta
            clean = lambda v: v if isinstance(v, str) else ""
            if name == "channel":
                user = LINK_USER_RE.search(clean(m["link"]))
                value = " \n ".join([clean(m["channel"]), clean(m["channel_title"]),
                                     user.group(1) if user else ""])
            elif name == "from":
                value = clean(m["forwarded_from"])
            elif name == "mention":
                value = " ".join(extract_mentions(
                    self.raw, self_keys(clean(m["channel"]), clean(m["link"]),
                                        clean(m["channel_title"]))))
            else:
                value = " ".join(domain_of(u) for u in extract_urls(self.raw))
            self._fields[name] = normalize_text(value)
        return self._fields[name]

    def word_index(self, pos):
        if self._word_starts is None:
            self._word_starts = [w.start() for w in re.finditer(r"\w+", self._fields["text"])]
        return max(0, bisect_right(self._word_starts, pos) - 1)


def _spans(node, rec):
    if node[0] == "term":
        return [(m.start(), max(m.start(), m.end() - 1))
                for m in node[4].finditer(rec.field("text"))]
    return [s for k in node[1] for s in _spans(k, rec)]


def query_matches(node, rec):
    kind = node[0]
    if kind == "term":
        return node[4].search(rec.field(node[1])) is not None
    if kind == "and":
        return all(query_matches(k, rec) for k in node[1])
    if kind == "or":
        return any(query_matches(k, rec) for k in node[1])
    if kind == "not":
        return not query_matches(node[1], rec)
    n, left, right = node[1], _spans(node[2], rec), _spans(node[3], rec)
    if not left or not right:
        return False
    for a0, a1 in left:
        for b0, b1 in right:
            if a0 > b0:
                (a0, a1), (b0, b1) = (b0, b1), (a0, a1)
            gap = rec.word_index(b0) - rec.word_index(a1) - 1
            if gap <= n:
                return True
    return False


def matched_terms(node, rec, negated=False, out=None):
    """Positive terms from the query that appear in the post, for the spreadsheet."""
    out = [] if out is None else out
    kind = node[0]
    if kind == "term":
        if not negated and node[4].search(rec.field(node[1])):
            label = term_label(node)
            if label not in out:
                out.append(label)
    elif kind == "not":
        matched_terms(node[1], rec, not negated, out)
    elif kind == "near":
        matched_terms(node[2], rec, negated, out)
        matched_terms(node[3], rec, negated, out)
    else:
        for k in node[1]:
            matched_terms(k, rec, negated, out)
    return out


def telegram_search_terms(node):
    """Smallest set of plain terms that Telegram's search must return for any match.

    Returns None when the query cannot be narrowed by Telegram search
    (for example a query that is only NOT terms, field filters or regular expressions).
    """
    found = _search_plan(node)
    return None if found is None else list(dict.fromkeys(t for t, _ in found))


def _search_plan(node):
    """Returns a list of (search text, is_fragment) pairs, or None."""
    kind = node[0]
    if kind == "term":
        _, field, tkind, value, _ = node
        if field != "text" or tkind == "regex":
            return None
        words = [w for w in value.split() if "*" not in w and "?" not in w]
        if words:
            joined = " ".join(words)
            weak = (len(words) < len(value.split()) or "://" in joined
                    or not re.search(r"[^\W\d_]{2}", joined))
            return [(joined, weak)]
        chunks = [c for c in re.split(r"[*?]", value) if len(c) >= 3]
        return [(max(chunks, key=len), True)] if chunks else None
    if kind == "or":
        plans = [_search_plan(k) for k in node[1]]
        return None if any(p is None for p in plans) else [x for p in plans for x in p]
    if kind in ("and", "near"):
        kids = node[1] if kind == "and" else [node[2], node[3]]
        plans = [p for p in (_search_plan(k) for k in kids) if p]
        if not plans:
            return None
        # Prefer whole words over fragments, links and numbers, then fewer searches,
        # then the order the user wrote them in.
        return min(plans, key=lambda p: (any(frag for _, frag in p), len(p)))
    return None


QUERY_HELP = """
**Terms** match whole words, ignoring case (ё and е are treated alike).
`aukus` finds AUKUS and #AUKUS but not "aukusfile".

| Syntax | Meaning | Example |
|---|---|---|
| `a b` or `a AND b` | Both terms | `aukus submarine` |
| `a OR b` | Either term | `aukus OR аукус` |
| `NOT a` or `-a` | Excludes a term | `aukus -"virginia class"` |
| `( )` | Groups terms | `(aukus OR аукус) AND (submarine OR подлодка)` |
| `"..."` | Exact phrase; spacing and punctuation between words are flexible | `"pillar ii"` |
| `*` | Any number of letters | `submarin*`, `*marine` |
| `?` | One letter | `organi?ation` |
| `a NEAR/n b` | Within n words of each other, either order (NEAR alone means 10) | `aukus NEAR/5 cancel*` |
| `/.../` | Regular expression, ignoring case | `/ssn[- ]?aukus/` |
| `channel:` | Posting channel's handle or title | `channel:rybar` |
| `from:` | Forward source | `from:@dva_majors` |
| `mention:` | Telegram accounts named in the post | `mention:@geopolitics_prime` |
| `domain:` | Linked websites, including subdomains | `domain:ria.ru` |

Operators must be in capitals (AND, OR, NOT, NEAR). `&`, `|` and `!` also work.
Without parentheses, NOT applies first, then NEAR, then AND, then OR.
"""


# ═══════════════════════════════════════════════════════════════════
# Link classification: social media accounts vs. websites
# ═══════════════════════════════════════════════════════════════════
# (name, badge, color). YouTube, X and VK get their own colors; every other
# platform shares one color and is identified by its badge and name.
OTHER_SOCIAL_COLOR = "#C2185B"
PLATFORMS = {
    "youtube": ("YouTube", "YT", "#E62117"),
    "x": ("X", "X", "#111111"),
    "vk": ("VK", "VK", "#0077FF"),
    "tiktok": ("TikTok", "TT", OTHER_SOCIAL_COLOR),
    "instagram": ("Instagram", "IG", OTHER_SOCIAL_COLOR),
    "facebook": ("Facebook", "FB", OTHER_SOCIAL_COLOR),
    "rumble": ("Rumble", "RU", OTHER_SOCIAL_COLOR),
    "truthsocial": ("Truth Social", "TS", OTHER_SOCIAL_COLOR),
    "threads": ("Threads", "TH", OTHER_SOCIAL_COLOR),
    "bluesky": ("Bluesky", "BS", OTHER_SOCIAL_COLOR),
    "gab": ("Gab", "GB", OTHER_SOCIAL_COLOR),
    "gettr": ("Gettr", "GT", OTHER_SOCIAL_COLOR),
    "substack": ("Substack", "SS", OTHER_SOCIAL_COLOR),
    "telegraph": ("Telegraph", "TP", OTHER_SOCIAL_COLOR),
    "reddit": ("Reddit", "RD", OTHER_SOCIAL_COLOR),
    "odysee": ("Odysee", "OD", OTHER_SOCIAL_COLOR),
    "bitchute": ("BitChute", "BC", OTHER_SOCIAL_COLOR),
    "rutube": ("Rutube", "RT", OTHER_SOCIAL_COLOR),
    "ok": ("Odnoklassniki", "OK", OTHER_SOCIAL_COLOR),
    "dzen": ("Dzen", "DZ", OTHER_SOCIAL_COLOR),
    "linkedin": ("LinkedIn", "LI", OTHER_SOCIAL_COLOR),
    "twitch": ("Twitch", "TW", OTHER_SOCIAL_COLOR),
}
PLATFORM_HOSTS = {
    "youtube.com": "youtube", "youtu.be": "youtube", "youtube-nocookie.com": "youtube",
    "music.youtube.com": "youtube",
    "x.com": "x", "twitter.com": "x", "fxtwitter.com": "x", "vxtwitter.com": "x",
    "fixupx.com": "x", "fixvx.com": "x",
    "vk.com": "vk", "vk.ru": "vk", "vkvideo.ru": "vk", "vk.cc": "vk",
    "tiktok.com": "tiktok", "vm.tiktok.com": "tiktok", "vt.tiktok.com": "tiktok",
    "instagram.com": "instagram", "instagr.am": "instagram",
    "facebook.com": "facebook", "fb.com": "facebook", "fb.watch": "facebook",
    "fb.me": "facebook",
    "rumble.com": "rumble", "truthsocial.com": "truthsocial",
    "threads.net": "threads", "threads.com": "threads",
    "bsky.app": "bluesky", "gab.com": "gab", "gettr.com": "gettr",
    "substack.com": "substack", "telegra.ph": "telegraph", "graph.org": "telegraph",
    "reddit.com": "reddit", "old.reddit.com": "reddit", "redd.it": "reddit",
    "odysee.com": "odysee", "bitchute.com": "bitchute", "rutube.ru": "rutube",
    "ok.ru": "ok", "dzen.ru": "dzen", "zen.yandex.ru": "dzen",
    "linkedin.com": "linkedin", "lnkd.in": "linkedin", "twitch.tv": "twitch",
}
SHORTENER_HOSTS = {
    "t.co", "bit.ly", "tinyurl.com", "vm.tiktok.com", "vt.tiktok.com", "fb.watch",
    "fb.me", "ow.ly", "buff.ly", "is.gd", "cutt.ly", "rb.gy", "dlvr.it", "vk.cc",
    "clck.ru", "redd.it", "shorturl.at", "trib.al", "lnkd.in", "tiny.cc", "t.ly",
    "goo.su", "s.id", "surl.li",
}
X_RESERVED = {"i", "home", "search", "hashtag", "intent", "share", "explore", "settings",
              "messages", "notifications", "compose", "tos", "privacy", "login", "signup"}
VK_RESERVED = {"away.php", "feed", "im", "search", "video", "videos", "audios", "music",
               "share.php", "doc", "docs", "wall", "photo", "album", "clips", "clip",
               "app", "apps", "friends", "groups", "login", "note", "topic", "market",
               "story", "stories", "@", "reg"}
GENERIC_RESERVED = {"watch", "share", "sharer", "sharer.php", "login", "p", "reel", "reels",
                    "tv", "explore", "home", "search", "hashtag", "tag", "tags", "video",
                    "videos", "embed", "about", "help", "privacy", "terms", "policies",
                    "pages", "events", "photo.php", "story.php", "permalink.php", "posts",
                    "live", "direct", "accounts", "stories", "discover", "trending"}


def normalize_host(host):
    host = (host or "").lower().split("@")[-1].split(":")[0].strip(".")
    for prefix in ("www.", "m.", "mobile.", "mbasic.", "touch.", "new."):
        if host.startswith(prefix) and host.count(".") > 1:
            host = host[len(prefix):]
    return host


def platform_of(host):
    if host in PLATFORM_HOSTS:
        return PLATFORM_HOSTS[host]
    if host.endswith(".substack.com"):
        return "substack"
    for base, plat in PLATFORM_HOSTS.items():
        if host.endswith("." + base):
            return plat
    return None


def social_ref(platform, key, label=None):
    """A social media account. The key comes from the label, so a label saved to a
    spreadsheet and read back identifies the same account."""
    label = label or key
    norm = normalize_text(label).lstrip("@").strip()
    return {"type": "social", "platform": platform, "key": f"{platform}:{norm}",
            "label": label}


def unknown_social(platform, what="account not identified"):
    name = PLATFORMS[platform][0]
    return {"type": "social", "platform": platform, "key": f"{platform}:?",
            "label": f"{name} ({what})", "what": what, "unknown": True}


def classify_link(url):
    """Classify one URL.

    Returns a dict with type "telegram", "social" or "website", or None for
    Telegram links (those are handled as mentions). Social refs carry the
    platform and account; "resolvable" marks links a web lookup can identify.
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    host = normalize_host(parsed.netloc)
    if not host or host in TELEGRAM_DOMAINS:
        return None
    path = [unquote(p) for p in parsed.path.split("/") if p]
    first = path[0] if path else ""
    low = first.lower()
    query = parse_qs(parsed.query)
    plat = platform_of(host)

    if plat is None:
        if host in SHORTENER_HOSTS:
            return {"type": "website", "key": f"web:{host}", "label": host, "resolvable": True}
        return {"type": "website", "key": f"web:{host}", "label": host}

    ref = None
    if plat == "youtube":
        if host == "youtu.be":
            ref = dict(unknown_social("youtube", "channel not identified"), resolvable=True)
        elif low.startswith("@") and len(first) > 1:
            ref = social_ref("youtube", first, first)
        elif low == "channel" and len(path) > 1:
            ref = social_ref("youtube", path[1], f"channel {path[1]}")
        elif low in ("c", "user") and len(path) > 1:
            ref = social_ref("youtube", path[1], path[1])
        elif low in ("watch", "shorts", "live", "embed", "v") or "v" in query:
            ref = dict(unknown_social("youtube", "channel not identified"), resolvable=True)
        elif first and low not in GENERIC_RESERVED and low not in ("playlist", "results",
                                                                   "feed", "redirect"):
            ref = social_ref("youtube", first, first)
    elif plat == "x":
        if first and low not in X_RESERVED and re.fullmatch(r"[A-Za-z0-9_]{1,15}", first):
            ref = social_ref("x", first, f"@{first}")
    elif plat == "vk":
        if host == "vk.cc":
            ref = dict(unknown_social("vk"), resolvable=True)
        else:
            owner = re.match(r"(?:wall|video|photo|clip|audio|album|topic|market)(-?\d+)_",
                             low) or re.fullmatch(r"(?:club|public|event)(\d+)", low)
            if owner:
                num = owner.group(1)
                if low.startswith(("club", "public", "event")) or num.startswith("-"):
                    ref = social_ref("vk", f"club{num.lstrip('-')}",
                                     f"community {num.lstrip('-')}")
                else:
                    ref = social_ref("vk", f"id{num}", f"user {num}")
            elif re.fullmatch(r"id\d+", low):
                ref = social_ref("vk", low, f"user {low[2:]}")
            elif "w" in query:
                m = re.match(r"(?:wall|video)(-?\d+)_", query["w"][0])
                if m:
                    num = m.group(1)
                    ref = (social_ref("vk", f"club{num[1:]}", f"community {num[1:]}")
                           if num.startswith("-") else social_ref("vk", f"id{num}", f"user {num}"))
            elif first and low not in VK_RESERVED and re.fullmatch(r"[a-z0-9_.]{2,}", low):
                ref = social_ref("vk", low, low)
    elif plat == "tiktok":
        if low.startswith("@") and len(first) > 1:
            ref = social_ref("tiktok", first, first)
        elif host in SHORTENER_HOSTS or low == "t":
            ref = dict(unknown_social("tiktok"), resolvable=True)
    elif plat == "instagram":
        if low == "stories" and len(path) > 1:
            ref = social_ref("instagram", path[1], f"@{path[1]}")
        elif first and low not in GENERIC_RESERVED:
            ref = social_ref("instagram", first, f"@{first}")
    elif plat == "facebook":
        if host in ("fb.watch", "fb.me"):
            ref = dict(unknown_social("facebook"), resolvable=True)
        elif low == "profile.php" and "id" in query:
            ref = social_ref("facebook", f"id{query['id'][0]}", f"profile {query['id'][0]}")
        elif low == "groups" and len(path) > 1:
            ref = social_ref("facebook", f"groups/{path[1]}", f"group {path[1]}")
        elif low == "people" and len(path) > 1:
            ref = social_ref("facebook", path[1], path[1])
        elif first and low not in GENERIC_RESERVED and low not in ("share", "watch", "story",
                                                                   "photo", "l.php"):
            ref = social_ref("facebook", first, first)
    elif plat == "rumble":
        if low in ("c", "user") and len(path) > 1:
            ref = social_ref("rumble", path[1], path[1])
        elif re.match(r"v[a-z0-9]+-", low) or low == "embed":
            ref = dict(unknown_social("rumble", "channel not identified"), resolvable=True)
    elif plat in ("truthsocial", "threads", "odysee"):
        if low.startswith("@") and len(first) > 1:
            name = first.split(":")[0]
            ref = social_ref(plat, name, name)
    elif plat == "bluesky":
        if low == "profile" and len(path) > 1:
            ref = social_ref("bluesky", path[1], f"@{path[1]}")
    elif plat in ("gab", "twitch"):
        if first and low not in GENERIC_RESERVED and low not in ("groups", "directory"):
            ref = social_ref(plat, first, f"@{first}" if plat == "gab" else first)
    elif plat == "gettr":
        if low == "user" and len(path) > 1:
            ref = social_ref("gettr", path[1], f"@{path[1]}")
    elif plat == "substack":
        sub = host[: -len(".substack.com")] if host.endswith(".substack.com") else ""
        if sub and sub not in ("open", "on", "support"):
            ref = social_ref("substack", sub, sub)
        elif low.startswith("@") and len(first) > 1:
            ref = social_ref("substack", first, first)
    elif plat == "telegraph":
        if first:
            ref = dict(unknown_social("telegraph", "author not identified"), resolvable=True)
    elif plat == "reddit":
        if low == "r" and len(path) > 1:
            ref = social_ref("reddit", f"r/{path[1]}", f"r/{path[1]}")
        elif low in ("u", "user") and len(path) > 1:
            ref = social_ref("reddit", f"u/{path[1]}", f"u/{path[1]}")
        elif host == "redd.it":
            ref = dict(unknown_social("reddit"), resolvable=True)
    elif plat == "bitchute":
        if low == "channel" and len(path) > 1:
            ref = social_ref("bitchute", path[1], path[1])
    elif plat == "rutube":
        if low == "channel" and len(path) > 1:
            ref = social_ref("rutube", path[1], f"channel {path[1]}")
        elif low in ("u", "c") and len(path) > 1:
            ref = social_ref("rutube", path[1], path[1])
    elif plat == "ok":
        if low in ("group", "profile") and len(path) > 1:
            ref = social_ref("ok", f"{low}/{path[1]}", f"{low} {path[1]}")
        elif first and low not in GENERIC_RESERVED and low not in ("dk", "game", "app"):
            ref = social_ref("ok", first, first)
    elif plat == "dzen":
        if low == "id" and len(path) > 1:
            ref = social_ref("dzen", path[1], f"channel {path[1]}")
        elif first and low not in GENERIC_RESERVED and low not in ("a", "b", "news", "media"):
            ref = social_ref("dzen", first, first)
    elif plat == "linkedin":
        if low in ("in", "company", "school") and len(path) > 1:
            ref = social_ref("linkedin", f"{low}/{path[1]}", path[1])
        elif host == "lnkd.in":
            ref = dict(unknown_social("linkedin"), resolvable=True)
    return ref or unknown_social(plat)


def platform_name(platform):
    return PLATFORMS[platform][0]


def ref_text(ref):
    """How a reference appears in spreadsheet columns, e.g. 'YouTube: @handle'."""
    if ref["type"] == "social":
        return f"{platform_name(ref['platform'])}: {ref.get('what') or ref['label']}"
    if ref["type"] == "telegram":
        return f"Telegram: {ref['label']}"
    return f"Website: {ref['label']}"


# ── Optional web lookups ────────────────────────────────────────────
LOOKUP_TIMEOUT = 8
LOOKUP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TelegramChannelCollector/1.0)"}
OEMBED = {
    "youtube": "https://www.youtube.com/oembed?format=json&url=",
    "rumble": "https://rumble.com/api/Media/oembed.json?url=",
}


def _http_get(url, want_json=False):
    req = urllib.request.Request(url, headers=LOOKUP_HEADERS)
    with urllib.request.urlopen(req, timeout=LOOKUP_TIMEOUT) as resp:
        final = resp.geturl()
        body = resp.read(200_000)
    if want_json:
        return final, json.loads(body.decode("utf-8", "replace"))
    text = body.decode("utf-8", "replace")
    refresh = re.search(r'http-equiv=["\']?refresh["\']?[^>]*url=([^"\'>]+)', text, re.I)
    return (refresh.group(1).strip() if refresh else final), None


def lookup_link(url, depth=0):
    """Try to identify the account behind a link. Returns a ref dict or None.

    Performs web requests: oEmbed for YouTube and Rumble videos, the public
    Telegraph API for article authors, and redirect-following for short links.
    """
    ref = classify_link(url)
    if not ref or not ref.get("resolvable") or depth > 2:
        return None
    try:
        plat = ref.get("platform")
        host = normalize_host(urlparse(url).netloc)
        if plat in OEMBED and host not in SHORTENER_HOSTS:
            _, data = _http_get(OEMBED[plat] + quote(url, safe=""), want_json=True)
            author_url = (data or {}).get("author_url", "")
            found = classify_link(author_url) if author_url else None
            if found and not found.get("unknown"):
                if plat == "youtube" and found["label"].startswith("channel ") \
                        and data.get("author_name"):
                    return social_ref("youtube", data["author_name"])
                return found
            name = (data or {}).get("author_name")
            return social_ref(plat, name, name) if name else None
        if plat == "telegraph":
            slug = urlparse(url).path.strip("/")
            _, data = _http_get(f"https://api.telegra.ph/getPage/{quote(slug)}", want_json=True)
            page = (data or {}).get("result") or {}
            author_url = page.get("author_url") or ""
            if author_url:
                found = classify_link(author_url)
                if found is None:
                    handle = TG_RE.search(author_url)
                    if handle:
                        return {"type": "telegram", "key": handle.group(1).lower(),
                                "label": f"@{handle.group(1)}"}
                elif not found.get("unknown"):
                    return found
            name = page.get("author_name")
            return social_ref("telegraph", name, name) if name else None
        final, _ = _http_get(url)
        if final and final != url:
            tg = TG_RE.search(final)
            if tg and normalize_host(urlparse(final).netloc) in TELEGRAM_DOMAINS:
                return {"type": "telegram", "key": tg.group(1).lower(),
                        "label": f"@{tg.group(1)}"}
            found = classify_link(final)
            if found and found.get("resolvable"):
                return lookup_link(final, depth + 1)
            if found and not found.get("unknown"):
                return found
    except Exception:
        return None
    return None


@lru_cache(maxsize=50_000)
def cached_lookup(url):
    return lookup_link(url)


def resolve_links(urls, progress=None, workers=8):
    """Look up many links in parallel. Returns {url: ref} for links that were identified."""
    urls = list(dict.fromkeys(urls))
    found = {}
    if not urls:
        return found
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, (url, ref) in enumerate(zip(urls, pool.map(cached_lookup, urls)), start=1):
            if ref:
                found[url] = ref
            if progress:
                progress(i, len(urls))
    return found


def resolvable_urls(texts):
    out = []
    for text in texts:
        for url in extract_urls(text if isinstance(text, str) else ""):
            ref = classify_link(url)
            if ref and ref.get("resolvable"):
                out.append(url)
    return list(dict.fromkeys(out))


RESOLVED_SEP = " → "


def format_resolved(found):
    return "\n".join(f"{u}{RESOLVED_SEP}{ref_text(r)}" for u, r in found.items())


def parse_resolved(cell):
    """Read a resolved_links cell back into {url: ref}."""
    out = {}
    if not isinstance(cell, str):
        return out
    names = {v[0]: k for k, v in PLATFORMS.items() if not k.endswith("_")}
    for line in cell.splitlines():
        if RESOLVED_SEP not in line:
            continue
        url, desc = line.split(RESOLVED_SEP, 1)
        kind, _, label = desc.partition(": ")
        if kind == "Telegram":
            out[url.strip()] = {"type": "telegram", "key": handle_key(label), "label": label}
        elif kind == "Website":
            out[url.strip()] = {"type": "website", "key": f"web:{label}", "label": label}
        elif kind in names and "not identified" not in label:
            out[url.strip()] = social_ref(names[kind], label)
    return out


def post_references(text, channel="", link="", title="", resolved=None):
    """Every referral target in a post's text, deduplicated, in order of appearance."""
    text = text if isinstance(text, str) else ""
    resolved = resolved or {}
    mine = self_keys(channel, link, title)
    refs, seen = [], set()

    def add(ref):
        if ref["key"] in seen or ref["key"] in mine:
            return
        seen.add(ref["key"])
        refs.append(ref)

    for mention in extract_mentions(text, mine):
        add({"type": "telegram", "key": handle_key(mention), "label": mention})
    for url in extract_urls(text):
        ref = resolved.get(url) or classify_link(url)
        if ref:
            add(ref)
    return refs


# ═══════════════════════════════════════════════════════════════════
# Referral analysis
# ═══════════════════════════════════════════════════════════════════
UNRESOLVED_RE = re.compile(r"(?:channel_id|user_id|chat_id)=(\d+)")
TYPES = ["mention", "forward", "social", "website"]
TYPE_NAMES = {"mention": "mention", "forward": "forward", "social": "social media link",
              "website": "website link"}
TYPE_HEX = {"mention": "#8E44AD", "forward": "#F2994A", "social": OTHER_SOCIAL_COLOR,
            "website": "#27AE60"}
KINDS = ["telegram", "social", "website"]
KIND_TITLES = {"telegram": "Telegram channels", "social": "Social media accounts",
               "website": "Websites"}
HUB_COLORS = {"telegram": "#1F4FD1", "website": "#27AE60"}
NODE_COLOR = "rgba(120, 120, 120, 0.85)"
TELEGRAM_DOMAINS = {"t.me", "telegram.me", "telegram.dog"}


def domain_of(url):
    try:
        host = urlparse(url).netloc.lower().split("@")[-1].split(":")[0]
    except Exception:
        return ""
    return host[4:] if host.startswith("www.") else host


def line_color(kind_type, platform=""):
    if kind_type == "social" and platform in PLATFORMS:
        return PLATFORMS[platform][2]
    return TYPE_HEX[kind_type]


def key_kind(key):
    if key.startswith("web:"):
        return "website"
    prefix = key.split(":", 1)[0]
    return "social" if ":" in key and prefix in PLATFORMS else "telegram"


def key_platform(key):
    return key.split(":", 1)[0] if key_kind(key) == "social" else ""


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


def is_forward_row(row):
    fwd = row.get("forwarded_from")
    if isinstance(fwd, str) and fwd.strip():
        return True
    flag = row.get("is_forward")
    return bool(flag) if isinstance(flag, (bool, int)) and not pd.isna(flag) else False


def referral_edges(posts, extra_resolved=None):
    """One row per (post, referred account, type).

    An original post refers its audience to every account and website it names.
    A forward refers its audience to the forward source only; references inside the
    forwarded content are kept as separate "via forward" rows.
    """
    extra_resolved = extra_resolved or {}
    records = posts.to_dict("records")
    labels, title_to_key = {}, {}
    for r in records:
        key = handle_key(r.get("channel"))
        user = LINK_USER_RE.search(str(r.get("link") or ""))
        labels.setdefault(key, f"@{user.group(1)}" if user else f"@{r.get('channel')}")
        title = r.get("channel_title")
        if isinstance(title, str) and title.strip():
            title_to_key.setdefault(title.strip().lower(), key)

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

    shorts, platforms = {}, {}
    rows = []
    for uid, r in enumerate(records):
        text = r.get("text") if isinstance(r.get("text"), str) else ""
        channel, link = r.get("channel"), r.get("link") if isinstance(r.get("link"), str) else ""
        title = r.get("channel_title") if isinstance(r.get("channel_title"), str) else ""
        views = pd.to_numeric(r.get("views"), errors="coerce")
        base = {"post_uid": uid, "referrer_key": handle_key(channel),
                "date": r.get("date"), "views": 0 if pd.isna(views) else float(views),
                "post_link": link}
        mine = self_keys(channel, link, title)
        resolved = dict(parse_resolved(r.get("resolved_links")))
        resolved.update(extra_resolved)
        refs = post_references(text, channel, link, title, resolved)
        via = False
        if is_forward_row(r):
            fwd = r.get("forwarded_from")
            if isinstance(fwd, str) and fwd.strip():
                key, label = source_node(fwd)
                if key not in mine and key.removeprefix("name:") not in mine:
                    labels.setdefault(key, label)
                    rows.append({**base, "referred_key": key, "type": "forward",
                                 "platform": "", "via": False})
                    refs = [x for x in refs if x["key"] != key]
            via = True
        for ref in refs:
            key = ref["key"]
            if ref["type"] == "social":
                plat = ref["platform"]
                platforms[key] = plat
                shorts.setdefault(key, ref["label"])
                shown = (ref["label"] if ref.get("unknown")
                         else f"{ref['label']} · {platform_name(plat)}")
                labels.setdefault(key, shown)
                kind_type = "social"
            else:
                labels.setdefault(key, ref["label"])
                kind_type = "mention" if ref["type"] == "telegram" else "website"
                plat = ""
            rows.append({**base, "referred_key": key, "type": kind_type,
                         "platform": plat, "via": via})

    cols = ["post_uid", "referrer_key", "referred_key", "type", "platform", "via",
            "date", "views", "post_link"]
    edges = pd.DataFrame(rows, columns=cols)
    edges["date"] = pd.to_datetime(edges["date"], errors="coerce", utc=True)
    edges["via"] = edges["via"].astype(bool)
    edges["referrer"] = edges["referrer_key"].map(labels)
    edges["referred"] = edges["referred_key"].map(labels)
    edges["referred_short"] = edges["referred_key"].map(lambda k: shorts.get(k, labels.get(k)))
    edges["referred_kind"] = edges["referred_key"].map(key_kind)
    return edges


@st.cache_data(max_entries=8, show_spinner=False)
def cached_edges(posts, resolved_json):
    extra = {u: r for u, r in json.loads(resolved_json).items()}
    return referral_edges(posts, extra)


def summarize_edges(edges):
    if edges.empty:
        return pd.DataFrame(columns=["referrer_key", "referrer", "referred_key", "referred",
                                     "referred_short", "referred_kind", "type", "platform",
                                     "via", "posts", "views", "first_seen", "last_seen",
                                     "example_post"])
    g = edges.groupby(["referrer_key", "referrer", "referred_key", "referred",
                       "referred_short", "referred_kind", "type", "platform", "via"],
                      dropna=False)
    return g.agg(posts=("post_uid", "nunique"), views=("views", "sum"),
                 first_seen=("date", "min"), last_seen=("date", "max"),
                 example_post=("post_link", "first")).reset_index()


def counterparty_table(summary, side_key, other_key, other_label, focus):
    part = summary[summary[side_key] == focus]
    if part.empty:
        return pd.DataFrame()
    part = part.assign(column=lambda d: d.apply(
        lambda r: "via_forwards" if r["via"] else {
            "mention": "mentions", "forward": "forwards", "social": "social_links",
            "website": "website_links"}[r["type"]], axis=1))
    wide = part.pivot_table(index=[other_key, other_label], columns="column",
                            values="posts", aggfunc="sum", fill_value=0)
    count_cols = ["mentions", "forwards", "social_links", "website_links", "via_forwards"]
    for c in count_cols:
        if c not in wide.columns:
            wide[c] = 0
    direct = part[~part["via"]]
    extra = part.groupby([other_key, other_label]).agg(
        first_seen=("first_seen", "min"), last_seen=("last_seen", "max"),
        example_post=("example_post", "first"))
    views = direct.groupby([other_key, other_label])["views"].sum().rename("views")
    table = wide[count_cols].join(views).join(extra).reset_index()
    table["views"] = table["views"].fillna(0)
    table["total"] = table[count_cols[:-1]].sum(axis=1)
    table = table.rename(columns={other_label: "account"})
    keep = ["account", "total"] + [c for c in count_cols if table[c].any()] + [
        "views", "first_seen", "last_seen", "example_post"]
    return (table[keep].sort_values(["total", "views"], ascending=False)
            .reset_index(drop=True))


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
    return (part.groupby([key_col, label_col, "type", "platform"], as_index=False)
            .agg(posts=("posts", "sum"), views=("views", "sum")))


def short(label, limit=40):
    return label if len(label) <= limit else label[: limit - 1] + "…"


def compact(n):
    n = float(n)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= div:
            return f"{n / div:.1f}".rstrip("0").rstrip(".") + suffix
    return f"{n:,.0f}"


def plural(n, word):
    suffix = "" if n == 1 else ("es" if word.endswith(("ch", "sh", "s", "x")) else "s")
    return f"{n:,} {word}{suffix}"


def fit(text, px, size, bold=False):
    limit = max(4, int(px / (size * (0.68 if bold else 0.58))))
    return text if len(text) <= limit else text[: limit - 1] + "…"


def rgba(hex_color, alpha):
    h = hex_color.lstrip("#")
    return f"rgba({int(h[0:2], 16)}, {int(h[2:4], 16)}, {int(h[4:6], 16)}, {alpha})"


# ── One-channel diagram ─────────────────────────────────────────────
def ego_cards(summary, edges, focus, side, weight, limits, show_via):
    if side == "in":
        part = summary[summary["referred_key"] == focus]
        key, label = "referrer_key", "referrer"
        ids = edges.loc[(edges["referred_key"] == focus) & ~edges["via"], "post_uid"]
    else:
        part = summary[summary["referrer_key"] == focus]
        key, label = "referred_key", "referred_short"
        ids = edges.loc[(edges["referrer_key"] == focus) & ~edges["via"], "post_uid"]
    if not show_via:
        part = part[~part["via"]]
    direct = part[~part["via"]]
    named = direct[~direct[key].str.endswith(":?")]
    counts = {k: int(named.loc[named[key].map(key_kind) == k, key].nunique()) for k in KINDS}
    totals = {"counts": counts, "posts": int(ids.nunique()), "ids": set(ids)}
    sections = {k: [] for k in KINDS}
    if part.empty:
        return sections, totals

    def card(rows, name, kind, other=False, unknown=False):
        lines = (rows.groupby(["type", "platform", "via"], dropna=False)[["posts", "views"]]
                 .sum().reset_index())
        plats = [p for p in rows["platform"].unique() if p]
        if unknown and "(" in name:
            name = name[name.index("(") + 1:].rstrip(")").capitalize()
        return {"label": name, "kind": kind, "other": other, "unknown": unknown,
                "platform": plats[0] if len(plats) == 1 else "",
                "lines": [dict(r) for r in lines.to_dict("records")]}

    part = part.assign(_kind=part[key].map(key_kind))
    for kind in KINDS:
        grp = part[part["_kind"] == kind]
        n = limits.get(kind, 10)
        if grp.empty or n == 0:
            continue
        direct_w = grp[~grp["via"]].groupby(key)[weight].sum()
        via_w = grp[grp["via"]].groupby(key)[weight].sum()
        names = grp.groupby(key)[label].first()
        order = pd.DataFrame({"d": direct_w, "v": via_w}).fillna(0)
        order["unknown"] = [k.endswith(":?") for k in order.index]
        order = order.sort_values(["unknown", "d", "v"], ascending=[True, False, False])
        shown, rest = list(order.index[:n]), list(order.index[n:])
        for k in shown:
            sections[kind].append(card(grp[grp[key] == k], names[k], kind,
                                       unknown=k.endswith(":?")))
        if rest:
            noun = {"telegram": "channel", "social": "account", "website": "website"}[kind]
            sections[kind].append(card(grp[grp[key].isin(rest)],
                                       other_label(len(rest), noun), kind, other=True))
    return sections, totals


def card_stats(card, weight):
    direct = [l for l in card["lines"] if not l["via"]]
    via_posts = sum(l["posts"] for l in card["lines"] if l["via"])
    by_type = {}
    for l in direct:
        by_type[l["type"]] = by_type.get(l["type"], 0) + l["posts"]
    parts = []
    if weight == "views":
        parts.append(f"{compact(sum(l['views'] for l in direct))} views")
        if direct:
            parts.append(plural(sum(by_type.values()), "post"))
    else:
        for t in ("mention", "forward"):
            if by_type.get(t):
                parts.append(plural(by_type[t], t))
        links = by_type.get("social", 0) + by_type.get("website", 0)
        if links:
            parts.append(plural(links, "link"))
    if via_posts:
        parts.append(f"{via_posts:,} via forward{'' if via_posts == 1 else 's'}")
    prefix = ""
    if card["kind"] == "social" and card["platform"]:
        prefix = platform_name(card["platform"]) + " · "
    return prefix + " · ".join(parts)


def ego_svg(summary, edges, focus, focus_label, weight, limits, collected=True,
            show_via=False):
    left, tin = ego_cards(summary, edges, focus, "in", weight, limits, show_via)
    right, tout = ego_cards(summary, edges, focus, "out", weight, limits, show_via)
    focus_kind = key_kind(focus)

    W, CW, CH, GAP, R, LABEL_H = 1100, 300, 64, 14, 74, 30
    LX, RX, HX = 12, W - 12 - CW, W / 2
    TOP = 112

    def column(sections):
        items = []
        for kind in KINDS:
            if sections[kind]:
                items.append(("label", KIND_TITLES[kind]))
                items += [("card", c) for c in sections[kind]]
        return items

    left_items, right_items = column(left), column(right)

    def height_of(items):
        return sum(LABEL_H if kind == "label" else CH + GAP for kind, _ in items)

    body = max(height_of(left_items), height_of(right_items), 3 * (CH + GAP))
    HY = TOP + body / 2
    BOX_H, BOX_GAP = 58, 10
    box_top = TOP + body + 34
    right_boxes = 4 if collected and focus_kind == "telegram" else 0
    H = box_top + max(2, right_boxes) * (BOX_H + BOX_GAP) + 6

    idx = 0 if weight == "posts" else 1
    values = [l[weight] for _, c in left_items + right_items if isinstance(c, dict)
              for l in c["lines"]]
    vmax = max(values) if values else 1

    def stroke(v):
        return 1.5 + 22 * sqrt(v / vmax) if vmax > 0 else 1.5

    curves, cards_svg, used = [], [], set()
    for side, items, x0 in (("in", left_items, LX), ("out", right_items, RX)):
        y = TOP + (body - height_of(items)) / 2
        for kind, c in items:
            if kind == "label":
                anchor = "end" if side == "in" else "start"
                lx = x0 + CW if side == "in" else x0
                cards_svg.append(
                    f'<text x="{lx}" y="{y + 20}" font-size="12" font-weight="bold" '
                    f'fill="#8A96A8" text-anchor="{anchor}" letter-spacing="0.5">'
                    f'{escape(c)}</text>')
                y += LABEL_H
                continue
            top, mid = y, y + CH / 2
            y += CH + GAP
            lines = sorted(c["lines"], key=lambda l: (l["via"], TYPES.index(l["type"]),
                                                      l["platform"]))
            widths = [stroke(l[weight]) for l in lines]
            span = sum(widths) + 2 * (len(widths) - 1)
            offset = mid - span / 2
            for l, w in zip(lines, widths):
                ya = offset + w / 2
                offset += w + 2
                yb = HY + (ya - mid) * 0.35
                xa, xb = (x0 + CW, HX - R + 4) if side == "in" else (x0, HX + R - 4)
                xc = (xa + xb) / 2
                color = line_color(l["type"], l["platform"])
                used.add((l["type"], l["platform"]))
                if l["via"]:
                    used.add(("via", ""))
                what = (platform_name(l["platform"]) + " link" if l["type"] == "social"
                        else TYPE_NAMES[l["type"]])
                via_note = " inside forwarded posts" if l["via"] else ""
                tip = (f"{c['label']}: {plural(int(l['posts']), what)}{via_note}, "
                       f"{l['views']:,.0f} views")
                dash = ' stroke-dasharray="7 6"' if l["via"] else ""
                opacity = 0.38 if l["via"] else 0.8
                curves.append(
                    f'<path d="M{xa:.1f},{ya:.1f} C{xc:.1f},{ya:.1f} {xc:.1f},{yb:.1f} '
                    f'{xb:.1f},{yb:.1f}" fill="none" stroke="{color}" '
                    f'stroke-opacity="{opacity}" stroke-width="{w:.1f}"{dash}>'
                    f'<title>{escape(tip)}</title></path>')

            stats = card_stats(c, weight)
            if c["other"]:
                fill = "#EDF0F4"
            else:
                fill = {"telegram": "#FFFFFF", "social": "#F6F7FA",
                        "website": "#EAF7EF"}[c["kind"]]
            if c["kind"] == "social" and c["platform"]:
                badge_text = PLATFORMS[c["platform"]][1]
                badge_color = PLATFORMS[c["platform"]][2]
            elif c["kind"] == "website":
                badge_text, badge_color = c["label"][:1].upper(), "#27AE60"
            else:
                badge_text = c["label"].lstrip("@")[:1].upper() or "?"
                badge_color = "#8A96A8"
            if c["other"]:
                badge_text = "…"
            badge_x = x0 + CW - 34 if side == "in" else x0 + 34
            text_x = x0 + 16 if side == "in" else x0 + 66
            size = 17 if len(badge_text) == 1 else 13
            tip = f"{c['label']}\n{stats}"
            cards_svg.append(
                f'<g><title>{escape(tip)}</title>'
                f'<rect x="{x0}" y="{top}" width="{CW}" height="{CH}" rx="6" fill="{fill}" '
                f'stroke="#B8C2CE" stroke-width="1"/>'
                f'<circle cx="{badge_x}" cy="{mid}" r="20" fill="{badge_color}"/>'
                f'<text x="{badge_x}" y="{mid + size / 3 + 0.5:.1f}" font-size="{size}" '
                f'font-weight="bold" fill="#FFFFFF" text-anchor="middle">'
                f'{escape(badge_text)}</text>'
                f'<text x="{text_x}" y="{top + 27}" font-size="15.5" fill="#2D3748">'
                f'{escape(fit(c["label"], CW - 70, 15.5))}</text>'
                f'<text x="{text_x}" y="{top + 48}" font-size="12.5" fill="#718096">'
                f'{escape(fit(stats, CW - 70, 12.5))}</text></g>')

    if focus_kind != "telegram":
        right_msg = [("Websites" if focus_kind == "website" else "Social media accounts")
                     + " are not collected,", "so their outgoing referrals are unknown"]
    elif not collected:
        right_msg = ["Not a collected channel,", "so outgoing referrals are unknown"]
    else:
        right_msg = ["No outgoing referrals", "in this data"]
    for items, x0, msg in ((left_items, LX, ["No incoming referrals", "in this data"]),
                           (right_items, RX, right_msg)):
        if not items:
            for j, line in enumerate(msg):
                cards_svg.append(f'<text x="{x0 + CW / 2}" y="{HY - 8 + j * 20}" '
                                 f'font-size="14" fill="#94A0B0" text-anchor="middle">'
                                 f'{escape(line)}</text>')

    if focus_kind == "social":
        hub_color = PLATFORMS[key_platform(focus)][2]
    else:
        hub_color = HUB_COLORS[focus_kind]
    name = fit(focus_label, 2 * R - 20, 14, bold=True)
    n_hub = len(tin["ids"] | tout["ids"])
    hub = (f'<circle cx="{HX}" cy="{HY}" r="{R + 6}" fill="#FFFFFF" stroke="#D5DBE3"/>'
           f'<circle cx="{HX}" cy="{HY}" r="{R}" fill="{hub_color}"/>'
           f'<text x="{HX}" y="{HY + 2}" font-size="14" font-weight="bold" fill="#FFFFFF" '
           f'text-anchor="middle">{escape(name)}</text>'
           f'<text x="{HX}" y="{HY + 22}" font-size="11.5" fill="#F0F3FA" '
           f'text-anchor="middle">{escape(plural(n_hub, "post"))}</text>'
           f'<title>{escape(focus_label)}</title>')

    def box(x, y, label, value, align):
        tx = x + CW - 16 if align == "end" else x + 16
        return (f'<rect x="{x}" y="{y}" width="{CW}" height="{BOX_H}" rx="4" fill="#EEF1F5" '
                f'stroke="#8C96A3"/>'
                f'<text x="{tx}" y="{y + 22}" font-size="13.5" fill="#4A5568" '
                f'text-anchor="{align}">{escape(label)}</text>'
                f'<text x="{tx}" y="{y + 47}" font-size="22" fill="#2D3748" '
                f'text-anchor="{align}">{value:,}</text>')

    step = BOX_H + BOX_GAP
    boxes = [box(LX, box_top, "Telegram channels referring", tin["counts"]["telegram"], "end"),
             box(LX, box_top + step, "Posts with referrals", tin["posts"], "end")]
    if right_boxes:
        boxes += [
            box(RX, box_top, "Telegram channels referred to", tout["counts"]["telegram"],
                "start"),
            box(RX, box_top + step, "Social media accounts identified",
                tout["counts"]["social"], "start"),
            box(RX, box_top + 2 * step, "Websites linked", tout["counts"]["website"], "start"),
            box(RX, box_top + 3 * step, "Posts with referrals", tout["posts"], "start")]

    legend_items = [(("mention", ""), "Telegram mention", TYPE_HEX["mention"], False),
                    (("forward", ""), "Telegram forward", TYPE_HEX["forward"], False)]
    for plat in ("youtube", "x", "vk"):
        legend_items.append((("social", plat), platform_name(plat), PLATFORMS[plat][2], False))
    others = {p for t, p in used if t == "social" and p not in ("youtube", "x", "vk")}
    legend_items.append((("social", "*"), "Other social media", OTHER_SOCIAL_COLOR, False))
    legend_items.append((("website", ""), "Website", TYPE_HEX["website"], False))
    legend_items.append((("via", ""), "Inside forwarded posts", "#8A96A8", True))

    def in_use(k):
        if k == ("social", "*"):
            return bool(others)
        return k in used

    shown = [it for it in legend_items if in_use(it[0])]
    legend, lx = [], 12
    for _, text, color, dashed in shown:
        dash = ' stroke-dasharray="7 5"' if dashed else ""
        legend.append(f'<line x1="{lx}" y1="30" x2="{lx + 26}" y2="30" stroke="{color}" '
                      f'stroke-width="6"{dash}/>'
                      f'<text x="{lx + 33}" y="35" font-size="12.5" fill="#4A5568">'
                      f'{escape(text)}</text>')
        lx += 45 + len(text) * 7.2
    width_note = ("Line width and order: number of posts" if weight == "posts"
                  else "Line width and order: views")
    headers = (f'<text x="12" y="60" font-size="11.5" fill="#94A0B0">{width_note}</text>'
               f'<text x="{LX}" y="92" font-size="15" font-weight="bold" fill="#2D3748">'
               f'Incoming referrals</text>'
               f'<text x="{RX + CW}" y="92" font-size="15" font-weight="bold" fill="#2D3748" '
               f'text-anchor="end">Outgoing referrals</text>')

    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H:.0f}" '
            f'width="{W}" height="{round(H)}" '
            f'font-family="DejaVu Sans, Helvetica, Arial, sans-serif">'
            f'<rect width="{W}" height="{H:.0f}" fill="#FFFFFF"/>'
            + "".join(legend) + headers + "".join(curves) + "".join(cards_svg) + hub
            + "".join(boxes) + "</svg>"), H


def svg_to_png(svg):
    return bytes(resvg_py.svg_to_bytes(svg_string=svg, zoom=2, background="#ffffff"))


# ── All-channels views ──────────────────────────────────────────────
def build_sankey(nodes, links, weight, height):
    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(label=[short(n[0]) for n in nodes], color=[n[1] for n in nodes],
                  pad=12, thickness=16, customdata=[n[0] for n in nodes],
                  hovertemplate="%{customdata}<br>%{value:,.0f} " + weight + "<extra></extra>"),
        link=dict(
            source=[l["source"] for l in links], target=[l["target"] for l in links],
            value=[l["value"] for l in links],
            color=[rgba(line_color(l["type"], l["platform"]), 0.5) for l in links],
            customdata=[[l["what"], l["posts"], l["views"]] for l in links],
            hovertemplate=("%{source.customdata} → %{target.customdata}<br>"
                           "%{customdata[0]}: %{customdata[1]:,} posts, "
                           "%{customdata[2]:,.0f} views<extra></extra>")),
    ))
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=10, b=10), font_size=12)
    return fig


def overview_sankey(summary, weight, n_sources, n_targets):
    """Collected channels on the left, the accounts they refer to on the right."""
    cols = ["referrer_key", "referrer", "referred_key", "referred", "type", "platform"]
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
    right = agg.assign(kind_order=agg["referred_key"].map(
        lambda k: KINDS.index(key_kind(k)) if not k.startswith("__") else 3))
    right = (right.groupby(["referred_key", "referred", "kind_order"])[weight].sum()
             .reset_index().sort_values(["kind_order", weight], ascending=[True, False]))
    nodes = [(lbl, NODE_COLOR) for _, lbl in left.index]
    li = {k: i for i, (k, _) in enumerate(left.index)}
    ri = {}
    for r in right.itertuples(index=False):
        ri[r.referred_key] = len(nodes)
        kind = key_kind(r.referred_key)
        color = (line_color("social", key_platform(r.referred_key)) if kind == "social"
                 else TYPE_HEX["website"] if kind == "website" else NODE_COLOR)
        nodes.append((r.referred, color))
    links = [{"source": li[r.referrer_key], "target": ri[r.referred_key],
              "value": getattr(r, weight), "type": r.type, "platform": r.platform,
              "what": (platform_name(r.platform) + " link") if r.type == "social"
              else TYPE_NAMES[r.type], "posts": r.posts, "views": r.views}
             for r in agg.itertuples(index=False)]
    height = max(480, 28 * max(len(li), len(ri)) + 80)
    return build_sankey(nodes, links, weight, height)


NODE_SYMBOLS = {"collected": "circle", "telegram": "diamond", "social": "square",
                "website": "triangle-up"}
NODE_TYPE_NAMES = {"collected": "Collected channel", "telegram": "Other Telegram account",
                   "social": "Social media account", "website": "Website"}
COMMUNITY_COLORS = ["#1F77B4", "#FF7F0E", "#2CA02C", "#D62728", "#9467BD", "#8C564B",
                    "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF", "#393B79", "#AD494A",
                    "#637939", "#8C6D31", "#7B4173", "#3182BD"]


def build_graph(summary, collected):
    """Directed, weighted graph of referrals (direct referrals only)."""
    G = nx.DiGraph()
    direct = summary[~summary["via"]]
    agg = (direct.groupby(["referrer_key", "referrer", "referred_key", "referred"])
           .agg(posts=("posts", "sum"), views=("views", "sum"),
                types=("type", lambda s: ", ".join(sorted(set(s))))).reset_index())
    for r in agg.itertuples(index=False):
        for key, label in ((r.referrer_key, r.referrer), (r.referred_key, r.referred)):
            if key not in G:
                kind = key_kind(key)
                node_type = "collected" if key in collected else kind
                G.add_node(key, label=str(label), node_type=node_type,
                           platform=key_platform(key) or "")
        G.add_edge(r.referrer_key, r.referred_key, posts=float(r.posts),
                   views=float(r.views), types=r.types)
    return G


def graph_metrics(G, weight):
    if G.number_of_nodes() == 0:
        return pd.DataFrame()
    w = {(u, v): max(d[weight], 1e-9) for u, v, d in G.edges(data=True)}
    nx.set_edge_attributes(G, w, "w")
    pagerank = nx.pagerank(G, weight="w") if G.number_of_edges() else {}
    k = None if G.number_of_nodes() <= 1500 else 300
    betweenness = nx.betweenness_centrality(G, k=k, seed=7) if G.number_of_edges() else {}
    U = G.to_undirected()
    communities = {}
    if U.number_of_edges():
        for i, members in enumerate(sorted(nx.community.louvain_communities(
                U, weight="w", seed=7), key=len, reverse=True), start=1):
            for m in members:
                communities[m] = i
    rows = []
    for n, d in G.nodes(data=True):
        rows.append({
            "key": n, "account": d["label"], "node_type": NODE_TYPE_NAMES[d["node_type"]],
            "platform": platform_name(d["platform"]) if d["platform"] else "",
            "referred_to_by": G.in_degree(n),
            "posts_referring_to_it": sum(e["posts"] for _, _, e in G.in_edges(n, data=True)),
            "refers_to": G.out_degree(n),
            "posts_referring_out": sum(e["posts"] for _, _, e in G.out_edges(n, data=True)),
            "views_in": sum(e["views"] for _, _, e in G.in_edges(n, data=True)),
            "pagerank": round(pagerank.get(n, 0), 6),
            "betweenness": round(betweenness.get(n, 0), 6),
            "community": communities.get(n, 0),
        })
    return pd.DataFrame(rows).sort_values("pagerank", ascending=False).reset_index(drop=True)


def filter_graph(G, metrics, min_posts, min_referrers, max_nodes):
    H = nx.DiGraph()
    H.add_nodes_from(G.nodes(data=True))
    H.add_edges_from((u, v, d) for u, v, d in G.edges(data=True) if d["posts"] >= min_posts)
    if min_referrers > 1:
        drop = [n for n, d in H.nodes(data=True)
                if d["node_type"] != "collected" and H.in_degree(n) < min_referrers]
        H.remove_nodes_from(drop)
    H.remove_nodes_from([n for n in list(H) if H.degree(n) == 0])
    if H.number_of_nodes() > max_nodes:
        score = metrics.set_index("key")["posts_referring_to_it"] + \
            metrics.set_index("key")["posts_referring_out"]
        keep = set(score[score.index.isin(H.nodes)].sort_values(ascending=False)
                   .index[:max_nodes])
        H.remove_nodes_from([n for n in list(H) if n not in keep])
        H.remove_nodes_from([n for n in list(H) if H.degree(n) == 0])
    return H


def network_figure(H, metrics, weight, size_by, color_by, n_labels, arrows=True):
    m = metrics.set_index("key")
    U = H.to_undirected()
    for u, v, d in U.edges(data=True):
        d["lw"] = np.log1p(d[weight])
    pos = nx.spring_layout(U, weight="lw", seed=7, iterations=150,
                           k=1.8 / max(sqrt(max(U.number_of_nodes(), 1)), 1))
    fig = go.Figure()

    wmax = max((d[weight] for _, _, d in H.edges(data=True)), default=1) or 1
    buckets = {}
    mids = {"x": [], "y": [], "text": [], "angle": []}
    for u, v, d in H.edges(data=True):
        kind = key_kind(v)
        if d["types"] == "forward":
            color = TYPE_HEX["forward"]
        elif kind == "social":
            color = line_color("social", key_platform(v))
        elif kind == "website":
            color = TYPE_HEX["website"]
        else:
            color = TYPE_HEX["mention"]
        width = round(0.6 + 7 * sqrt(d[weight] / wmax), 0)
        b = buckets.setdefault((color, width), {"x": [], "y": []})
        (x0, y0), (x1, y1) = pos[u], pos[v]
        b["x"] += [x0, x1, None]
        b["y"] += [y0, y1, None]
        mids["x"].append(x0 + (x1 - x0) * 0.6)
        mids["y"].append(y0 + (y1 - y0) * 0.6)
        mids["angle"].append(np.degrees(np.arctan2(x1 - x0, y1 - y0)))
        mids["text"].append(f"{H.nodes[u]['label']} → {H.nodes[v]['label']}<br>"
                            f"{int(d['posts']):,} posts ({d['types']}), "
                            f"{d['views']:,.0f} views")
    for (color, width), b in buckets.items():
        fig.add_trace(go.Scatter(x=b["x"], y=b["y"], mode="lines", hoverinfo="skip",
                                 line=dict(color=rgba(color, 0.45), width=width),
                                 showlegend=False))
    fig.add_trace(go.Scatter(
        x=mids["x"], y=mids["y"], mode="markers",
        marker=dict(size=9, symbol="arrow", angle=mids["angle"],
                    color="rgba(70,70,70,0.55)" if arrows else "rgba(0,0,0,0)"),
        hovertext=mids["text"], hoverinfo="text", showlegend=False))

    size_col = {"Times referred to": "posts_referring_to_it",
                "Referrals made": "posts_referring_out",
                "PageRank": "pagerank"}[size_by]
    smax = max(m[size_col].max(), 1e-9)
    ranked = m.loc[[n for n in H.nodes]].sort_values("pagerank", ascending=False)
    labeled = set(ranked.index[:n_labels])
    for node_type in ("collected", "telegram", "social", "website"):
        nodes = [n for n, d in H.nodes(data=True) if d["node_type"] == node_type]
        if not nodes:
            continue
        if color_by == "Community":
            colors = [COMMUNITY_COLORS[(int(m.at[n, "community"]) - 1)
                                       % len(COMMUNITY_COLORS)]
                      if m.at[n, "community"] else "#999999" for n in nodes]
        else:
            colors = []
            for n in nodes:
                d = H.nodes[n]
                if node_type == "collected":
                    colors.append("#1F4FD1")
                elif node_type == "social":
                    colors.append(line_color("social", d["platform"]))
                elif node_type == "website":
                    colors.append(TYPE_HEX["website"])
                else:
                    colors.append(TYPE_HEX["mention"])
        sizes = [9 + 34 * sqrt(max(m.at[n, size_col], 0) / smax) for n in nodes]
        hover = [f"<b>{m.at[n, 'account']}</b><br>{m.at[n, 'node_type']}"
                 + (f" ({m.at[n, 'platform']})" if m.at[n, "platform"] else "")
                 + f"<br>Referred to by {m.at[n, 'referred_to_by']} accounts "
                 f"({int(m.at[n, 'posts_referring_to_it']):,} posts)"
                 f"<br>Refers to {m.at[n, 'refers_to']} accounts "
                 f"({int(m.at[n, 'posts_referring_out']):,} posts)"
                 f"<br>PageRank {m.at[n, 'pagerank']:.4f} · community {m.at[n, 'community']}"
                 f"<br><i>Click to open in the one-channel view</i>"
                 for n in nodes]
        fig.add_trace(go.Scatter(
            x=[pos[n][0] for n in nodes], y=[pos[n][1] for n in nodes],
            mode="markers+text",
            text=[short(m.at[n, "account"], 28) if n in labeled else "" for n in nodes],
            textposition="top center", textfont=dict(size=11, color="#2D3748"),
            marker=dict(size=sizes, color=colors, symbol=NODE_SYMBOLS[node_type],
                        line=dict(width=1, color="#FFFFFF"), opacity=0.92),
            customdata=[[n, m.at[n, "account"]] for n in nodes],
            hovertext=hover, hoverinfo="text", name=NODE_TYPE_NAMES[node_type]))
    fig.update_layout(
        height=760, margin=dict(l=10, r=10, t=30, b=10), plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF", hovermode="closest", dragmode="pan",
        legend=dict(orientation="h", y=1.03, x=0),
        xaxis=dict(visible=False), yaxis=dict(visible=False, scaleanchor="x"))
    return fig


def matrix_figure(summary, metrics, weight, n_cols):
    direct = summary[~summary["via"]]
    mat = direct.pivot_table(index=["referrer_key", "referrer"],
                             columns=["referred_key", "referred"],
                             values=weight, aggfunc="sum", fill_value=0)
    if mat.empty:
        return None
    breadth = (mat > 0).sum(axis=0)
    volume = mat.sum(axis=0)
    cols = (pd.DataFrame({"b": breadth, "v": volume})
            .sort_values(["b", "v"], ascending=False).index[:n_cols])
    mat = mat[cols]
    mat = mat[mat.sum(axis=1) > 0]
    comm = metrics.set_index("key")["community"] if not metrics.empty else pd.Series(dtype=int)
    rows = sorted(mat.index, key=lambda r: (comm.get(r[0], 0), -mat.loc[r].sum()))
    cols = sorted(mat.columns, key=lambda c: (comm.get(c[0], 0), -mat[c].sum()))
    mat = mat.loc[rows, cols]
    z = mat.values
    hover = [[f"{r[1]} → {c[1]}<br>{z[i][j]:,.0f} {weight}" for j, c in enumerate(cols)]
             for i, r in enumerate(rows)]
    fig = go.Figure(go.Heatmap(
        z=np.sqrt(z), x=[short(c[1], 30) for c in cols], y=[short(r[1], 30) for r in rows],
        text=hover, hoverinfo="text", colorscale="Blues", showscale=False, xgap=1, ygap=1))
    fig.update_layout(height=max(360, 24 * len(rows) + 220),
                      margin=dict(l=10, r=10, t=10, b=10),
                      xaxis=dict(tickangle=-50, side="top"),
                      yaxis=dict(autorange="reversed"), plot_bgcolor="#FFFFFF")
    return fig


def graph_file(G, fmt):
    H = nx.DiGraph()
    for n, d in G.nodes(data=True):
        H.add_node(n, **{k: (v if isinstance(v, (int, float, str)) else str(v))
                         for k, v in d.items()})
    for u, v, d in G.edges(data=True):
        H.add_edge(u, v, weight=d["posts"], posts=d["posts"], views=d["views"],
                   types=d["types"])
    buf = io.BytesIO()
    if fmt == "gexf":
        nx.write_gexf(H, buf)
    else:
        nx.write_graphml(H, buf)
    return buf.getvalue()


TELEGRAM_APPS_URL = "https://my.telegram.org/auth?to=apps"
API_SETUP_STEPS = """
You need a Telegram account. If you do not have one, install Telegram on your phone and sign
up first.

1. Click **Open my.telegram.org** above. The page opens in a new tab.
2. Enter the phone number of your Telegram account in international format, with the
   country code, such as **+65 9123 4567**. Click **Next**.
3. Telegram sends a confirmation code **as a message in your Telegram app**, not by SMS.
   Open Telegram on any device, copy the code, enter it on the site and click **Sign In**.
4. Click **API development tools**.
5. Fill in the **Create new application** form:
   - **App title:** any name, such as *OSINT course*
   - **Short name:** 5 to 32 letters and numbers with no spaces, such as *osintcourse2026*
   - **URL** and **Description:** leave empty
   - **Platform:** any option; *Desktop* works
6. Click **Create application**.
7. The page now shows **App api_id** (a number) and **App api_hash** (32 letters and numbers).
   Copy them into **API ID** and **API hash** below.

You only create the application once. To see the ID and hash again later, sign in to
my.telegram.org and open **API development tools**. Keep the hash private: Telegram does not
let you replace it. Your account remains subject to Telegram's
[API Terms of Service](https://core.telegram.org/api/terms).
"""
API_SETUP_TROUBLESHOOTING = """
**The site shows "ERROR" after Create application.** This is the most common problem, and the
site gives no detail. Try these in order:
- Check that the short name is 5 to 32 letters and numbers, with no spaces or symbols.
- Turn off any VPN or proxy.
- Turn off ad blockers and privacy extensions for my.telegram.org, or use a private window.
- Try a different browser, or your phone's browser on mobile data instead of Wi-Fi.
- Wait 10 to 15 minutes and try again. Repeated attempts in a short time can keep failing.
- Recently created Telegram accounts are sometimes refused. Waiting a few days usually helps.

**The code does not arrive.** Look in the Telegram app for a message from *Telegram*
(with a blue check mark), on any device where you are logged in. The site does not send SMS.
Check that the phone number includes the country code.

**The site does not load.** Some networks block telegram.org. Try another network, such as
mobile data.

**"Too many tries."** Telegram has paused sign-ins for this number. Wait and try later; the
pause can last several hours.
"""


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
ss.setdefault("comparison", None)
ss.setdefault("estimate", None)
ss.setdefault("map_resolved", {})
ss.setdefault("map_checked", set())

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
    kind = job.get("kind", "collect")
    if prog.get("lookups"):
        i, n = prog["lookups"]
        st.progress(i / max(n, 1), text=f"Identifying accounts behind links: {i:,} of {n:,}")
    else:
        frac = prog["done"] / max(prog["total"], 1)
        phase = f"{prog['phase']}: " if prog.get("phase") else ""
        st.progress(min(frac, 1.0),
                    text=f"{phase}{prog['done']} of {prog['total']} channels")
    st.code("\n".join(prog["log"][-LOG_LINES_SHOWN:]) or "Starting...")
    if st.button("Cancel"):
        fut.cancel()
    if fut.done():
        if fut.cancelled():
            st.warning("Cancelled.")
        elif fut.exception():
            st.error(f"Failed: {fut.exception()}")
        elif kind == "compare":
            ss.comparison = fut.result()
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
        st.markdown("**Step 1 of 2: Enter your API ID and API hash**")
        st.caption("Telegram issues these to your own account. You get them once and reuse "
                   "them every time. They stay in this browser session's memory and are never "
                   "written to disk.")

        api_id = ss.get("api_id_input", "")
        with st.expander("First time? Get your API ID and API hash (about 5 minutes)",
                         expanded=not api_id):
            st.link_button("Open my.telegram.org", TELEGRAM_APPS_URL, type="primary",
                           icon=":material/open_in_new:")
            st.markdown(API_SETUP_STEPS)
            with st.expander("If the site shows ERROR or the code does not arrive"):
                st.markdown(API_SETUP_TROUBLESHOOTING)

        c1, c2 = st.columns(2)
        api_id = c1.text_input("API ID", key="api_id_input", placeholder="e.g. 1234567",
                               help="The number shown as App api_id on my.telegram.org.")
        api_hash = c2.text_input("API hash", type="password", key="api_hash_input",
                                 placeholder="32 letters and numbers",
                                 help="The code shown as App api_hash on my.telegram.org.")
        api_id, api_hash = api_id.strip(), api_hash.strip()
        creds_ok = True
        if api_id and not api_id.isdigit():
            c1.error("The API ID is a number only, such as 1234567. Copy App api_id, "
                     "not the app title or short name.")
            creds_ok = False
        if api_hash and not re.fullmatch(r"[0-9a-fA-F]{32}", api_hash):
            c2.error(f"The API hash is exactly 32 characters of numbers and the letters a to f "
                     f"(this entry has {len(api_hash)}). Copy App api_hash in full.")
            creds_ok = False
        if not (api_id and api_hash):
            creds_ok = False

        st.markdown("**Step 2 of 2: Log in**")
        if not creds_ok:
            st.caption("Enter a valid API ID and API hash above to continue.")

        tab_phone, tab_session = st.tabs(["Log in with phone", "Use a saved session string"])

        with tab_phone:
            if ss.login_stage == "phone":
                phone = st.text_input(
                    "Phone number of your Telegram account, with country code",
                    placeholder="e.g. +65 9123 4567",
                    help="Telegram sends the login code as a message in your Telegram app.")
                if st.button("Send login code", disabled=not (creds_ok and phone)):
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
            if st.button("Connect with session", disabled=not (creds_ok and sess)):
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
    channels_raw = st.text_area(
        "Target channels (one per line; @handle, handle, or t.me link)", height=160,
        key="channels_raw")

    query_text = st.text_area(
        "Search query", height=90, key="query_text",
        placeholder='(aukus OR аукус) AND (submarine* OR подлодк*) -"virginia class"',
        help="Boolean search. Open the guide below for the full syntax.")
    with st.expander("Query guide"):
        st.markdown(QUERY_HELP)

    parsed, query_error = None, None
    if query_text.strip():
        try:
            parsed = parse_query(query_text)
        except QueryError as e:
            query_error = str(e)
    plan = telegram_search_terms(parsed) if parsed else None

    method = st.radio(
        "Collection method",
        ["search", "scan"],
        format_func=lambda m: {
            "search": "Fast: Telegram search, then apply the query (can miss posts)",
            "scan": "Thorough: read every post in the date range (complete, slower)",
        }[m],
        help="Fast asks Telegram's search for a few key terms and applies your full query to "
             "what comes back. Telegram's search matches loosely, so it can miss posts; "
             "use the comparison tool below to measure how many for your query. Thorough "
             "reads every post, so nothing that matches is missed, but it reads about "
             "3,000 to 5,000 posts a minute.")

    if query_error:
        st.error(query_error)
    elif parsed:
        st.caption(f"Query read as: `{describe_query(parsed)}`")
        if method == "search":
            if plan:
                st.caption("Telegram will be searched for: "
                           + ", ".join(f"`{t}`" for t in plan))
            else:
                st.warning("This query has no plain words for Telegram's search to look for "
                           "(for example, it uses only NOT, field filters or regular "
                           "expressions). Use the thorough method, or add a required word.")

    with st.expander("Test the query on sample text"):
        sample = st.text_area("Paste a post", height=100, key="sample_text")
        if sample.strip() and parsed:
            rec = PostRecord(sample)
            if query_matches(parsed, rec):
                terms = matched_terms(parsed, rec)
                st.success("Matches" + (f": {', '.join(terms)}" if terms else ""))
            else:
                st.info("No match")
        st.caption("The test checks post text only; field filters such as channel: and from: "
                   "are checked during collection.")

    d1, d2, d3, d4 = st.columns(4)
    start_d = d1.date_input("Start date", value=date.today() - timedelta(days=30))
    end_d = d2.date_input("End date (inclusive)", value=date.today())
    text_limit = d3.number_input("Max characters per message (0 = full text)",
                                 0, 20000, 0, step=500)
    min_cascade = d4.number_input("Min text length for cascade matching", 10, 200, 50)
    resolve = st.checkbox(
        "Identify the accounts behind video and short links", value=True,
        help="YouTube and Rumble video links, Telegraph articles and short links "
             "(t.co, bit.ly, vm.tiktok.com and others) do not name an account. With this on, "
             "the app looks each one up after collection: the app's server sends a request "
             "to YouTube, Rumble, Telegraph or the link shortener. Short-link services log "
             "these requests. Up to 3,000 links per run.")
    prefix = st.text_input("File name prefix", value="telegram")

    channels = [clean_channel(c) for c in parse_lines(channels_raw)]
    start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_d + timedelta(days=1), datetime.min.time(),
                              tzinfo=timezone.utc)
    n_terms = len(plan or [])

    # ── size estimate ────────────────────────────────────────────────
    est_params = (tuple(channels), start_d, end_d)
    est = ss.estimate if ss.estimate and ss.estimate["params"] == est_params else None
    e1, e2 = st.columns([1, 3])
    if e1.button("Estimate collection size", disabled=running or not channels,
                 help="Checks each channel's post volume in the date range with two quick "
                      "requests per channel."):
        with st.spinner(f"Checking {plural(len(channels), 'channel')}..."):
            try:
                rows = runner.run(estimate_volume(ss.client, channels, start_dt, end_dt),
                                  timeout=900)
                ss.estimate = {"params": est_params, "rows": rows}
                est = ss.estimate
            except Exception as e:
                st.error(f"Estimate failed: {type(e).__name__}: {e}")
    if est:
        total = sum(r["posts"] or 0 for r in est["rows"])
        lo, hi, fast_m = estimate_minutes(total, len(channels), n_terms)
        busy = sorted((r for r in est["rows"] if r["posts"]), key=lambda r: -r["posts"])[:3]
        busiest = ", ".join(f"{r['channel']} ({r['posts']:,})" for r in busy)
        fast_text = ("under a minute" if fast_m < 1 else f"at least {minutes_text(fast_m)}")
        msg = (f"About **{total:,} posts** across {plural(len(channels), 'channel')} in this "
               f"date range. Thorough: **{range_text(lo, hi)}**. Fast: {fast_text}, "
               "longer when many posts match.")
        if busiest:
            msg += f" Busiest: {busiest}."
        if total > 100_000 and method == "scan":
            e2.error(msg + " A thorough run this large may take hours and hit long Telegram "
                           "rate limits. Consider fewer channels, a shorter date range or the "
                           "fast method.")
        elif total > 20_000 and method == "scan":
            e2.warning(msg)
        else:
            e2.info(msg)
        failed = [r["channel"] for r in est["rows"] if r["posts"] is None]
        if failed:
            st.caption("Could not check: " + ", ".join(failed))
    elif method == "scan" and len(channels) > 3:
        e2.warning(f"Thorough collection reads every post from "
                   f"{plural(len(channels), 'channel')}, which can take a long time. "
                   "Estimate the size before starting.")

    # ── compare methods ──────────────────────────────────────────────
    with st.expander("Compare fast and thorough on a sample"):
        st.caption("Runs both methods on up to three channels over a short period, then "
                   "lists the posts the fast method missed and why. Use it to decide whether "
                   "fast is good enough for this query.")
        c1, c2 = st.columns([3, 1])
        sample_channels = c1.multiselect("Channels", channels, default=channels[:1],
                                         max_selections=3)
        days = c2.number_input("Days, ending on the end date", 1, 60, 7)
        can_compare = bool(sample_channels and parsed and plan and not running)
        if st.button("Run comparison", disabled=not can_compare):
            cfg = {"channels": sample_channels, "query": query_text, "search_terms": plan,
                   "start": end_dt - timedelta(days=int(days)), "end": end_dt,
                   "text_limit": 0, "min_cascade_chars": 50}
            progress = {"log": [], "done": 0, "total": len(sample_channels), "skipped": []}
            future = runner.submit(compare_methods(runner, ss.key, ss.client, cfg, progress))
            ss.job = {"future": future, "progress": progress, "kind": "compare",
                      "prefix": prefix}
            ss.comparison = None
            st.rerun()
        if parsed and not plan:
            st.caption("This query has nothing for Telegram's search to look for, so only the "
                       "thorough method applies.")
        comp = ss.comparison
        if comp:
            t = comp["thorough"]
            share = f"{comp['both'] / t:.0%}" if t else "n/a"
            st.markdown(
                f"**Fast found {comp['both']:,} of the {t:,} posts that thorough found "
                f"({share})** in {', '.join(comp['channels'])} over {comp['days']} days. "
                f"Telegram was searched for: {', '.join(comp['search_terms'])}.")
            if comp["fast_only"]:
                st.caption(f"{plural(comp['fast_only'], 'post')} appeared only in the fast "
                           "results.")
            if not comp["missed"].empty:
                st.dataframe(comp["missed"], width="stretch", hide_index=True)

    if st.button("Start collection", type="primary", disabled=running):
        cfg = {
            "channels": channels,
            "query": query_text,
            "method": method,
            "search_terms": plan or [],
            "start": start_dt,
            "end": end_dt,
            "text_limit": int(text_limit),
            "min_cascade_chars": int(min_cascade),
            "resolve_links": resolve,
        }
        problems = []
        if not cfg["channels"]:
            problems.append("Add at least one channel.")
        if not query_text.strip():
            problems.append("Enter a search query.")
        elif query_error:
            problems.append(f"Fix the query first: {query_error}")
        elif method == "search" and not plan:
            problems.append("Fast collection needs at least one plain word in the query. "
                            "Switch to the thorough method or add a required word.")
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
            st.info("No posts matched. Check the query, channels and date range.")
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
                            "Mentioned channels", "Social media accounts", "Domains",
                            "Run log"])
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
                st.dataframe(counts(posts["social_accounts"], ";", "account"),
                             width="stretch")
                st.caption("Links inside forwarded posts are listed in "
                           "forwarded_content_refs and are not counted here.")
            with tabs[6]:
                st.dataframe(counts(posts["domains"], ",", "domain"), width="stretch")
            with tabs[7]:
                st.code("\n".join(ss.results["log"]))

MAP_VIEWS = ["One channel", "Network", "Sankey", "Matrix"]


def open_node_from_network():
    state = st.session_state.get("net_chart") or {}
    points = (state.get("selection") or {}).get("points") or []
    for point in points:
        data = point.get("customdata")
        if isinstance(data, (list, tuple)) and len(data) == 2:
            st.session_state["focus_choice"] = (data[0], data[1])
            st.session_state["map_view"] = "One channel"
            return


def referral_page():
    st.title("Referral map")
    st.caption(
        "A referral is a post that points its audience somewhere else: a mention of another "
        "Telegram account (@handle or t.me link), a forward of another account's post, or a "
        "link to a social media account or website. A forward counts only as a referral to "
        "its source; references inside forwarded posts are shown separately when you turn "
        "them on. Self-mentions and self-forwards are excluded.")

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

    filter_text = st.text_input(
        "Limit to posts matching a query (optional)", key="map_filter",
        placeholder='e.g. aukus -"virginia class"   or   channel:rybar OR channel:dva_majors',
        help="Uses the same Boolean syntax as the collection query.")
    if filter_text.strip():
        try:
            node = parse_query(filter_text)
        except QueryError as e:
            st.error(str(e))
            return
        get = lambda row, col: row.get(col) if isinstance(row.get(col), str) else ""
        mask = [query_matches(node, PostRecord(get(r, "text"), get(r, "channel"),
                                               get(r, "channel_title"), get(r, "link"),
                                               get(r, "forwarded_from")))
                for r in posts.to_dict("records")]
        st.caption(f"{sum(mask):,} of {len(posts):,} posts match `{describe_query(node)}`.")
        posts = posts[mask]
        if posts.empty:
            st.warning("No posts match that query.")
            return
        with st.expander("Query guide"):
            st.markdown(QUERY_HELP)

    # Links that name no account can be looked up here too, for older or unresolved files.
    known = set(ss.map_resolved) | ss.map_checked
    if "resolved_links" in posts.columns:
        for cell in posts["resolved_links"].dropna():
            known |= set(parse_resolved(cell))
    pending = [u for u in resolvable_urls(posts["text"]) if u not in known]
    if pending:
        l1, l2 = st.columns([3, 1])
        l1.info(f"{plural(len(pending), 'video or short link')} in these posts "
                f"{'does' if len(pending) == 1 else 'do'} not name an account yet, so "
                f"{'it appears' if len(pending) == 1 else 'they appear'} as "
                "\"account not identified\".")
        if l2.button("Identify accounts", help="Looks up each link from the app's server "
                                               "(YouTube, Rumble, Telegraph, link shorteners)."):
            batch = pending[:MAX_LOOKUPS]
            bar = st.progress(0.0, text="Identifying accounts behind links")
            found = resolve_links(batch, lambda i, n: bar.progress(
                i / n, text=f"Identifying accounts behind links: {i:,} of {n:,}"))
            ss.map_resolved.update(found)
            ss.map_checked |= set(batch)
            ss.lookup_note = (f"Identified accounts for {len(found):,} of "
                              f"{plural(len(batch), 'link')}. The rest could not be identified "
                              "and stay grouped as \"account not identified\".")
            st.rerun()
    if ss.get("lookup_note"):
        st.success(ss.pop("lookup_note"))

    all_edges = cached_edges(posts, json.dumps(ss.map_resolved, sort_keys=True))
    if all_edges.empty:
        st.warning("No mentions, forwards or links to other accounts were found in these posts.")
        return

    c1, c2, c3 = st.columns([2, 2, 1])
    types = c1.multiselect("Referral types", TYPES, default=TYPES,
                           format_func=lambda t: TYPE_NAMES[t].capitalize())
    edges = all_edges
    dated = edges["date"].dropna()
    if not dated.empty:
        lo, hi = dated.min().date(), dated.max().date()
        picked = c2.date_input("Date range", value=(lo, hi), min_value=lo, max_value=hi)
        if isinstance(picked, tuple) and len(picked) == 2:
            start = pd.Timestamp(picked[0], tz="UTC")
            end = pd.Timestamp(picked[1], tz="UTC") + pd.Timedelta(days=1)
            edges = edges[edges["date"].isna()
                          | ((edges["date"] >= start) & (edges["date"] < end))]
    weight = c3.radio("Line width and order", ["posts", "views"],
                      format_func=str.capitalize)
    show_via = st.checkbox(
        "Show references inside forwarded posts (dashed lines)", value=False,
        help="When a channel forwards a post, the accounts and links inside that post "
             "belong to the original author. Turn this on to see them as faint dashed "
             "lines from the forwarding channel. They are left out of totals, the network "
             "and the matrix.")

    edges = edges[edges["type"].isin(types)]
    summary = summarize_edges(edges)
    if summary.empty:
        st.warning("No referrals match these filters.")
        return
    direct = summary[~summary["via"]]
    collected = set(posts["channel"].map(handle_key))
    n_sources = direct["referrer_key"].nunique()
    n_targets = direct["referred_key"].nunique()

    if n_sources <= 1:
        suggested = "One channel"
    elif n_sources <= 8 and n_targets <= 30:
        suggested = "Sankey"
    else:
        suggested = "Network"
    if "map_view" not in st.session_state:
        st.session_state["map_view"] = suggested
    view = st.radio("View", MAP_VIEWS, key="map_view", horizontal=True)
    st.caption(f"{plural(n_sources, 'collected channel')} referring to "
               f"{plural(n_targets, 'account')} and website"
               f"{'' if n_targets == 1 else 's'}. Suggested view for data this size: "
               f"**{suggested}**. One channel suits any size; Sankey stays readable up to "
               "about 8 channels and 30 destinations; Network and Matrix handle larger sets.")
    png = {"toImageButtonOptions": {"format": "png", "scale": 2, "filename": "referral_map"}}

    if view == "One channel":
        shown = summary if show_via else direct
        totals = pd.concat([
            shown.groupby(["referrer_key", "referrer"])["posts"].sum()
                 .rename_axis(["key", "label"]),
            shown.groupby(["referred_key", "referred"])["posts"].sum()
                 .rename_axis(["key", "label"]),
        ]).groupby(level=[0, 1]).sum()
        order = pd.DataFrame({"posts": totals})
        order["collected"] = [k in collected for k, _ in order.index]
        order = order.sort_values(["collected", "posts"], ascending=False)
        options = list(order.index)
        if st.session_state.get("focus_choice") not in options:
            st.session_state["focus_choice"] = options[0]
        choice = st.selectbox(
            "Channel, account or website", options, key="focus_choice",
            format_func=lambda kl: f"{kl[1]}  ({totals[kl]:,} referrals"
                                   f"{', collected' if kl[0] in collected else ''})")
        g1, g2, g3 = st.columns(3)
        limits = {
            "telegram": g1.slider("Telegram channels shown per side", 3, 30, 10),
            "social": g2.slider("Social media accounts shown", 0, 30, 8,
                                help="Set to 0 to hide social media accounts."),
            "website": g3.slider("Websites shown", 0, 30, 5,
                                 help="Set to 0 to hide websites."),
        }
        focus, focus_label = choice
        svg, height = ego_svg(summary, edges, focus, focus_label, weight, limits,
                              collected=focus in collected, show_via=show_via)
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
        st.caption("Hover over a card or line for exact counts. Within each section, the "
                   "most frequent partner is at the top. Links that do not name an account "
                   "are grouped per platform below the identified accounts. The SVG file "
                   "scales cleanly in PowerPoint and Word.")
        inc = counterparty_table(summary if show_via else direct, "referred_key",
                                 "referrer_key", "referrer", focus)
        out = counterparty_table(summary if show_via else direct, "referrer_key",
                                 "referred_key", "referred", focus)
        t1, t2 = st.columns(2)
        t1.markdown(f"**Incoming to {focus_label}**")
        t1.dataframe(inc, width="stretch", hide_index=True)
        t2.markdown(f"**Outgoing from {focus_label}**")
        t2.dataframe(out, width="stretch", hide_index=True)

    elif view == "Network":
        G = build_graph(summary, collected)
        metrics = graph_metrics(G, weight)
        big = G.number_of_nodes() > 150
        n1, n2, n3 = st.columns(3)
        min_posts = n1.slider("Minimum posts per link", 1, 20, 1)
        min_refs = n2.slider(
            "Only destinations referred to by at least this many collected channels",
            1, 10, 2 if big else 1,
            help="Destinations shared by several channels often indicate common sourcing "
                 "or coordination.")
        max_nodes = n3.slider("Maximum nodes", 20, 600, 150, step=10)
        n4, n5, n6, n7 = st.columns(4)
        size_by = n4.selectbox("Node size", ["Times referred to", "Referrals made",
                                             "PageRank"])
        color_by = n5.selectbox("Node color", ["Type and platform", "Community"])
        n_labels = n6.slider("Labeled nodes", 0, 80, 20)
        arrows = n7.checkbox("Direction arrows", value=True)
        H = filter_graph(G, metrics, min_posts, min_refs, max_nodes)
        if H.number_of_nodes() == 0:
            st.warning("No links remain with these settings. Lower the minimums.")
        else:
            with st.spinner("Laying out the network..."):
                fig = network_figure(H, metrics, weight, size_by, color_by, n_labels, arrows)
            st.caption(f"Showing {H.number_of_nodes():,} of {G.number_of_nodes():,} nodes and "
                       f"{H.number_of_edges():,} of {G.number_of_edges():,} links. Circles are "
                       "collected channels, diamonds other Telegram accounts, squares social "
                       "media accounts and triangles websites. Scroll to zoom, drag to pan, "
                       "click a node to open it in the one-channel view.")
            st.plotly_chart(fig, width="stretch", config=png, key="net_chart",
                            on_select=open_node_from_network, selection_mode="points")
        st.markdown("**Node metrics**")
        st.caption("PageRank: overall prominence from being referred to by prominent "
                   "accounts. Betweenness: how often an account sits on the shortest path "
                   "between others, marking bridges between clusters. Community: clusters "
                   "found with the Louvain method.")
        st.dataframe(metrics.drop(columns=["key"]), width="stretch", hide_index=True)
        x1, x2, x3 = st.columns(3)
        x1.download_button("Node metrics (.xlsx)",
                           xlsx_bytes(metrics.drop(columns=["key"]), sheet="Nodes",
                                      tall_rows=False),
                           file_name=f"network_nodes_{date.today():%Y%m%d}.xlsx")
        x2.download_button("Network for Gephi (.gexf)", graph_file(G, "gexf"),
                           file_name=f"referral_network_{date.today():%Y%m%d}.gexf")
        x3.download_button("Network as GraphML (.graphml)", graph_file(G, "graphml"),
                           file_name=f"referral_network_{date.today():%Y%m%d}.graphml")

    elif view == "Sankey":
        a1, a2 = st.columns(2)
        n_src = a1.slider("Collected channels shown", 3, 50, 20)
        n_tgt = a2.slider("Destinations shown", 5, 80, 30)
        st.caption("Left: collected channels. Right: Telegram accounts, then social media "
                   "accounts, then websites. References inside forwarded posts are not "
                   "included. Hover and click the camera icon to save an image.")
        st.plotly_chart(overview_sankey(direct, weight, n_src, n_tgt),
                        width="stretch", config=png)

    else:
        if n_sources < 2:
            st.info("The matrix compares collected channels, so it needs at least two.")
        else:
            n_cols = st.slider("Destinations shown", 10, 120, 40)
            metrics = graph_metrics(build_graph(summary, collected), weight)
            fig = matrix_figure(summary, metrics, weight, n_cols)
            st.caption("Rows: collected channels. Columns: the destinations referred to by "
                       "the most collected channels. Darker cells mean more referrals. Rows "
                       "and columns are grouped by network community, so channels that "
                       "share sources form solid blocks.")
            st.plotly_chart(fig, width="stretch", config=png)

    table = (summary.rename(columns={"referrer": "from_channel", "referred": "to_account",
                                     "referred_kind": "destination_type"})
             .assign(type=lambda d: d["type"].map(TYPE_NAMES),
                     platform=lambda d: d["platform"].map(
                         lambda p: platform_name(p) if p else ""),
                     inside_forwarded_post=lambda d: d["via"])
             .drop(columns=["referrer_key", "referred_key", "referred_short", "via"])
             .sort_values(["posts", "views"], ascending=False))
    for col in ("first_seen", "last_seen"):
        table[col] = table[col].dt.strftime("%Y-%m-%d %H:%M").fillna("")
    st.download_button("Download referral table (.xlsx)",
                       xlsx_bytes(table, sheet="Referrals", tall_rows=False),
                       file_name=f"referrals_{date.today():%Y%m%d}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


COLLECT_PAGE = st.Page(collector_page, title="Collect posts", icon="📥", default=True)
REFERRAL_PAGE = st.Page(referral_page, title="Referral map", icon="🔀")
st.navigation([COLLECT_PAGE, REFERRAL_PAGE]).run()

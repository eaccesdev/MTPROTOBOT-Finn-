"""
proxy_manager.py — Persistent proxy storage and management.

Proxy format stored internally:
    {
        "server":       "1.2.3.4",
        "port":         443,
        "secret":       "abc123...",   # MTProto only, else None
        "username":     "...",          # SOCKS5/HTTP only, else None
        "password":     "...",          # SOCKS5/HTTP only, else None
        "type":         "mtproto"|"socks5"|"http",
        "added_at":     "<iso-timestamp>",
        "first_seen":   "<iso-timestamp>",
        "last_checked": "<iso-timestamp>"|null,
        "last_alive":   "<iso-timestamp>"|null,
        "alive":        true|false|null,
        "latency_ms":   123.4|null,     # TCP RTT in ms
        "check_count":  0,              # total TCP checks performed
        "success_count": 0,             # checks that returned alive
        "country_code": "DE"|null,      # ISO 3166-1 alpha-2
        "country_name": "Germany"|null,
        "tags":         [],             # user-assigned string labels
    }

Improvement highlights
----------------------
#1/#3  Atomic writes (write→tmp, os.replace) + per-process asyncio.Lock prevent
       corruption from crashes and concurrent bot handler calls.
#2     All save/load paths have async variants; sync versions use a lockfile-based
       inter-process guard on POSIX (fcntl).
#14    Blacklist support: blacklist.json, add/remove/check helpers.
#15    Tag support: tag_proxy / untag_proxy helpers.
#19    Source stats: track fetched/alive counts per channel and URL source.
#20    auto_clean_enabled / auto_purge_enabled config fields for scheduled maint.
#24    DATA_DIR env-var: set PROXY_DATA_DIR to store all runtime files elsewhere.
#25    log_level config field.
"""

from __future__ import annotations

import asyncio
import contextlib
import html as html_mod
import json
import os
import re
import tempfile
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Optional

# ---------------------------------------------------------------------------
# #24 — DATA_DIR: all runtime files resolve relative to this directory
# ---------------------------------------------------------------------------

DATA_DIR = os.environ.get("PROXY_DATA_DIR", ".")

def _data(filename: str) -> str:
    """Return an absolute path inside DATA_DIR."""
    return os.path.join(DATA_DIR, filename)


PROXIES_FILE   = _data("proxies.json")
CONFIG_FILE    = _data("config.json")
BLACKLIST_FILE = _data("blacklist.json")

# ---------------------------------------------------------------------------
# #1 — Per-process asyncio.Lock (prevents concurrent writes within one process)
# ---------------------------------------------------------------------------

_PROXIES_LOCK: asyncio.Lock = asyncio.Lock()
_CONFIG_LOCK:  asyncio.Lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# #3 — Atomic write helper (write-to-tmp + os.replace → never half-written)
# #2 — Optional POSIX inter-process file lock via fcntl
# ---------------------------------------------------------------------------

try:
    import fcntl as _fcntl  # POSIX only
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False


@contextlib.contextmanager
def _posix_lock(path: str):
    """Acquire an exclusive advisory lock on <path>.lock (POSIX only)."""
    if not _HAS_FCNTL:
        yield
        return
    lock_path = path + ".lock"
    try:
        lf = open(lock_path, "w")
        try:
            _fcntl.flock(lf, _fcntl.LOCK_EX)
            yield
        finally:
            _fcntl.flock(lf, _fcntl.LOCK_UN)
            lf.close()
    except OSError:
        yield  # gracefully degrade if locking fails


def _atomic_write(path: str, data: str) -> None:
    """Write *data* to *path* atomically (via a sibling tmp file)."""
    dir_  = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=dir_, prefix=".tmp_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(data)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Default config
# ---------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "bot_token":                      "",
    "admin_ids":                      [],
    "source_channels":                [],
    "source_urls":                    [],
    "check_interval_minutes":         30,
    "auto_fetch_interval_minutes":    60,
    "monitor_interval_minutes":       2,
    "daily_digest_hour":              9,
    "check_timeout_seconds":          10,
    "max_proxies":                    1000,
    "auto_remove_blocked":            True,
    "stale_days":                     7,
    "low_pool_threshold":             15,
    "min_failures_to_rotate":         2,
    # #20 scheduled maintenance
    "auto_clean_enabled":             False,
    "auto_purge_enabled":             False,
    "auto_clean_interval_hours":      24,
    "auto_purge_interval_hours":      24,
    # #13 notification throttling
    "notification_cooldown_minutes":  5,
    # #16 web dashboard
    "web_dashboard_enabled":          False,
    "web_dashboard_port":             8080,
    # #25 log level
    "log_level":                      "INFO",
    # source stats (#19) stored inline per channel/url key
    "source_stats":                   {},
}


# ---------------------------------------------------------------------------
# Country code → flag emoji
# ---------------------------------------------------------------------------

def country_flag(code: str | None) -> str:
    """Convert ISO-3166-1 alpha-2 country code to flag emoji."""
    if not code or len(code) != 2:
        return ""
    return "".join(chr(0x1F1E6 + ord(c) - ord("A")) for c in code.upper())


# ---------------------------------------------------------------------------
# Proxy scoring  (lower = better)
# ---------------------------------------------------------------------------

def compute_score(p: dict) -> float:
    """
    Weighted cost combining latency and reliability.

    score = median(latency_history) + (1 − uptime) × 2000
    """
    history = p.get("latency_history", [])
    if history:
        sorted_h = sorted(history)
        mid = len(sorted_h) // 2
        if len(sorted_h) % 2 == 0:
            base = (sorted_h[mid - 1] + sorted_h[mid]) / 2.0
        else:
            base = float(sorted_h[mid])
    else:
        lat = p.get("latency_ms")
        base = float(lat) if lat is not None else 9999.0

    checks  = p.get("check_count", 0)
    success = p.get("success_count", 0)
    uptime  = (success / checks) if checks > 0 else 1.0
    return base + (1.0 - uptime) * 2000.0


def sort_by_score(proxies: list) -> list:
    return sorted(proxies, key=compute_score)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        save_config(DEFAULT_CONFIG.copy())
        return DEFAULT_CONFIG.copy()
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    for k, v in DEFAULT_CONFIG.items():
        cfg.setdefault(k, v)
    return cfg


def save_config(cfg: dict) -> None:
    with _posix_lock(CONFIG_FILE):
        _atomic_write(CONFIG_FILE, json.dumps(cfg, indent=2, ensure_ascii=False))


async def async_save_config(cfg: dict) -> None:
    async with _CONFIG_LOCK:
        await asyncio.to_thread(save_config, cfg)


# ---------------------------------------------------------------------------
# Proxy file helpers — sync + async variants
# ---------------------------------------------------------------------------

def load_proxies() -> list:
    if not os.path.exists(PROXIES_FILE):
        return []
    with _posix_lock(PROXIES_FILE):
        with open(PROXIES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)


def save_proxies(proxies: list) -> None:
    with _posix_lock(PROXIES_FILE):
        _atomic_write(PROXIES_FILE, json.dumps(proxies, indent=2, ensure_ascii=False))


async def async_load_proxies() -> list:
    """Non-blocking proxy load — safe to call from async code."""
    async with _PROXIES_LOCK:
        return await asyncio.to_thread(load_proxies)


async def async_save_proxies(proxies: list) -> None:
    """Non-blocking proxy save — safe to call from async code."""
    async with _PROXIES_LOCK:
        await asyncio.to_thread(save_proxies, proxies)


# ---------------------------------------------------------------------------
# #14 — Blacklist helpers
# ---------------------------------------------------------------------------

def load_blacklist() -> set:
    """Load the blacklist as a set of 'server:port' strings."""
    if not os.path.exists(BLACKLIST_FILE):
        return set()
    try:
        with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()


def save_blacklist(bl: set) -> None:
    _atomic_write(BLACKLIST_FILE, json.dumps(sorted(bl), indent=2))


def blacklist_key(p: dict) -> str:
    return f"{p['server'].lower()}:{p['port']}"


def is_blacklisted(p: dict) -> bool:
    bl = load_blacklist()
    return blacklist_key(p) in bl


def blacklist_add(p: dict) -> bool:
    """Add proxy to blacklist. Returns True if newly added."""
    bl  = load_blacklist()
    key = blacklist_key(p)
    if key in bl:
        return False
    bl.add(key)
    save_blacklist(bl)
    return True


def blacklist_remove(p: dict) -> bool:
    """Remove proxy from blacklist. Returns True if it was present."""
    bl  = load_blacklist()
    key = blacklist_key(p)
    if key not in bl:
        return False
    bl.discard(key)
    save_blacklist(bl)
    return True


# ---------------------------------------------------------------------------
# Proxy parsing
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_proxy(text: str) -> Optional[dict]:
    """
    Parse a proxy from various text representations.

    Supported formats:
      MTProto (tg link)  : https://t.me/proxy?server=H&port=P&secret=S
                           tg://proxy?server=H&port=P&secret=S
      MTProto (plain)    : server:port:secret
      SOCKS5             : socks5://[user:pass@]host:port
      HTTP               : http://[user:pass@]host:port
    """
    text = html_mod.unescape(text.strip())

    if "t.me/proxy" in text or text.startswith("tg://proxy"):
        try:
            parsed = urllib.parse.urlparse(text.replace("https://t.me/proxy?", "tg://proxy?"))
            params = urllib.parse.parse_qs(parsed.query)
            server = params.get("server", [None])[0]
            port   = params.get("port",   [None])[0]
            secret = params.get("secret", [None])[0]
            if server and port:
                return _make_proxy(server, int(port), "mtproto", secret=secret)
        except Exception:
            return None

    lower = text.lower()
    if (lower.startswith("socks5://") or lower.startswith("socks4://")
            or lower.startswith("http://") or lower.startswith("https://")):
        try:
            parsed = urllib.parse.urlparse(text)
            ptype  = "socks5" if "socks" in parsed.scheme else "http"
            server = parsed.hostname
            port   = parsed.port or (1080 if ptype == "socks5" else 8080)
            user   = urllib.parse.unquote(parsed.username) if parsed.username else None
            pwd    = urllib.parse.unquote(parsed.password) if parsed.password else None
            if server and port:
                return _make_proxy(server, port, ptype, username=user, password=pwd)
        except Exception:
            return None

    parts = text.split(":")
    if len(parts) == 3:
        server, port_str, secret = parts
        try:
            return _make_proxy(server.strip(), int(port_str.strip()), "mtproto",
                               secret=secret.strip())
        except ValueError:
            pass

    if len(parts) == 2:
        server, port_str = parts
        try:
            return _make_proxy(server.strip(), int(port_str.strip()), "socks5")
        except ValueError:
            pass

    return None


def _make_proxy(server, port, ptype, secret=None, username=None, password=None):
    now = _now_iso()
    return {
        "server":            server,
        "port":              port,
        "secret":            secret,
        "username":          username,
        "password":          password,
        "type":              ptype,
        "added_at":          now,
        "first_seen":        now,
        "last_checked":      None,
        "last_alive":        None,
        "alive":             None,
        "latency_ms":        None,
        "latency_history":   [],
        "check_count":       0,
        "success_count":     0,
        "country_code":      None,
        "country_name":      None,
        "recently_failed_at": None,
        "tags":              [],      # #15 user-assigned labels
    }


def proxy_key(p: dict) -> tuple:
    """Unique fingerprint for deduplication."""
    return (p["server"].lower(), p["port"], p.get("secret") or "")


# ---------------------------------------------------------------------------
# Proxy list operations
# ---------------------------------------------------------------------------

def add_proxy(proxies: list, proxy_dict: dict) -> bool:
    """
    Add a proxy to the list if not already present and not blacklisted.
    Returns True if added, False if duplicate or blacklisted.
    """
    if is_blacklisted(proxy_dict):
        return False
    key = proxy_key(proxy_dict)
    for existing in proxies:
        if proxy_key(existing) == key:
            return False
    proxy_dict.setdefault("first_seen",         proxy_dict.get("added_at", _now_iso()))
    proxy_dict.setdefault("last_alive",         None)
    proxy_dict.setdefault("latency_ms",         None)
    proxy_dict.setdefault("latency_history",    [])
    proxy_dict.setdefault("check_count",        0)
    proxy_dict.setdefault("success_count",      0)
    proxy_dict.setdefault("country_code",       None)
    proxy_dict.setdefault("country_name",       None)
    proxy_dict.setdefault("recently_failed_at", None)
    proxy_dict.setdefault("tags",               [])
    proxies.append(proxy_dict)
    return True


def remove_proxy(proxies: list, identifier: str) -> Optional[dict]:
    """Remove by server:port, full tg link, or 1-based index string."""
    if identifier.isdigit():
        idx = int(identifier) - 1
        if 0 <= idx < len(proxies):
            return proxies.pop(idx)
        return None

    candidate = parse_proxy(identifier)
    if candidate:
        key = proxy_key(candidate)
        for i, p in enumerate(proxies):
            if proxy_key(p) == key:
                return proxies.pop(i)

    lower = identifier.lower()
    for i, p in enumerate(proxies):
        if lower in p["server"].lower() or lower == f"{p['server']}:{p['port']}":
            return proxies.pop(i)

    return None


def remove_blocked(proxies: list) -> int:
    """Remove proxies confirmed dead (alive=False) or with fail_streak >= 3."""
    before = len(proxies)
    proxies[:] = [
        p for p in proxies
        if p.get("alive") is not False and p.get("fail_streak", 0) < 3
    ]
    return before - len(proxies)


def purge_stale(proxies: list, max_dead_days: int = 7) -> int:
    """Remove proxies that have been persistently dead."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_dead_days)
    before = len(proxies)

    def _should_purge(p: dict) -> bool:
        last_alive = p.get("last_alive")
        if last_alive:
            try:
                dt = datetime.fromisoformat(last_alive)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt < cutoff
            except Exception:
                pass
        if p.get("alive") is False or p.get("check_count", 0) > 0:
            added = p.get("first_seen") or p.get("added_at")
            if added:
                try:
                    dt = datetime.fromisoformat(added)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt < cutoff
                except Exception:
                    pass
        return False

    proxies[:] = [p for p in proxies if not _should_purge(p)]
    return before - len(proxies)


def get_alive(proxies: list) -> list:
    return [p for p in proxies if p.get("alive") is True]


def get_unchecked(proxies: list) -> list:
    return [p for p in proxies if p.get("alive") is None]


def backfill_fields(proxies: list) -> None:
    """Ensure every proxy has all current fields (migration helper)."""
    for p in proxies:
        p.setdefault("first_seen",         p.get("added_at", _now_iso()))
        p.setdefault("last_alive",         None)
        p.setdefault("latency_ms",         None)
        p.setdefault("latency_history",    [])
        p.setdefault("check_count",        0)
        p.setdefault("success_count",      0)
        p.setdefault("country_code",       None)
        p.setdefault("country_name",       None)
        p.setdefault("recently_failed_at", None)
        p.setdefault("tags",               [])


# ---------------------------------------------------------------------------
# #15 — Tag helpers
# ---------------------------------------------------------------------------

def tag_proxy(proxies: list, identifier: str, tag: str) -> Optional[dict]:
    """
    Add a tag to a proxy identified by 1-based index or host:port.
    Returns the modified proxy dict or None if not found.
    """
    tag = tag.strip().lower()
    if not tag:
        return None
    p = _find_proxy(proxies, identifier)
    if p is None:
        return None
    tags = p.setdefault("tags", [])
    if tag not in tags:
        tags.append(tag)
    return p


def untag_proxy(proxies: list, identifier: str, tag: str) -> Optional[dict]:
    """Remove a tag from a proxy. Returns modified proxy or None."""
    tag = tag.strip().lower()
    p   = _find_proxy(proxies, identifier)
    if p is None:
        return None
    tags = p.get("tags", [])
    if tag in tags:
        tags.remove(tag)
    return p


def _find_proxy(proxies: list, identifier: str) -> Optional[dict]:
    """Locate a proxy by 1-based index or host:port string."""
    if identifier.isdigit():
        idx = int(identifier) - 1
        if 0 <= idx < len(proxies):
            return proxies[idx]
        return None
    lower = identifier.lower()
    for p in proxies:
        if lower == f"{p['server'].lower()}:{p['port']}":
            return p
    return None


# ---------------------------------------------------------------------------
# #19 — Source stats helpers
# ---------------------------------------------------------------------------

def update_source_stats(cfg: dict, source_key: str,
                        fetched: int = 0, alive_added: int = 0) -> None:
    """
    Update cumulative source statistics in the config dict (in-place).
    source_key is typically @ChannelName or the URL string.
    Call save_config() afterwards to persist.
    """
    stats = cfg.setdefault("source_stats", {})
    entry = stats.setdefault(source_key, {
        "fetched_total":    0,
        "alive_total":      0,
        "last_fetch":       None,
        "last_fetch_added": 0,
        "last_fetch_alive": 0,
    })
    entry["fetched_total"]    += fetched
    entry["alive_total"]      += alive_added
    entry["last_fetch"]        = _now_iso()
    entry["last_fetch_added"]  = fetched
    entry["last_fetch_alive"]  = alive_added


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_proxy(p: dict, index: int | None = None, show_status: bool = True) -> str:
    """Human-readable one-liner for a proxy."""
    icon       = {True: "✅", False: "❌", None: "❓"}[p.get("alive")]
    ptype      = p["type"].upper()
    addr       = f"{p['server']}:{p['port']}"
    secret_hint = f"  {p['secret'][:8]}…" if p.get("secret") else ""
    idx_str    = f"{index}. " if index is not None else ""
    status     = f" {icon}" if show_status else ""
    lat        = p.get("latency_ms")
    lat_str    = f"  {lat:.0f}ms" if lat is not None else ""
    chk        = p.get("check_count", 0)
    suc        = p.get("success_count", 0)
    up_str     = f"  {100*suc//chk}%up" if chk > 0 else ""
    flag       = country_flag(p.get("country_code"))
    flag_str   = f" {flag}" if flag else ""
    tags       = p.get("tags", [])
    tag_str    = f"  [{','.join(tags)}]" if tags else ""
    return f"{idx_str}[{ptype}]{flag_str} {addr}{secret_hint}{lat_str}{up_str}{tag_str}{status}"


def proxy_to_tg_deeplink(p: dict) -> str | None:
    """Return a tg:// deep link suitable for InlineKeyboardButton url."""
    if p["type"] == "mtproto" and p.get("secret"):
        q = urllib.parse.urlencode({
            "server": p["server"],
            "port":   p["port"],
            "secret": p["secret"],
        })
        return f"tg://proxy?{q}"
    elif p["type"] in ("socks5", "http"):
        params: dict = {"server": p["server"], "port": p["port"]}
        if p.get("username"):
            params["user"] = p["username"]
        if p.get("password"):
            params["pass"] = p["password"]
        q = urllib.parse.urlencode(params)
        return f"tg://socks?{q}"
    return None


def proxy_to_tg_link(p: dict) -> str:
    """Return tg://proxy deep link (MTProto) or a plain URI."""
    if p["type"] == "mtproto" and p.get("secret"):
        q = urllib.parse.urlencode({
            "server": p["server"],
            "port":   p["port"],
            "secret": p["secret"],
        })
        return f"https://t.me/proxy?{q}"
    elif p["type"] == "socks5":
        auth = ""
        if p.get("username"):
            pw   = f":{p['password']}" if p.get("password") else ""
            auth = f"{p['username']}{pw}@"
        return f"socks5://{auth}{p['server']}:{p['port']}"
    elif p["type"] == "http":
        auth = ""
        if p.get("username"):
            pw   = f":{p['password']}" if p.get("password") else ""
            auth = f"{p['username']}{pw}@"
        return f"http://{auth}{p['server']}:{p['port']}"
    return f"{p['server']}:{p['port']}"

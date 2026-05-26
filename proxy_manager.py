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
        "country_name": "Germany"|null
    }
"""

import asyncio
import html as html_mod
import json
import os
import re
import urllib.parse
from datetime import datetime, timezone, timedelta

PROXIES_FILE = "proxies.json"
CONFIG_FILE  = "config.json"

DEFAULT_CONFIG = {
    "bot_token": "",
    "admin_ids": [],
    "source_channels": [],
    "source_urls": [],                    # GitHub raw / API proxy sources
    "check_interval_minutes": 30,
    "auto_fetch_interval_minutes": 60,
    "monitor_interval_minutes": 2,
    "daily_digest_hour": 9,               # UTC hour for daily stats broadcast
    "check_timeout_seconds": 10,
    "max_proxies": 1000,
    "auto_remove_blocked": True,
    "stale_days": 7,                      # purge proxies dead this many days
    "low_pool_threshold": 15,             # trigger emergency fetch below this many alive proxies
    "min_failures_to_rotate": 2,          # consecutive monitor failures before actually rotating
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
# Proxy scoring  (lower = better, like a cost)
# ---------------------------------------------------------------------------

def compute_score(p: dict) -> float:
    """
    Weighted cost combining latency and reliability.

    Score formula:
      base  = median of latency_history (or last latency_ms, or 9999 penalty)
      bonus = (1 - uptime) * 2000   (penalises flaky proxies)
      total = base + bonus

    Using the median of recent readings instead of a single sample makes the
    score robust against one-off latency spikes or dips.  A verified, fast,
    reliable proxy scores lowest and sorts first.
    """
    history = p.get("latency_history", [])
    if history:
        # Median of the rolling history window — stable, outlier-resistant
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
    if checks > 0:
        uptime = success / checks
    else:
        uptime = 1.0          # no data → assume perfect (don't penalise new proxies)

    bonus = (1.0 - uptime) * 2000.0
    return base + bonus


def sort_by_score(proxies: list) -> list:
    """Return a new list of proxies sorted best-first by compute_score."""
    return sorted(proxies, key=compute_score)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config():
    if not os.path.exists(CONFIG_FILE):
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    # fill missing keys with defaults
    for k, v in DEFAULT_CONFIG.items():
        cfg.setdefault(k, v)
    return cfg


def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Proxy file helpers — sync + async variants
# ---------------------------------------------------------------------------

def load_proxies() -> list:
    if not os.path.exists(PROXIES_FILE):
        return []
    with open(PROXIES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_proxies(proxies: list):
    with open(PROXIES_FILE, "w", encoding="utf-8") as f:
        json.dump(proxies, f, indent=2, ensure_ascii=False)


async def async_load_proxies() -> list:
    """Non-blocking proxy load — safe to call from async code."""
    return await asyncio.to_thread(load_proxies)


async def async_save_proxies(proxies: list):
    """Non-blocking proxy save — safe to call from async code."""
    await asyncio.to_thread(save_proxies, proxies)


# ---------------------------------------------------------------------------
# Proxy parsing
# ---------------------------------------------------------------------------

def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_proxy(text: str):
    """
    Parse a proxy from various text representations.

    Supported formats:
      MTProto (tg link)  : https://t.me/proxy?server=H&port=P&secret=S
                           tg://proxy?server=H&port=P&secret=S
      MTProto (plain)    : server:port:secret
      SOCKS5             : socks5://[user:pass@]host:port
      HTTP               : http://[user:pass@]host:port

    Returns a proxy dict or None if unparseable.
    """
    text = html_mod.unescape(text.strip())   # handle &amp; from HTML contexts

    # --- tg:// or t.me proxy link ---
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

    # --- socks5:// or http:// URI ---
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

    # --- plain server:port:secret (MTProto) ---
    parts = text.split(":")
    if len(parts) == 3:
        server, port_str, secret = parts
        try:
            return _make_proxy(server.strip(), int(port_str.strip()), "mtproto",
                               secret=secret.strip())
        except ValueError:
            pass

    # --- plain server:port (SOCKS5 / unknown) ---
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
        "latency_history":   [],    # rolling window of last 5 latency readings
        "check_count":       0,
        "success_count":     0,
        "country_code":      None,
        "country_name":      None,
        "recently_failed_at": None, # ISO ts of last rotation cooldown mark
    }


def proxy_key(p):
    """Unique fingerprint for deduplication."""
    return (p["server"].lower(), p["port"], p.get("secret") or "")


# ---------------------------------------------------------------------------
# Proxy list operations
# ---------------------------------------------------------------------------

def add_proxy(proxies: list, proxy_dict: dict) -> bool:
    """
    Add a proxy to the list if not already present.
    Returns True if added, False if duplicate.
    """
    key = proxy_key(proxy_dict)
    for existing in proxies:
        if proxy_key(existing) == key:
            return False
    # Back-fill new fields on import from old pool files
    proxy_dict.setdefault("first_seen",         proxy_dict.get("added_at", _now_iso()))
    proxy_dict.setdefault("last_alive",         None)
    proxy_dict.setdefault("latency_ms",         None)
    proxy_dict.setdefault("latency_history",    [])
    proxy_dict.setdefault("check_count",        0)
    proxy_dict.setdefault("success_count",      0)
    proxy_dict.setdefault("country_code",       None)
    proxy_dict.setdefault("country_name",       None)
    proxy_dict.setdefault("recently_failed_at", None)
    proxies.append(proxy_dict)
    return True


def remove_proxy(proxies: list, identifier: str):
    """
    Remove by server:port, full tg link, or 1-based index string.
    Returns removed proxy dict or None.
    """
    # by index
    if identifier.isdigit():
        idx = int(identifier) - 1
        if 0 <= idx < len(proxies):
            return proxies.pop(idx)
        return None

    # by key — try parsing as a proxy first
    candidate = parse_proxy(identifier)
    if candidate:
        key = proxy_key(candidate)
        for i, p in enumerate(proxies):
            if proxy_key(p) == key:
                return proxies.pop(i)

    # by partial match (server or server:port)
    lower = identifier.lower()
    for i, p in enumerate(proxies):
        if lower in p["server"].lower() or lower == f"{p['server']}:{p['port']}":
            return proxies.pop(i)

    return None


def remove_blocked(proxies: list) -> int:
    """Remove proxies confirmed dead (alive=False) or with fail_streak >= 3.
    Returns count removed."""
    before = len(proxies)
    proxies[:] = [
        p for p in proxies
        if p.get("alive") is not False and p.get("fail_streak", 0) < 3
    ]
    return before - len(proxies)


def purge_stale(proxies: list, max_dead_days: int = 7) -> int:
    """
    Remove proxies that have been persistently dead.

    A proxy is purged if:
      - It was never seen alive AND was added more than max_dead_days ago, OR
      - Its last_alive timestamp is older than max_dead_days.

    Returns count of purged proxies.
    """
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

        # Never alive — purge if old enough
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


def backfill_fields(proxies: list):
    """Ensure every proxy in the list has all current fields (migration helper)."""
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


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_proxy(p: dict, index: int | None = None, show_status: bool = True) -> str:
    """Human-readable one-liner for a proxy."""
    icon  = {True: "✅", False: "❌", None: "❓"}[p.get("alive")]
    ptype = p["type"].upper()
    addr  = f"{p['server']}:{p['port']}"
    secret_hint = f"  {p['secret'][:8]}…" if p.get("secret") else ""
    idx_str = f"{index}. " if index is not None else ""
    status  = f" {icon}" if show_status else ""

    # Latency
    lat = p.get("latency_ms")
    lat_str = f"  {lat:.0f}ms" if lat is not None else ""

    # Uptime
    chk = p.get("check_count", 0)
    suc = p.get("success_count", 0)
    up_str = f"  {100*suc//chk}%up" if chk > 0 else ""

    # Country flag
    flag = country_flag(p.get("country_code"))
    flag_str = f" {flag}" if flag else ""

    return f"{idx_str}[{ptype}]{flag_str} {addr}{secret_hint}{lat_str}{up_str}{status}"


def proxy_to_tg_deeplink(p: dict) -> str | None:
    """Return a tg:// deep link that opens the proxy dialog directly in the Telegram app.
    Suitable for use as an InlineKeyboardButton url.
    Returns None for proxy types that have no supported deep link scheme.
    """
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

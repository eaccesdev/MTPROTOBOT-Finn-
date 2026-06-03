"""
bot.py — Telegram Proxy Manager Bot
====================================
Full pipeline: Scrape → Save → Connect → Monitor → Auto-rotate → Notify

Commands
--------
/start          — Welcome message & quick usage
/help           — Full command reference

Connection & monitoring
/connect [n]    — Connect to proxy n (or auto-pick best available)
/disconnect     — Stop monitoring, clear active proxy
/status         — Show active proxy health & monitoring state

Proxy management
/add <proxy>    — Add a proxy (tg link, socks5://, or server:port:secret)
/remove <id>    — Remove proxy by list number or server:port
/list [page]    — Show all proxies with live/dead status (paginated)
/filter         — Filter proxy list by type, country, tag, status
/top            — Show top 10 proxies ranked by latency + uptime
/info <id>      — Detailed info + latency history for a single proxy
/check          — Re-check all proxies right now
/clean          — Delete all dead proxies
/purge          — Remove stale proxies (dead > N days)
/export         — Download the proxy pool as a JSON file
/import         — Send a .json or .txt file to import proxies
/share <id>     — Generate a shareable proxy card
/blacklist <id> — Permanently ban a proxy from being re-added
/tag <id> <tag> — Label a proxy (e.g. /tag 3 fast)
/untag <id> <tag> — Remove a tag from a proxy
/backup         — Download a ZIP of all runtime state files
/restore        — Upload a ZIP to restore state

Channel sources
/addchannel <@ch>    — Add a Telegram channel to auto-fetch from
/removechannel <@ch> — Remove a source channel
/channels            — List source channels with quality stats

URL sources
/addsource <url>     — Add an external URL source (GitHub raw, APIs)
/removesource <url>  — Remove a URL source
/sources             — List URL sources with quality stats

Fetch
/fetch               — Pull proxies from all channels + URL sources
/crawl [keywords]    — Discover proxy channels via web directories + Telethon search

Settings
/settings            — Interactive inline settings editor
/setinterval <min>   — Set automatic check interval (minutes)
/setfetch <min>      — Set automatic fetch interval (minutes)
/setmonitor <min>    — Set active-proxy monitor interval (minutes)
/setnotify <mode>    — Set notification mode: normal|silent|verbose
/reload              — Hot-reload config.json without restart

System
/daemon              — Show daemon process status and active proxy
/stats               — Pool statistics (latency, uptime, type breakdown)
/logs                — Show recent bot log lines
"""

from __future__ import annotations

import asyncio
import collections
import functools
import io
import json
import logging
import os
import subprocess
import sys
import zipfile
from datetime import datetime, timezone, timedelta

try:
    from telegram import (
        Update,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        InputFile,
    )
    from telegram.ext import (
        Application,
        CommandHandler,
        MessageHandler,
        CallbackQueryHandler,
        filters,
        ContextTypes,
    )
    from telegram.constants import ParseMode
except ImportError:
    sys.exit(
        "python-telegram-bot not found.\n"
        "Install it with:  pip install 'python-telegram-bot[job-queue]' aiohttp\n"
    )

import proxy_manager as pm
import checker
import fetcher
import crawler
import connection as conn
import geoip

# ---------------------------------------------------------------------------
# #25 — In-memory log ring buffer + log-level from config
# ---------------------------------------------------------------------------

_LOG_BUFFER: collections.deque = collections.deque(maxlen=200)

class _BufferingHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            ts  = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
            lvl = record.levelname[0]   # D/I/W/E
            _LOG_BUFFER.append(f"[{ts}][{lvl}] {record.name}: {record.getMessage()}")
        except Exception:
            pass


def _setup_logging(cfg: dict) -> None:
    level_name = cfg.get("log_level", "INFO").upper()
    level      = getattr(logging, level_name, logging.INFO)

    # Root logger — file or stderr
    log_file = cfg.get("log_file", "")
    handlers: list[logging.Handler] = []
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    else:
        handlers.append(logging.StreamHandler())

    # Buffer handler always active
    buf_handler = _BufferingHandler()
    buf_handler.setLevel(logging.DEBUG)
    handlers.append(buf_handler)

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=level,
        handlers=handlers,
        force=True,
    )


logger = logging.getLogger(__name__)

# Proxies per page in /list
_LIST_PAGE_SIZE = 10

# Consecutive monitor-check failures before rotating
_active_fail_count: int = 0

# ---------------------------------------------------------------------------
# #13 — Notification throttling: track last-sent time per admin
# ---------------------------------------------------------------------------

_last_rotation_notify: dict[int, datetime] = {}   # uid -> last alert time


def _can_notify_rotation(uid: int, cooldown_minutes: int) -> bool:
    last = _last_rotation_notify.get(uid)
    if last is None:
        return True
    return (datetime.now(timezone.utc) - last) >= timedelta(minutes=cooldown_minutes)


def _mark_rotation_notified(uid: int) -> None:
    _last_rotation_notify[uid] = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_admin(user_id: int, cfg: dict) -> bool:
    admins = cfg.get("admin_ids", [])
    return not admins or user_id in admins


def require_admin(func):
    @functools.wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        cfg = pm.load_config()
        if not is_admin(update.effective_user.id, cfg):
            await update.message.reply_text("⛔ You are not authorised to use this bot.")
            return
        return await func(update, ctx)
    return wrapper


async def reply(update: Update, text: str, parse_mode=ParseMode.HTML, **kwargs):
    MAX = 4096
    for i in range(0, len(text), MAX):
        await update.message.reply_text(text[i:i + MAX], parse_mode=parse_mode, **kwargs)


async def _broadcast(bot, cfg: dict, text: str, reply_markup=None,
                     throttle_rotation: bool = False) -> None:
    """Send a message to every configured admin, optionally throttled (#13)."""
    cooldown = cfg.get("notification_cooldown_minutes", 5)
    for uid in cfg.get("admin_ids", []):
        if throttle_rotation and not _can_notify_rotation(uid, cooldown):
            continue
        try:
            await bot.send_message(uid, text, parse_mode=ParseMode.HTML,
                                   reply_markup=reply_markup)
            if throttle_rotation:
                _mark_rotation_notified(uid)
        except Exception as exc:
            logger.warning("broadcast to %s failed: %s", uid, exc)


# ---------------------------------------------------------------------------
# Notification card helpers
# ---------------------------------------------------------------------------

_DIV = "─" * 32


def _card(icon: str, title: str, body: str, footer: str = "") -> str:
    parts = [_DIV, f"{icon}  <b>{title}</b>", "", body]
    if footer:
        parts += ["", footer]
    parts.append(_DIV)
    return "\n".join(parts)


def _proxy_summary(p: dict) -> str:
    flag    = pm.country_flag(p.get("country_code")) or ""
    country = p.get("country_name") or ""
    geo     = f"{flag} {country}".strip() or "Unknown"
    lat     = p.get("latency_ms")
    lat_s   = f"⚡ {lat:.0f} ms" if lat is not None else ""
    chk     = p.get("check_count", 0)
    suc     = p.get("success_count", 0)
    up_s    = f"📊 {100*suc//chk}% uptime" if chk > 0 else ""
    stats   = "  ·  ".join(s for s in [lat_s, up_s] if s)
    ptype   = p["type"].upper()
    lines   = [
        f"  🌐 <code>{p['server']} : {p['port']}</code>",
        f"  📡 {ptype}  ·  {geo}",
    ]
    if stats:
        lines.append(f"  {stats}")
    tags = p.get("tags", [])
    if tags:
        lines.append(f"  🏷 {', '.join(tags)}")
    return "\n".join(lines)


def _fmt_active(p: dict, state: dict) -> str:
    alive_ico  = {True: "🟢 Alive", False: "🔴 Dead", None: "⚪ Unchecked"}[p.get("alive")]
    connected  = state.get("connected_at", "—")
    rotations  = state.get("rotations", 0)
    last_rot   = state.get("last_rotated") or "never"
    monitoring = state.get("monitoring", False)
    body = (
        f"{_proxy_summary(p)}\n\n"
        f"  Status     :  {alive_ico}\n"
        f"  Connected  :  {connected}\n"
        f"  Rotations  :  {rotations}  (last: {last_rot})\n"
        f"  Monitor    :  {'🟢 ON' if monitoring else '🔴 OFF'}"
    )
    return _card("🔌", "ACTIVE PROXY", body)


def _daemon_running() -> tuple[bool, int | None]:
    try:
        out = subprocess.check_output(
            ["ps", "-eo", "pid,cmd", "--no-headers"],
            text=True, stderr=subprocess.DEVNULL
        )
        for line in out.splitlines():
            if "auto_proxy_daemon" in line and "grep" not in line:
                pid = int(line.strip().split()[0])
                return True, pid
        return False, None
    except Exception:
        return False, None


# ---------------------------------------------------------------------------
# ASCII sparkline for latency history (#11)
# ---------------------------------------------------------------------------

def _sparkline(values: list[float]) -> str:
    """Render a mini sparkline from a list of latency values."""
    if not values:
        return ""
    bars = "▁▂▃▄▅▆▇█"
    lo, hi = min(values), max(values)
    rng = hi - lo or 1.0
    return "".join(bars[int((v - lo) / rng * 7)] for v in values)


# ---------------------------------------------------------------------------
# /start  /help
# ---------------------------------------------------------------------------

@require_admin
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "🛡 <b>Telegram Proxy Manager Bot</b>\n\n"
        "Full pipeline: Scrape → Save → Connect → Monitor → Auto-rotate\n\n"
        "<b>Quick start:</b>\n"
        "  1. /addchannel @ProxyMTProto  — add a proxy source\n"
        "  2. /fetch                     — scrape proxies\n"
        "  3. /check                     — test them all\n"
        "  4. /top                       — see fastest proxies\n"
        "  5. /connect                   — auto-connect to best\n\n"
        "Type /help for the full command list."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


@require_admin
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "📋 <b>Command Reference</b>\n\n"
        "<b>Connection</b>\n"
        "  /connect [n]   — connect to proxy n, or auto-pick best\n"
        "  /disconnect    — stop monitoring, clear active proxy\n"
        "  /status        — active proxy health &amp; state\n\n"
        "<b>Proxy pool</b>\n"
        "  /add &lt;proxy&gt;         — add a proxy\n"
        "  /remove &lt;id&gt;        — remove by number or host:port\n"
        "  /list [page]          — paginated list (10 per page)\n"
        "  /filter               — filter by type/country/tag/status\n"
        "  /top                  — top 10 fastest &amp; most reliable\n"
        "  /info &lt;id&gt;          — detailed proxy info + history\n"
        "  /check                — test all proxies now\n"
        "  /clean                — remove all dead proxies\n"
        "  /purge                — remove stale proxies (dead &gt; 7 days)\n"
        "  /export               — download pool as JSON\n"
        "  /import               — upload a .json or .txt file\n"
        "  /share &lt;id&gt;         — generate a shareable proxy card\n"
        "  /blacklist &lt;id&gt;     — permanently ban a proxy from re-import\n"
        "  /tag &lt;id&gt; &lt;label&gt;   — label a proxy\n"
        "  /untag &lt;id&gt; &lt;label&gt; — remove a label\n"
        "  /backup               — download a ZIP of all state files\n"
        "  /restore              — upload a ZIP to restore state\n\n"
        "<b>Sources</b>\n"
        "  /addchannel &lt;@ch&gt;    — add auto-fetch channel\n"
        "  /removechannel &lt;@ch&gt; — remove channel\n"
        "  /channels              — list channels with quality stats\n"
        "  /addsource &lt;url&gt;    — add external URL source\n"
        "  /removesource &lt;url&gt; — remove URL source\n"
        "  /sources               — list URL sources with quality stats\n"
        "  /fetch                 — fetch from all sources now\n"
        "  /crawl [keywords]      — discover proxy channels via web + Telethon search\n\n"
        "<b>Settings</b>\n"
        "  /settings           — interactive settings editor\n"
        "  /setinterval &lt;min&gt; — full-check interval\n"
        "  /setfetch &lt;min&gt;    — fetch interval\n"
        "  /setmonitor &lt;min&gt;  — monitor interval\n"
        "  /setnotify &lt;mode&gt;  — normal | silent | verbose\n"
        "  /reload             — hot-reload config without restart\n\n"
        "<b>System</b>\n"
        "  /daemon  — daemon process status\n"
        "  /stats   — pool statistics\n"
        "  /logs    — recent log lines"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# /connect
# ---------------------------------------------------------------------------

@require_admin
async def cmd_connect(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg     = pm.load_config()
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)

    if not proxies:
        await update.message.reply_text(
            "📭 No proxies in the list.\n"
            "Use /fetch to pull from channels, or /add to add one manually."
        )
        return

    target = None
    if ctx.args and ctx.args[0].isdigit():
        idx = int(ctx.args[0]) - 1
        if 0 <= idx < len(proxies):
            target = proxies[idx]
        else:
            await update.message.reply_text(f"❌ No proxy #{ctx.args[0]}.")
            return
    else:
        target = conn.pick_best(proxies)
        if not target:
            await update.message.reply_text(
                "❌ No live or unchecked proxies available.\n"
                "Run /check to test your list, or /fetch to pull new ones."
            )
            return

    timeout = cfg.get("check_timeout_seconds", 8)
    msg = await update.message.reply_text(
        f"🔍 Verifying {target['server']}:{target['port']} before connecting…"
    )
    alive = await checker.check_one(target, timeout=timeout)
    await pm.async_save_proxies(proxies)

    if not alive:
        tried = {pm.proxy_key(target)}
        found = False
        candidates = [p for p in pm.sort_by_score(proxies)
                      if p.get("alive") is not False and pm.proxy_key(p) not in tried]
        for candidate in candidates[:5]:
            await msg.edit_text(f"❌ Dead. Trying {candidate['server']}:{candidate['port']}…")
            alive = await checker.check_one(candidate, timeout=timeout)
            await pm.async_save_proxies(proxies)
            tried.add(pm.proxy_key(candidate))
            if alive:
                target = candidate
                found  = True
                break
        if not found:
            await msg.edit_text(
                "❌ All tried proxies are unreachable.\n"
                "Run /fetch then /check to refresh the pool."
            )
            return

    await msg.delete()

    global _active_fail_count
    _active_fail_count = 0

    conn.set_active(target)
    interval = cfg.get("monitor_interval_minutes", 5)
    body = (
        f"{_proxy_summary(target)}\n\n"
        f"  🔁 Auto-monitor every {interval} min"
    )
    text     = _card("✅", "PROXY CONNECTED", body)
    deeplink = pm.proxy_to_tg_deeplink(target)
    keyboard = None
    if deeplink:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Apply Proxy in Telegram", url=deeplink)
        ]])
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


# ---------------------------------------------------------------------------
# /disconnect
# ---------------------------------------------------------------------------

@require_admin
async def cmd_disconnect(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = conn.get_active()
    if not active:
        await update.message.reply_text("ℹ️ No active proxy. Nothing to disconnect.")
        return
    conn.clear_active()
    await update.message.reply_text(
        f"⏹ Disconnected from <code>{active['server']}:{active['port']}</code>.\n"
        "Auto-monitoring stopped.",
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------

@require_admin
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state   = conn.get_state()
    active  = state.get("active_proxy")
    proxies = await pm.async_load_proxies()

    alive_count = sum(1 for p in proxies if p.get("alive") is True)
    dead_count  = sum(1 for p in proxies if p.get("alive") is False)
    unk_count   = sum(1 for p in proxies if p.get("alive") is None)

    pool_body = (
        f"  Total   :  {len(proxies)}\n"
        f"  ✅ Alive  :  {alive_count}\n"
        f"  ❌ Dead   :  {dead_count}\n"
        f"  ❓ Unknown:  {unk_count}"
    )

    if not active:
        text = _card("⏹", "NO ACTIVE PROXY", pool_body, "Use /connect to start.")
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
        return

    text = _fmt_active(active, state) + "\n\n" + _card("📦", "POOL", pool_body)
    deeplink = pm.proxy_to_tg_deeplink(active)
    keyboard = None
    if deeplink:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Apply Proxy in Telegram", url=deeplink)
        ]])
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


# ---------------------------------------------------------------------------
# /top
# ---------------------------------------------------------------------------

@require_admin
async def cmd_top(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)

    alive = [p for p in proxies if p.get("alive") is True]
    if not alive:
        await update.message.reply_text("📭 No alive proxies to rank. Run /check first.")
        return

    ranked = pm.sort_by_score(alive)[:10]
    active = conn.get_active()

    lines = ["🏆 <b>Top 10 Proxies</b> (latency + uptime score)\n"]
    for i, p in enumerate(ranked, 1):
        flag  = pm.country_flag(p.get("country_code"))
        lat   = p.get("latency_ms")
        lat_s = f"{lat:.0f}ms" if lat is not None else "?"
        chk   = p.get("check_count", 0)
        suc   = p.get("success_count", 0)
        up_s  = f"{100*suc//chk}%" if chk > 0 else "?"
        mark  = " 🔌" if (active and pm.proxy_key(p) == pm.proxy_key(active)) else ""
        lines.append(
            f"{i}. {flag} <code>{p['server']}:{p['port']}</code>"
            f"  <b>{lat_s}</b>  {up_s} uptime{mark}"
        )

    lines.append("\nTap a button below to apply a proxy in Telegram:")
    rows = []
    for i, p in enumerate(ranked, 1):
        deeplink = pm.proxy_to_tg_deeplink(p)
        if deeplink:
            rows.append([InlineKeyboardButton(
                f"🔗 #{i} — {p['server']}:{p['port']}", url=deeplink
            )])

    keyboard = InlineKeyboardMarkup(rows) if rows else None
    await reply(update, "\n".join(lines), reply_markup=keyboard)


# ---------------------------------------------------------------------------
# #11 — /info <id>  — detailed proxy info + latency sparkline
# ---------------------------------------------------------------------------

@require_admin
async def cmd_info(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: /info &lt;number&gt;  or  /info &lt;host:port&gt;",
            parse_mode=ParseMode.HTML,
        )
        return

    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)

    identifier = ctx.args[0]
    p = pm._find_proxy(proxies, identifier)
    if p is None:
        await update.message.reply_text("❌ Proxy not found.")
        return

    alive_ico  = {True: "✅ Alive", False: "❌ Dead", None: "❓ Unchecked"}[p.get("alive")]
    lat        = p.get("latency_ms")
    lat_str    = f"{lat:.0f} ms" if lat is not None else "—"
    chk        = p.get("check_count", 0)
    suc        = p.get("success_count", 0)
    up_str     = f"{100*suc//chk}%" if chk > 0 else "—"
    flag       = pm.country_flag(p.get("country_code"))
    country    = p.get("country_name") or "Unknown"
    geo        = f"{flag} {country}" if flag else country
    link       = pm.proxy_to_tg_link(p)
    history    = p.get("latency_history", [])
    spark      = _sparkline(history) if history else "(no history)"
    tags       = ", ".join(p.get("tags", [])) or "none"
    added      = p.get("first_seen") or p.get("added_at") or "—"
    last_chk   = p.get("last_checked") or "never"
    last_alive = p.get("last_alive") or "never"
    score      = pm.compute_score(p)

    body = (
        f"  Type       :  {p['type'].upper()}\n"
        f"  Address    :  <code>{p['server']}:{p['port']}</code>\n"
        f"  Status     :  {alive_ico}\n"
        f"  Location   :  {geo}\n"
        f"  Latency    :  {lat_str}\n"
        f"  Uptime     :  {up_str}  ({suc}/{chk} checks)\n"
        f"  Score      :  {score:.0f} (lower = better)\n"
        f"  Tags       :  {tags}\n"
        f"  Added      :  {added[:19]}\n"
        f"  Last check :  {last_chk[:19] if last_chk != 'never' else 'never'}\n"
        f"  Last alive :  {last_alive[:19] if last_alive != 'never' else 'never'}\n\n"
        f"  Latency history (last {len(history)}):\n"
        f"  <code>{spark}</code>\n"
        f"  {' → '.join(f'{v:.0f}' for v in history[-5:]) or '—'} ms\n\n"
        f"  <code>{link}</code>"
    )
    text     = _card("🔍", "PROXY DETAIL", body)
    deeplink = pm.proxy_to_tg_deeplink(p)
    keyboard = None
    if deeplink:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Apply Proxy in Telegram", url=deeplink)
        ]])
    await reply(update, text, reply_markup=keyboard)


# ---------------------------------------------------------------------------
# #10 — /filter  — filter proxy list by type, country, tag, status
# ---------------------------------------------------------------------------

@require_admin
async def cmd_filter(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /filter [type=mtproto|socks5|http] [country=DE] [tag=fast] [status=alive|dead|unknown]

    All filters are optional and combinable.
    """
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)

    # Parse key=value args
    filters_applied: dict[str, str] = {}
    for arg in (ctx.args or []):
        if "=" in arg:
            k, _, v = arg.partition("=")
            filters_applied[k.lower()] = v.lower()

    if not filters_applied:
        await update.message.reply_text(
            "Usage: /filter [type=mtproto|socks5|http] [country=DE] [tag=fast] [status=alive|dead|unknown]\n\n"
            "Examples:\n"
            "  /filter type=mtproto\n"
            "  /filter country=DE status=alive\n"
            "  /filter tag=fast"
        )
        return

    results = proxies
    if "type" in filters_applied:
        t = filters_applied["type"]
        results = [p for p in results if p.get("type", "").lower() == t]
    if "country" in filters_applied:
        cc = filters_applied["country"].upper()
        results = [p for p in results if (p.get("country_code") or "").upper() == cc]
    if "tag" in filters_applied:
        tg = filters_applied["tag"]
        results = [p for p in results if tg in [x.lower() for x in p.get("tags", [])]]
    if "status" in filters_applied:
        s = filters_applied["status"]
        if s == "alive":
            results = [p for p in results if p.get("alive") is True]
        elif s == "dead":
            results = [p for p in results if p.get("alive") is False]
        elif s in ("unknown", "unchecked"):
            results = [p for p in results if p.get("alive") is None]

    if not results:
        await update.message.reply_text("🔍 No proxies match the given filters.")
        return

    active = conn.get_active()
    idx_map = {pm.proxy_key(p): i for i, p in enumerate(proxies, 1)}

    lines = [f"🔍 <b>Filter Results</b> — {len(results)} match\n"]
    rows  = []
    for p in results[:20]:  # cap at 20 for readability
        idx    = idx_map.get(pm.proxy_key(p), "?")
        marker = " 🔌" if (active and pm.proxy_key(p) == pm.proxy_key(active)) else ""
        lines.append(pm.format_proxy(p, index=idx) + marker)
        deeplink = pm.proxy_to_tg_deeplink(p)
        if deeplink:
            rows.append([InlineKeyboardButton(
                f"🔗 #{idx} — {p['server']}:{p['port']}", url=deeplink
            )])

    if len(results) > 20:
        lines.append(f"\n… and {len(results)-20} more")

    keyboard = InlineKeyboardMarkup(rows) if rows else None
    await reply(update, "\n".join(lines), reply_markup=keyboard)


# ---------------------------------------------------------------------------
# /add
# ---------------------------------------------------------------------------

@require_admin
async def cmd_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: /add &lt;proxy&gt;\n\n"
            "Examples:\n"
            "  /add https://t.me/proxy?server=1.2.3.4&amp;port=443&amp;secret=abc\n"
            "  /add socks5://user:pass@1.2.3.4:1080\n"
            "  /add 1.2.3.4:443:abc123secret",
            parse_mode=ParseMode.HTML,
        )
        return

    raw   = " ".join(ctx.args)
    proxy = pm.parse_proxy(raw)
    if not proxy:
        await update.message.reply_text("❌ Could not parse the proxy. Check the format.")
        return

    if pm.is_blacklisted(proxy):
        await update.message.reply_text("🚫 That proxy is blacklisted and cannot be added.")
        return

    cfg     = pm.load_config()
    proxies = await pm.async_load_proxies()

    if len(proxies) >= cfg.get("max_proxies", 1000):
        await update.message.reply_text(f"⚠️ Maximum proxy limit ({cfg['max_proxies']}) reached.")
        return

    if not pm.add_proxy(proxies, proxy):
        await update.message.reply_text("ℹ️ That proxy is already in the list.")
        return

    await pm.async_save_proxies(proxies)
    link = pm.proxy_to_tg_link(proxy)
    await update.message.reply_text(
        f"✅ Proxy added (#{len(proxies)}):\n<code>{link}</code>",
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# /remove
# ---------------------------------------------------------------------------

@require_admin
async def cmd_remove(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: /remove &lt;number&gt; or /remove &lt;host:port&gt;",
            parse_mode=ParseMode.HTML,
        )
        return

    identifier = " ".join(ctx.args)
    proxies    = await pm.async_load_proxies()
    removed    = pm.remove_proxy(proxies, identifier)
    if removed is None:
        await update.message.reply_text("❌ Proxy not found.")
        return

    await pm.async_save_proxies(proxies)

    active = conn.get_active()
    if active and pm.proxy_key(active) == pm.proxy_key(removed):
        conn.clear_active()
        await update.message.reply_text(
            f"🗑 Removed active proxy <code>{pm.proxy_to_tg_link(removed)}</code>.\n"
            "⚠️ Active proxy cleared — use /connect to reconnect.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"🗑 Removed: <code>{pm.proxy_to_tg_link(removed)}</code>",
            parse_mode=ParseMode.HTML,
        )


# ---------------------------------------------------------------------------
# #14 — /blacklist  /unblacklist
# ---------------------------------------------------------------------------

@require_admin
async def cmd_blacklist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Permanently ban a proxy from being re-added."""
    if not ctx.args:
        await update.message.reply_text(
            "Usage: /blacklist &lt;number&gt; or /blacklist &lt;host:port&gt;\n\n"
            "The proxy will also be removed from the pool if present.\n"
            "Use /unblacklist &lt;host:port&gt; to undo.",
            parse_mode=ParseMode.HTML,
        )
        return

    identifier = " ".join(ctx.args)
    proxies    = await pm.async_load_proxies()
    target     = pm._find_proxy(proxies, identifier)

    if target is None:
        # Try parsing as a proxy string
        target = pm.parse_proxy(identifier)

    if target is None:
        await update.message.reply_text("❌ Could not identify the proxy.")
        return

    added = pm.blacklist_add(target)
    # Also remove from pool if present
    pm.remove_proxy(proxies, f"{target['server']}:{target['port']}")
    await pm.async_save_proxies(proxies)

    if added:
        await update.message.reply_text(
            f"🚫 <code>{target['server']}:{target['port']}</code> added to blacklist.\n"
            "It will not be re-added on future /fetch or /import.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text("ℹ️ That proxy was already blacklisted.")


@require_admin
async def cmd_unblacklist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /unblacklist &lt;host:port&gt;", parse_mode=ParseMode.HTML)
        return
    raw   = " ".join(ctx.args)
    proxy = pm.parse_proxy(raw) or {"server": raw.split(":")[0], "port": int(raw.split(":")[1]) if ":" in raw else 0}
    removed = pm.blacklist_remove(proxy)
    if removed:
        await update.message.reply_text(f"✅ <code>{proxy['server']}:{proxy['port']}</code> removed from blacklist.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("ℹ️ That proxy was not in the blacklist.")


# ---------------------------------------------------------------------------
# #15 — /tag  /untag
# ---------------------------------------------------------------------------

@require_admin
async def cmd_tag(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Label a proxy: /tag <id> <label>"""
    if not ctx.args or len(ctx.args) < 2:
        await update.message.reply_text(
            "Usage: /tag &lt;number|host:port&gt; &lt;label&gt;\n\n"
            "Example: /tag 3 fast-gaming",
            parse_mode=ParseMode.HTML,
        )
        return

    identifier = ctx.args[0]
    tag        = ctx.args[1].lower()
    proxies    = await pm.async_load_proxies()
    p          = pm.tag_proxy(proxies, identifier, tag)

    if p is None:
        await update.message.reply_text("❌ Proxy not found.")
        return

    await pm.async_save_proxies(proxies)
    await update.message.reply_text(
        f"🏷 Tagged <code>{p['server']}:{p['port']}</code> as <b>{tag}</b>.\n"
        f"Current tags: {', '.join(p.get('tags', [])) or 'none'}",
        parse_mode=ParseMode.HTML,
    )


@require_admin
async def cmd_untag(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args or len(ctx.args) < 2:
        await update.message.reply_text("Usage: /untag &lt;id&gt; &lt;label&gt;", parse_mode=ParseMode.HTML)
        return
    identifier = ctx.args[0]
    tag        = ctx.args[1].lower()
    proxies    = await pm.async_load_proxies()
    p          = pm.untag_proxy(proxies, identifier, tag)
    if p is None:
        await update.message.reply_text("❌ Proxy not found.")
        return
    await pm.async_save_proxies(proxies)
    await update.message.reply_text(
        f"🏷 Removed tag <b>{tag}</b> from <code>{p['server']}:{p['port']}</code>.",
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# #17 — /share <id>  — generate a shareable proxy card
# ---------------------------------------------------------------------------

@require_admin
async def cmd_share(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /share &lt;number|host:port&gt;", parse_mode=ParseMode.HTML)
        return

    proxies = await pm.async_load_proxies()
    p       = pm._find_proxy(proxies, ctx.args[0])
    if p is None:
        await update.message.reply_text("❌ Proxy not found.")
        return

    link    = pm.proxy_to_tg_link(p)
    flag    = pm.country_flag(p.get("country_code")) or "🌐"
    country = p.get("country_name") or "Unknown"
    lat     = p.get("latency_ms")
    lat_str = f"{lat:.0f} ms" if lat is not None else "—"
    chk     = p.get("check_count", 0)
    suc     = p.get("success_count", 0)
    up_str  = f"{100*suc//chk}%" if chk > 0 else "—"

    card = (
        f"📡 <b>Proxy Share Card</b>\n\n"
        f"{flag} <b>{p['type'].upper()}</b> · {country}\n"
        f"🔗 <code>{p['server']}:{p['port']}</code>\n"
        f"⚡ Latency : {lat_str}\n"
        f"📊 Uptime  : {up_str}\n\n"
        f"<code>{link}</code>\n\n"
        f"<i>Tap the link above to apply this proxy in Telegram.</i>"
    )
    deeplink = pm.proxy_to_tg_deeplink(p)
    keyboard = None
    if deeplink:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Apply Proxy in Telegram", url=deeplink)
        ]])
    await update.message.reply_text(card, parse_mode=ParseMode.HTML, reply_markup=keyboard)


# ---------------------------------------------------------------------------
# /list  (paginated with inline buttons)
# ---------------------------------------------------------------------------

def _build_list_page(proxies: list, page: int, active) -> tuple[str, InlineKeyboardMarkup]:
    total_pages = max(1, (len(proxies) + _LIST_PAGE_SIZE - 1) // _LIST_PAGE_SIZE)
    page        = max(0, min(page, total_pages - 1))

    start = page * _LIST_PAGE_SIZE
    chunk = proxies[start:start + _LIST_PAGE_SIZE]

    alive_count = sum(1 for p in proxies if p.get("alive") is True)
    dead_count  = sum(1 for p in proxies if p.get("alive") is False)
    unk_count   = sum(1 for p in proxies if p.get("alive") is None)

    lines = [
        f"📋 <b>Proxy List</b> — {len(proxies)} total "
        f"(✅{alive_count} ❌{dead_count} ❓{unk_count})\n"
    ]
    for i, p in enumerate(chunk, start + 1):
        marker = " 🔌" if (active and pm.proxy_key(p) == pm.proxy_key(active)) else ""
        lines.append(pm.format_proxy(p, index=i) + marker)

    text = "\n".join(lines)

    rows = []
    for j, p in enumerate(chunk):
        link = pm.proxy_to_tg_deeplink(p)
        if link:
            label = f"🔗 #{start+j+1} — {p['server']}:{p['port']}"
            rows.append([InlineKeyboardButton(label, url=link)])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"list_page:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="list_page:noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶ Next", callback_data=f"list_page:{page+1}"))
    rows.append(nav)

    return text, InlineKeyboardMarkup(rows)


@require_admin
async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)

    if not proxies:
        await update.message.reply_text("📭 No proxies in the list. Use /add to add one.")
        return

    page = 0
    if ctx.args and ctx.args[0].isdigit():
        page = max(0, int(ctx.args[0]) - 1)

    active         = conn.get_active()
    text, keyboard = _build_list_page(proxies, page, active)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def cb_list_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    if data == "list_page:noop":
        return

    cfg = pm.load_config()
    if not is_admin(query.from_user.id, cfg):
        return

    page    = int(data.split(":")[1])
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)
    active  = conn.get_active()

    text, keyboard = _build_list_page(proxies, page, active)
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# /check
# ---------------------------------------------------------------------------

@require_admin
async def cmd_check(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)

    if not proxies:
        await update.message.reply_text("📭 Nothing to check. Add some proxies first.")
        return

    cfg     = pm.load_config()
    timeout = cfg.get("check_timeout_seconds", 8)
    msg     = await update.message.reply_text(
        f"🔍 Checking {len(proxies)} proxies…  (0/{len(proxies)})"
    )
    last_edit = {"n": 0}

    async def progress(done, total):
        if done - last_edit["n"] >= max(1, total // 10) or done == total:
            last_edit["n"] = done
            try:
                await msg.edit_text(f"🔍 Checking {total} proxies…  ({done}/{total})")
            except Exception:
                pass

    summary = await checker.check_all(proxies, timeout=timeout, progress_callback=progress)
    await pm.async_save_proxies(proxies)

    removed = 0
    if cfg.get("auto_remove_blocked", True):
        removed = pm.remove_blocked(proxies)
        if removed:
            await pm.async_save_proxies(proxies)

    extras = []
    if removed:
        extras.append(f"  🗑 Removed  :  {removed} dead")

    body = (
        f"  Checked   :  {summary['total']}\n"
        f"  ✅ Alive   :  {summary['alive']}\n"
        f"  ❌ Dead    :  {summary['dead']}\n"
        f"  📦 Pool    :  {len(proxies)} remaining"
        + ("\n\n" + "\n".join(extras) if extras else "")
    )

    active = conn.get_active()
    footer = ""
    if active and active.get("alive") is False:
        footer = "⚠️ Active proxy is dead — auto-rotate will kick in shortly."

    text = _card("🔍", "CHECK COMPLETE", body, footer)
    asyncio.create_task(_enrich_and_save(proxies))
    await msg.edit_text(text, parse_mode=ParseMode.HTML)


async def _enrich_and_save(proxies: list) -> None:
    try:
        enriched = await geoip.enrich_proxies(proxies, max_lookups=50)
        if enriched:
            await pm.async_save_proxies(proxies)
            logger.info("Geo-enriched %d proxies", enriched)
    except Exception as exc:
        logger.debug("geo enrichment error: %s", exc)


async def _background_emergency_fetch(bot, cfg: dict, proxies: list) -> None:
    channels = cfg.get("source_channels", [])
    urls     = cfg.get("source_urls", [])
    if not channels and not urls:
        return

    try:
        found  = await fetcher.fetch_all(channels, urls)
        max_p  = cfg.get("max_proxies", 1000)
        added  = 0
        for p in found:
            if len(proxies) >= max_p:
                break
            if pm.add_proxy(proxies, p):
                added += 1

        if added:
            await pm.async_save_proxies(proxies)
            timeout  = cfg.get("check_timeout_seconds", 8)
            new_ones = [p for p in proxies if p.get("alive") is None]
            new_alive = 0
            if new_ones:
                summary   = await checker.check_all(new_ones, timeout=timeout)
                await pm.async_save_proxies(proxies)
                new_alive = summary["alive"]

            threshold = cfg.get("low_pool_threshold", 15)
            body = (
                f"  Fetched    :  {len(found)} found, {added} new\n"
                f"  ✅ Checked  :  {new_alive} alive\n"
                f"  Trigger    :  alive pool was below {threshold}"
            )
            await _broadcast(bot, cfg, _card("⚠️", "LOW POOL — AUTO-FETCH", body))
    except Exception as exc:
        logger.warning("emergency fetch error: %s", exc)


# ---------------------------------------------------------------------------
# /clean  /purge
# ---------------------------------------------------------------------------

@require_admin
async def cmd_clean(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = await pm.async_load_proxies()
    removed = pm.remove_blocked(proxies)
    await pm.async_save_proxies(proxies)
    if removed:
        await update.message.reply_text(
            f"🧹 Removed {removed} dead proxy(ies). {len(proxies)} remaining."
        )
    else:
        await update.message.reply_text("✨ Nothing to clean — no dead proxies found.")


@require_admin
async def cmd_purge(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg     = pm.load_config()
    days    = cfg.get("stale_days", 7)
    proxies = await pm.async_load_proxies()
    removed = pm.purge_stale(proxies, max_dead_days=days)
    await pm.async_save_proxies(proxies)
    if removed:
        await update.message.reply_text(
            f"🗑 Purged {removed} stale proxy(ies) (dead > {days} days).\n"
            f"{len(proxies)} remaining."
        )
    else:
        await update.message.reply_text(
            f"✨ Nothing to purge — no proxy has been dead for > {days} days."
        )


# ---------------------------------------------------------------------------
# /export  /import
# ---------------------------------------------------------------------------

@require_admin
async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = await pm.async_load_proxies()
    if not proxies:
        await update.message.reply_text("📭 No proxies to export.")
        return

    data = json.dumps(proxies, indent=2, ensure_ascii=False).encode("utf-8")
    ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    name = f"proxies_{ts}.json"
    await update.message.reply_document(
        document=InputFile(io.BytesIO(data), filename=name),
        caption=f"📦 {len(proxies)} proxies exported.",
    )


@require_admin
async def cmd_import_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📥 <b>Import proxies</b>\n\n"
        "Send a <code>.json</code> or <code>.txt</code> file directly to this chat.\n\n"
        "Accepted formats:\n"
        "  • JSON array of proxy dicts (from /export)\n"
        "  • JSON array of tg://proxy or https://t.me/proxy links\n"
        "  • Plain text — one proxy per line",
        parse_mode=ParseMode.HTML,
    )


async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle file uploads for /import and /restore."""
    cfg = pm.load_config()
    if not is_admin(update.effective_user.id, cfg):
        return

    doc   = update.message.document
    if not doc:
        return
    fname = (doc.file_name or "").lower()

    # #18 — /restore: ZIP file
    if fname.endswith(".zip"):
        await _handle_restore(update, doc)
        return

    if not (fname.endswith(".json") or fname.endswith(".txt")):
        await update.message.reply_text("⚠️ Please send a .json, .txt, or .zip file.")
        return

    msg = await update.message.reply_text("📥 Parsing file…")
    try:
        tg_file = await doc.get_file()
        raw     = await tg_file.download_as_bytearray()
        text    = raw.decode("utf-8", errors="replace")
    except Exception as exc:
        await msg.edit_text(f"❌ Failed to download file: {exc}")
        return

    imported = []
    invalid  = 0
    try:
        data = json.loads(text)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "server" in item and "port" in item:
                    imported.append(item)
                elif isinstance(item, str):
                    p = pm.parse_proxy(item)
                    if p:
                        imported.append(p)
                    else:
                        invalid += 1
    except json.JSONDecodeError:
        for line in text.splitlines():
            line = line.strip()
            if line:
                p = pm.parse_proxy(line)
                if p:
                    imported.append(p)
                else:
                    invalid += 1

    if not imported:
        await msg.edit_text("❌ No parseable proxies found in the file.")
        return

    proxies  = await pm.async_load_proxies()
    max_p    = cfg.get("max_proxies", 1000)
    added    = 0
    dupes    = 0
    blacklisted = 0
    for p in imported:
        if len(proxies) >= max_p:
            break
        if pm.is_blacklisted(p):
            blacklisted += 1
            continue
        if pm.add_proxy(proxies, p):
            added += 1
        else:
            dupes += 1

    await pm.async_save_proxies(proxies)

    # #25 — detailed deduplication report
    details = [
        f"Parsed     :  {len(imported)}",
        f"Added      :  {added}",
        f"Duplicates :  {dupes}",
    ]
    if invalid:
        details.append(f"Invalid    :  {invalid}")
    if blacklisted:
        details.append(f"Blacklisted:  {blacklisted}")
    details.append(f"Pool total :  {len(proxies)}")

    await msg.edit_text("✅ Import complete!\n\n" + "\n".join(details))


# ---------------------------------------------------------------------------
# #18 — /backup and /restore
# ---------------------------------------------------------------------------

@require_admin
async def cmd_backup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Download a ZIP of all runtime state files."""
    files_to_backup = {
        "proxies.json":    pm.PROXIES_FILE,
        "config.json":     pm.CONFIG_FILE,
        "connection.json": os.path.join(pm.DATA_DIR, "connection.json"),
        "blacklist.json":  pm.BLACKLIST_FILE,
        "geoip_cache.json": os.path.join(pm.DATA_DIR, "geoip_cache.json"),
        "dns_cache.json":  os.path.join(pm.DATA_DIR, "dns_cache.json"),
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for archive_name, path in files_to_backup.items():
            if os.path.exists(path):
                zf.write(path, archive_name)

    buf.seek(0)
    ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    name = f"proxy_backup_{ts}.zip"
    await update.message.reply_document(
        document=InputFile(buf, filename=name),
        caption=(
            "📦 <b>Backup complete</b>\n\n"
            "Send this ZIP back to the bot with /restore to recover.\n"
            "<i>Contains: proxies, config, connection state, blacklist, caches.</i>"
        ),
        parse_mode=ParseMode.HTML,
    )


async def _handle_restore(update: Update, doc) -> None:
    """Restore state from an uploaded ZIP file (#18)."""
    msg = await update.message.reply_text("📥 Restoring from ZIP…")
    try:
        tg_file = await doc.get_file()
        raw     = await tg_file.download_as_bytearray()
    except Exception as exc:
        await msg.edit_text(f"❌ Failed to download file: {exc}")
        return

    allowed = {
        "proxies.json":     pm.PROXIES_FILE,
        "config.json":      pm.CONFIG_FILE,
        "connection.json":  os.path.join(pm.DATA_DIR, "connection.json"),
        "blacklist.json":   pm.BLACKLIST_FILE,
        "geoip_cache.json": os.path.join(pm.DATA_DIR, "geoip_cache.json"),
        "dns_cache.json":   os.path.join(pm.DATA_DIR, "dns_cache.json"),
    }

    restored = []
    try:
        with zipfile.ZipFile(io.BytesIO(bytes(raw))) as zf:
            for name in zf.namelist():
                if name in allowed:
                    data = zf.read(name)
                    # Validate JSON before writing
                    json.loads(data)
                    dest = allowed[name]
                    tmp  = dest + ".restore_tmp"
                    with open(tmp, "wb") as f:
                        f.write(data)
                    os.replace(tmp, dest)
                    restored.append(name)

        # Invalidate in-process caches
        import connection as _conn
        _conn._state_cache = None

        await msg.edit_text(
            f"✅ Restore complete!\n\nRestored: {', '.join(restored) or 'nothing'}\n\n"
            "Restart the bot for full effect."
        )
    except json.JSONDecodeError as exc:
        await msg.edit_text(f"❌ Invalid JSON in ZIP file: {exc}")
    except zipfile.BadZipFile:
        await msg.edit_text("❌ The file is not a valid ZIP archive.")
    except Exception as exc:
        await msg.edit_text(f"❌ Restore failed: {exc}")


# ---------------------------------------------------------------------------
# Channel management  (#19 — source stats)
# ---------------------------------------------------------------------------

@require_admin
async def cmd_addchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /addchannel @ProxyChannel")
        return
    channel  = ctx.args[0].lstrip("@").strip()
    cfg      = pm.load_config()
    channels = cfg.setdefault("source_channels", [])
    if channel.lower() in [c.lower() for c in channels]:
        await update.message.reply_text(f"ℹ️ @{channel} is already in the list.")
        return
    channels.append(channel)
    pm.save_config(cfg)
    await update.message.reply_text(f"✅ Added @{channel} as a proxy source channel.")


@require_admin
async def cmd_removechannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /removechannel @ProxyChannel")
        return
    channel  = ctx.args[0].lstrip("@").strip().lower()
    cfg      = pm.load_config()
    channels = cfg.get("source_channels", [])
    before   = len(channels)
    cfg["source_channels"] = [c for c in channels if c.lower() != channel]
    if len(cfg["source_channels"]) == before:
        await update.message.reply_text(f"❌ Channel @{channel} not found.")
        return
    pm.save_config(cfg)
    await update.message.reply_text(f"🗑 Removed @{channel} from source channels.")


@require_admin
async def cmd_channels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg      = pm.load_config()
    channels = cfg.get("source_channels", [])
    stats    = cfg.get("source_stats", {})

    if not channels:
        await update.message.reply_text(
            "No source channels configured.\nUse /addchannel @ChannelName to add one."
        )
        return

    lines = ["📡 <b>Source Channels</b>\n"]
    for i, ch in enumerate(channels, 1):
        key  = f"@{ch}"
        s    = stats.get(key, {})
        fetched = s.get("fetched_total", 0)
        alive   = s.get("alive_total", 0)
        pct     = f"{100*alive//fetched}%" if fetched > 0 else "—"
        last    = (s.get("last_fetch") or "never")[:10]
        lines.append(
            f"  {i}. @{ch}\n"
            f"      Fetched: {fetched}  Alive: {alive}  Quality: {pct}  Last: {last}"
        )

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# URL source management  (#19 — source stats)
# ---------------------------------------------------------------------------

@require_admin
async def cmd_addsource(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: /addsource &lt;url&gt;",
            parse_mode=ParseMode.HTML,
        )
        return
    url = ctx.args[0].strip()
    if not url.startswith(("http://", "https://")):
        await update.message.reply_text("❌ URL must start with http:// or https://")
        return
    cfg  = pm.load_config()
    urls = cfg.setdefault("source_urls", [])
    if url in urls:
        await update.message.reply_text("ℹ️ That URL is already in the source list.")
        return
    urls.append(url)
    pm.save_config(cfg)
    await update.message.reply_text(f"✅ Added URL source:\n<code>{url}</code>", parse_mode=ParseMode.HTML)


@require_admin
async def cmd_removesource(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /removesource &lt;url&gt;", parse_mode=ParseMode.HTML)
        return
    url  = ctx.args[0].strip()
    cfg  = pm.load_config()
    urls = cfg.get("source_urls", [])
    if url not in urls:
        await update.message.reply_text("❌ URL not found in sources.")
        return
    urls.remove(url)
    pm.save_config(cfg)
    await update.message.reply_text(f"🗑 Removed URL source:\n<code>{url}</code>", parse_mode=ParseMode.HTML)


@require_admin
async def cmd_sources(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg   = pm.load_config()
    urls  = cfg.get("source_urls", [])
    stats = cfg.get("source_stats", {})

    if not urls:
        await update.message.reply_text(
            "No URL sources configured.\nUse /addsource &lt;url&gt; to add one.",
            parse_mode=ParseMode.HTML,
        )
        return

    lines = ["🌐 <b>URL Sources</b>\n"]
    for i, u in enumerate(urls, 1):
        s       = stats.get(u, {})
        fetched = s.get("fetched_total", 0)
        alive   = s.get("alive_total", 0)
        pct     = f"{100*alive//fetched}%" if fetched > 0 else "—"
        last    = (s.get("last_fetch") or "never")[:10]
        lines.append(
            f"  {i}. <code>{u[:60]}{'…' if len(u)>60 else ''}</code>\n"
            f"      Fetched: {fetched}  Alive: {alive}  Quality: {pct}  Last: {last}"
        )
    await reply(update, "\n".join(lines))


# ---------------------------------------------------------------------------
# /fetch  (with source stats update, #19)
# ---------------------------------------------------------------------------

@require_admin
async def cmd_fetch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg      = pm.load_config()
    channels = cfg.get("source_channels", [])
    urls     = cfg.get("source_urls", [])

    if not channels and not urls:
        await update.message.reply_text(
            "No sources set.\nUse /addchannel @ChannelName or /addsource <url> first."
        )
        return

    parts = []
    if channels:
        parts.append(f"{len(channels)} channel(s)")
    if urls:
        parts.append(f"{len(urls)} URL source(s)")
    msg = await update.message.reply_text(f"📡 Fetching from {', '.join(parts)}…")

    found   = await fetcher.fetch_all(channels, urls)
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)
    max_p   = cfg.get("max_proxies", 1000)
    added   = 0
    for p in found:
        if len(proxies) >= max_p:
            break
        if pm.add_proxy(proxies, p):
            added += 1
    await pm.async_save_proxies(proxies)

    # #19 — update source stats (rough estimate: all found attributed to all sources)
    for ch in channels:
        pm.update_source_stats(cfg, f"@{ch}",
                               fetched=len(found) // max(len(channels) + len(urls), 1))
    for u in urls:
        pm.update_source_stats(cfg, u,
                               fetched=len(found) // max(len(channels) + len(urls), 1))
    pm.save_config(cfg)

    await msg.edit_text(
        f"📥 Fetch complete!\n\n"
        f"Found  : {len(found)}\n"
        f"New    : {added}\n"
        f"Total  : {len(proxies)}"
    )


# ---------------------------------------------------------------------------
# /crawl — search directories + optional Telethon search for proxy groups
# ---------------------------------------------------------------------------

@require_admin
async def cmd_crawl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /crawl [keyword1 keyword2 ...]

    Discovers Telegram proxy channels via:
      1. Public web directory search (tgstat, telemetr, lyzem, tchannels)
      2. Telethon: join discovered channels/groups, read message history,
         discover similar channels via Telegram's recommendations API
      3. Fallback (no session): scrape public t.me/s/ pages
      4. Telethon global message search across all of Telegram

    Found proxies of all types (MTProto, SOCKS5, SOCKS4) are added to the pool.
    Run /check afterwards to verify them.
    """
    cfg = pm.load_config()

    # Keywords: from args, or fall back to config, or fall back to defaults
    if ctx.args:
        keywords = [" ".join(ctx.args)]          # treat whole arg string as one query
    else:
        keywords = cfg.get("crawl_keywords", crawler.DEFAULT_KEYWORDS)

    # Telethon credentials from daemon_config if available
    api_id, api_hash, session_file = 0, "", ""
    daemon_cfg_path = os.path.join(pm.DATA_DIR, "daemon_config.json")
    if os.path.exists(daemon_cfg_path):
        try:
            with open(daemon_cfg_path) as f:
                dc = json.load(f)
            api_id      = int(dc.get("api_id", 0) or 0)
            api_hash    = str(dc.get("api_hash", "") or "")
            session_file = os.path.join(pm.DATA_DIR, "daemon_session")
        except Exception:
            pass

    _sf = session_file if not session_file.endswith(".session") else session_file[:-8]
    use_telethon = bool(api_id and api_hash and
                        os.path.exists(_sf + ".session"))

    # Build the status message
    kw_display = ", ".join(f'"{k}"' for k in keywords[:3])
    if len(keywords) > 3:
        kw_display += f" +{len(keywords)-3} more"
    strategy = "join + fetch + similar discovery" if use_telethon else "public page scraping"
    msg = await update.message.reply_text(
        f"🔍 <b>Crawling for proxy channels…</b>\n\n"
        f"  Keywords : {kw_display}\n"
        f"  Strategy : {strategy}\n\n"
        f"  <i>Stage 1: Querying channel directories…</i>",
        parse_mode=ParseMode.HTML,
    )

    last_edit = {"stage": "", "cur": -1}

    async def _progress(stage: str, cur: int, total: int) -> None:
        # Rate-limit edits: skip if same stage + same progress
        if stage == last_edit["stage"] and cur == last_edit["cur"]:
            return
        last_edit["stage"] = stage
        last_edit["cur"]   = cur
        try:
            if stage == "directories":
                await msg.edit_text(
                    f"🔍 <b>Crawling for proxy channels…</b>\n\n"
                    f"  Keywords : {kw_display}\n"
                    f"  Strategy : {strategy}\n\n"
                    f"  <i>Stage 1: Querying channel directories…</i>",
                    parse_mode=ParseMode.HTML,
                )
            elif stage == "channels":
                await msg.edit_text(
                    f"🔍 <b>Crawling for proxy channels…</b>\n\n"
                    f"  Keywords : {kw_display}\n\n"
                    f"  <i>Stage 2: Scraping public pages… ({cur}/{total})</i>",
                    parse_mode=ParseMode.HTML,
                )
            elif stage == "join_fetch":
                await msg.edit_text(
                    f"🔍 <b>Crawling for proxy channels…</b>\n\n"
                    f"  Keywords : {kw_display}\n\n"
                    f"  <i>Stage 2: Joining &amp; reading channels… ({cur}/{total})</i>",
                    parse_mode=ParseMode.HTML,
                )
            elif stage == "similar":
                await msg.edit_text(
                    f"🔍 <b>Crawling for proxy channels…</b>\n\n"
                    f"  Keywords : {kw_display}\n\n"
                    f"  <i>Stage 2b: Reading similar/recommended channels… ({cur}/{total})</i>",
                    parse_mode=ParseMode.HTML,
                )
            elif stage == "telethon":
                await msg.edit_text(
                    f"🔍 <b>Crawling for proxy channels…</b>\n\n"
                    f"  Keywords : {kw_display}\n\n"
                    f"  <i>Stage 3: Telegram global message search…</i>",
                    parse_mode=ParseMode.HTML,
                )
        except Exception:
            pass

    limit_ch = cfg.get("crawl_channel_limit", 30)
    pages_ch = cfg.get("crawl_pages_per_channel", 5)

    try:
        discovered_chs, found_proxies = await crawler.crawl(
            keywords         = keywords,
            use_telethon     = use_telethon,
            limit_channels   = limit_ch,
            pages_per_channel = pages_ch,
            api_id           = api_id,
            api_hash         = api_hash,
            session_file     = session_file,
            progress_callback = _progress,
        )
    except Exception as exc:
        await msg.edit_text(f"❌ Crawl failed: {exc}")
        return

    # Add discovered proxies to pool
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)
    max_p   = cfg.get("max_proxies", 1000)
    added   = 0
    dupes   = 0
    for p in found_proxies:
        if len(proxies) >= max_p:
            break
        if pm.add_proxy(proxies, p):
            added += 1
        else:
            dupes += 1
    await pm.async_save_proxies(proxies)

    # Auto-add newly discovered channels to source_channels
    existing_channels = {c.lower() for c in cfg.get("source_channels", [])}
    new_sources = [ch for ch in discovered_chs
                   if ch.lower() not in existing_channels][:10]
    if new_sources and cfg.get("crawl_auto_add_channels", True):
        cfg.setdefault("source_channels", []).extend(new_sources)
        pm.save_config(cfg)

    body = (
        f"  Keywords      :  {kw_display}\n"
        f"  Channels found:  {len(discovered_chs)}\n"
        f"  Proxies found :  {len(found_proxies)}\n"
        f"  Added to pool :  {added}\n"
        f"  Duplicates    :  {dupes}\n"
        f"  Pool total    :  {len(proxies)}\n"
        + (f"  New sources   :  {len(new_sources)} channel(s) added to /channels\n"
           if new_sources else "")
        + "\n  Run /check to test the new proxies."
    )
    await msg.edit_text(
        _card("🕸", "CRAWL COMPLETE", body),
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# #12 — /settings — interactive inline keyboard editor
# ---------------------------------------------------------------------------

def _settings_keyboard(cfg: dict) -> InlineKeyboardMarkup:
    """Build inline keyboard for interactive settings (#12)."""
    rows = [
        [InlineKeyboardButton(
            f"✅ Check: {cfg.get('check_interval_minutes', 30)} min",
            callback_data="settings:check"
        )],
        [InlineKeyboardButton(
            f"📡 Fetch: {cfg.get('auto_fetch_interval_minutes', 60)} min",
            callback_data="settings:fetch"
        )],
        [InlineKeyboardButton(
            f"👁 Monitor: {cfg.get('monitor_interval_minutes', 2)} min",
            callback_data="settings:monitor"
        )],
        [InlineKeyboardButton(
            f"🔔 Notify cooldown: {cfg.get('notification_cooldown_minutes', 5)} min",
            callback_data="settings:notify"
        )],
        [InlineKeyboardButton(
            f"📦 Max proxies: {cfg.get('max_proxies', 1000)}",
            callback_data="settings:maxproxies"
        )],
        [InlineKeyboardButton(
            f"🧹 Auto-clean: {'ON' if cfg.get('auto_clean_enabled') else 'OFF'}",
            callback_data="settings:autoclean"
        ),
         InlineKeyboardButton(
            f"🗑 Auto-purge: {'ON' if cfg.get('auto_purge_enabled') else 'OFF'}",
            callback_data="settings:autopurge"
        )],
        [InlineKeyboardButton(
            f"🌐 Dashboard: {'ON' if cfg.get('web_dashboard_enabled') else 'OFF'}",
            callback_data="settings:dashboard"
        )],
        [InlineKeyboardButton("✅ Done", callback_data="settings:done")],
    ]
    return InlineKeyboardMarkup(rows)


@require_admin
async def cmd_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg   = pm.load_config()
    state = conn.get_state()
    text  = (
        "⚙️ <b>Settings</b>\n\n"
        "Tap a button to toggle or cycle its value.\n"
        "Use /setinterval, /setfetch, /setmonitor for precise values.\n\n"
        f"Active proxy   : {'yes 🔌' if state.get('active_proxy') else 'none'}\n"
        f"Monitoring     : {'ON ✅' if state.get('monitoring') else 'OFF ❌'}\n"
        f"Admin IDs      : {cfg.get('admin_ids') or 'all users'}\n"
        f"Source channels: {len(cfg.get('source_channels', []))}\n"
        f"Source URLs    : {len(cfg.get('source_urls', []))}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                    reply_markup=_settings_keyboard(cfg))


async def cb_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle interactive settings keyboard presses (#12)."""
    query = update.callback_query
    await query.answer()

    cfg = pm.load_config()
    if not is_admin(query.from_user.id, cfg):
        return

    action = query.data.split(":")[1]

    if action == "done":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    # Toggle booleans
    if action == "autoclean":
        cfg["auto_clean_enabled"] = not cfg.get("auto_clean_enabled", False)
    elif action == "autopurge":
        cfg["auto_purge_enabled"] = not cfg.get("auto_purge_enabled", False)
    elif action == "dashboard":
        cfg["web_dashboard_enabled"] = not cfg.get("web_dashboard_enabled", False)

    # Cycle numeric values
    elif action == "check":
        opts = [5, 15, 30, 60, 120]
        cur  = cfg.get("check_interval_minutes", 30)
        cfg["check_interval_minutes"] = opts[(opts.index(cur) + 1) % len(opts)] if cur in opts else 30
    elif action == "fetch":
        opts = [10, 30, 60, 120, 240]
        cur  = cfg.get("auto_fetch_interval_minutes", 60)
        cfg["auto_fetch_interval_minutes"] = opts[(opts.index(cur) + 1) % len(opts)] if cur in opts else 60
    elif action == "monitor":
        opts = [1, 2, 5, 10, 15]
        cur  = cfg.get("monitor_interval_minutes", 2)
        cfg["monitor_interval_minutes"] = opts[(opts.index(cur) + 1) % len(opts)] if cur in opts else 2
    elif action == "notify":
        opts = [0, 1, 5, 10, 30]
        cur  = cfg.get("notification_cooldown_minutes", 5)
        cfg["notification_cooldown_minutes"] = opts[(opts.index(cur) + 1) % len(opts)] if cur in opts else 5
    elif action == "maxproxies":
        opts = [100, 250, 500, 1000, 2000]
        cur  = cfg.get("max_proxies", 1000)
        cfg["max_proxies"] = opts[(opts.index(cur) + 1) % len(opts)] if cur in opts else 1000

    pm.save_config(cfg)

    try:
        await query.edit_message_reply_markup(reply_markup=_settings_keyboard(cfg))
    except Exception:
        pass


@require_admin
async def cmd_setinterval(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args or not ctx.args[0].isdigit():
        await update.message.reply_text("Usage: /setinterval <minutes>")
        return
    minutes = int(ctx.args[0])
    if minutes < 5:
        await update.message.reply_text("⚠️ Minimum interval is 5 minutes.")
        return
    cfg = pm.load_config()
    cfg["check_interval_minutes"] = minutes
    pm.save_config(cfg)
    _reschedule_check(ctx.application, minutes)
    await update.message.reply_text(f"✅ Full-check interval set to {minutes} minutes.")


@require_admin
async def cmd_setfetch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args or not ctx.args[0].isdigit():
        await update.message.reply_text("Usage: /setfetch <minutes>")
        return
    minutes = int(ctx.args[0])
    if minutes < 10:
        await update.message.reply_text("⚠️ Minimum fetch interval is 10 minutes.")
        return
    cfg = pm.load_config()
    cfg["auto_fetch_interval_minutes"] = minutes
    pm.save_config(cfg)
    _reschedule_fetch(ctx.application, minutes)
    await update.message.reply_text(f"✅ Fetch interval set to {minutes} minutes.")


@require_admin
async def cmd_setmonitor(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args or not ctx.args[0].isdigit():
        await update.message.reply_text("Usage: /setmonitor <minutes>  (minimum 1)")
        return
    minutes = int(ctx.args[0])
    if minutes < 1:
        await update.message.reply_text("⚠️ Minimum monitor interval is 1 minute.")
        return
    cfg = pm.load_config()
    cfg["monitor_interval_minutes"] = minutes
    pm.save_config(cfg)
    _reschedule_monitor(ctx.application, minutes)
    await update.message.reply_text(f"✅ Monitor interval set to {minutes} minutes.")


@require_admin
async def cmd_setnotify(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """#13 — /setnotify normal|silent|verbose"""
    modes = {
        "silent":  0,
        "normal":  5,
        "verbose": 0,  # verbose = 0 cooldown (all notifications pass through)
    }
    if not ctx.args or ctx.args[0].lower() not in modes:
        await update.message.reply_text(
            "Usage: /setnotify &lt;mode&gt;\n\n"
            "  normal  — notifications with 5-min cooldown between rotation alerts\n"
            "  silent  — only critical failures (no rotation alerts)\n"
            "  verbose — all notifications, no throttling",
            parse_mode=ParseMode.HTML,
        )
        return
    mode    = ctx.args[0].lower()
    cooldown = modes[mode]
    cfg = pm.load_config()
    cfg["notification_cooldown_minutes"] = cooldown
    cfg["notify_mode"] = mode
    pm.save_config(cfg)
    await update.message.reply_text(f"✅ Notification mode set to <b>{mode}</b>.", parse_mode=ParseMode.HTML)


@require_admin
async def cmd_reload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg = pm.load_config()
    _setup_logging(cfg)
    _reschedule_check(ctx.application,   cfg.get("check_interval_minutes",      30))
    _reschedule_fetch(ctx.application,   cfg.get("auto_fetch_interval_minutes", 60))
    _reschedule_monitor(ctx.application, cfg.get("monitor_interval_minutes",     2))
    _reschedule_digest(ctx.application,  cfg.get("daily_digest_hour",            9))
    _reschedule_maintenance(ctx.application, cfg)
    await update.message.reply_text(
        "✅ Config reloaded.\n\n"
        f"Check    : {cfg.get('check_interval_minutes', 30)} min\n"
        f"Fetch    : {cfg.get('auto_fetch_interval_minutes', 60)} min\n"
        f"Monitor  : {cfg.get('monitor_interval_minutes', 2)} min\n"
        f"Digest   : daily at {cfg.get('daily_digest_hour', 9)}:00 UTC\n"
        f"Log level: {cfg.get('log_level', 'INFO')}"
    )


# ---------------------------------------------------------------------------
# /daemon
# ---------------------------------------------------------------------------

@require_admin
async def cmd_daemon(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    running, pid = _daemon_running()
    state        = conn.get_state()
    daemon_proxy = state.get("active_proxy")

    status_line = f"🟢 Running  (PID {pid})" if running else "🔴 Not running"

    if daemon_proxy:
        lat     = daemon_proxy.get("latency_ms")
        lat_str = f"  ({lat:.0f} ms)" if lat is not None else ""
        flag    = pm.country_flag(daemon_proxy.get("country_code"))
        proxy_line = (
            f"\n🔌 Active proxy{lat_str}:\n"
            f"  {flag} <code>{daemon_proxy['server']}:{daemon_proxy['port']}</code>\n"
            f"  <code>{pm.proxy_to_tg_link(daemon_proxy)}</code>"
        )
    else:
        proxy_line = "\nNo active proxy from daemon."

    rotations = state.get("rotations", 0)
    last_rot  = state.get("last_rotated") or "never"

    text = (
        f"🤖 <b>Auto-Proxy Daemon</b>\n\n"
        f"Status     : {status_line}\n"
        f"Rotations  : {rotations} total  (last: {last_rot})\n"
        + proxy_line
    )
    if not running:
        text += "\n\n💡 Start with:  <code>bash run_daemon.sh</code>"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# /stats
# ---------------------------------------------------------------------------

@require_admin
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = await pm.async_load_proxies()
    total   = len(proxies)
    alive   = [p for p in proxies if p.get("alive") is True]
    dead    = [p for p in proxies if p.get("alive") is False]
    unk     = [p for p in proxies if p.get("alive") is None]

    types: dict[str, int] = {}
    for p in proxies:
        t = p.get("type", "unknown")
        types[t] = types.get(t, 0) + 1

    lats = [p["latency_ms"] for p in alive if p.get("latency_ms") is not None]
    if lats:
        avg_lat = sum(lats) / len(lats)
        lat_str = f"{avg_lat:.0f} ms avg  ({min(lats):.0f}–{max(lats):.0f} ms range)"
    else:
        lat_str = "N/A"

    uptimes = []
    for p in proxies:
        chk = p.get("check_count", 0)
        suc = p.get("success_count", 0)
        if chk > 0:
            uptimes.append(suc / chk)
    avg_uptime = (sum(uptimes) / len(uptimes) * 100) if uptimes else 0.0

    state     = conn.get_state()
    rotations = state.get("rotations", 0)
    last_rot  = state.get("last_rotated") or "never"

    # Country breakdown (top 5)
    country_counts: dict[str, int] = {}
    for p in alive:
        cc = p.get("country_code") or "??"
        country_counts[cc] = country_counts.get(cc, 0) + 1
    top_countries = sorted(country_counts.items(), key=lambda x: -x[1])[:5]
    country_lines = "  ".join(
        f"{pm.country_flag(cc)}{cc}:{n}" for cc, n in top_countries
    )

    type_lines = "  ".join(f"{t.upper()}:{c}" for t, c in sorted(types.items()))

    bl_count = len(pm.load_blacklist())

    body = (
        f"  Total      :  {total}\n"
        f"  ✅ Alive    :  {len(alive)}\n"
        f"  ❌ Dead     :  {len(dead)}\n"
        f"  ❓ Unchecked:  {len(unk)}\n"
        f"  🚫 Blacklist:  {bl_count}\n\n"
        f"  Latency    :  {lat_str}\n"
        f"  Avg Uptime :  {avg_uptime:.1f}%\n"
        f"  Rotations  :  {rotations}  (last: {last_rot})\n\n"
        f"  Types      :  {type_lines}\n"
        f"  Countries  :  {country_lines or '—'}"
    )
    await reply(update, _card("📊", "POOL STATISTICS", body))


# ---------------------------------------------------------------------------
# #25 — /logs — show recent log lines
# ---------------------------------------------------------------------------

@require_admin
async def cmd_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    n = 20
    if ctx.args and ctx.args[0].isdigit():
        n = min(int(ctx.args[0]), 50)

    lines = list(_LOG_BUFFER)[-n:]
    if not lines:
        await update.message.reply_text("ℹ️ No log lines captured yet.")
        return

    text = "📋 <b>Recent Logs</b>\n\n<pre>" + "\n".join(lines) + "</pre>"
    await reply(update, text)


# ---------------------------------------------------------------------------
# Scheduled jobs
# ---------------------------------------------------------------------------

_CHECK_JOB       = "auto_check"
_FETCH_JOB       = "auto_fetch"
_MONITOR_JOB     = "monitor_active"
_DIGEST_JOB      = "daily_digest"
_CLEAN_JOB       = "auto_clean"      # #20
_PURGE_JOB       = "auto_purge"      # #20


def _reschedule_check(app, minutes: int) -> None:
    jq = app.job_queue
    for job in jq.get_jobs_by_name(_CHECK_JOB):
        job.schedule_removal()
    jq.run_repeating(_job_check, interval=minutes * 60, first=minutes * 60, name=_CHECK_JOB)


def _reschedule_fetch(app, minutes: int) -> None:
    jq = app.job_queue
    for job in jq.get_jobs_by_name(_FETCH_JOB):
        job.schedule_removal()
    jq.run_repeating(_job_fetch, interval=minutes * 60, first=minutes * 60, name=_FETCH_JOB)


def _reschedule_monitor(app, minutes: int) -> None:
    jq = app.job_queue
    for job in jq.get_jobs_by_name(_MONITOR_JOB):
        job.schedule_removal()
    jq.run_repeating(_job_monitor_active, interval=minutes * 60, first=30, name=_MONITOR_JOB)


def _reschedule_digest(app, hour_utc: int) -> None:
    jq = app.job_queue
    for job in jq.get_jobs_by_name(_DIGEST_JOB):
        job.schedule_removal()
    import datetime as dt_mod
    jq.run_daily(_job_daily_digest,
                 time=dt_mod.time(hour=hour_utc % 24, minute=0, tzinfo=timezone.utc),
                 name=_DIGEST_JOB)


def _reschedule_maintenance(app, cfg: dict) -> None:
    """#20 — Schedule or cancel auto-clean and auto-purge jobs."""
    jq = app.job_queue

    for job in jq.get_jobs_by_name(_CLEAN_JOB):
        job.schedule_removal()
    if cfg.get("auto_clean_enabled", False):
        hours = cfg.get("auto_clean_interval_hours", 24)
        jq.run_repeating(_job_auto_clean, interval=hours * 3600, first=hours * 3600,
                         name=_CLEAN_JOB)

    for job in jq.get_jobs_by_name(_PURGE_JOB):
        job.schedule_removal()
    if cfg.get("auto_purge_enabled", False):
        hours = cfg.get("auto_purge_interval_hours", 24)
        jq.run_repeating(_job_auto_purge, interval=hours * 3600, first=hours * 3600,
                         name=_PURGE_JOB)


# ---------------------------------------------------------------------------
# #20 — Auto-maintenance jobs
# ---------------------------------------------------------------------------

async def _job_auto_clean(context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg     = pm.load_config()
    proxies = await pm.async_load_proxies()
    removed = pm.remove_blocked(proxies)
    if removed:
        await pm.async_save_proxies(proxies)
        await _broadcast(context.bot, cfg,
                         _card("🧹", "AUTO-CLEAN", f"  Removed {removed} dead proxy(ies).\n  Pool: {len(proxies)} remaining"))
    logger.info("auto-clean: removed %d dead proxies", removed)


async def _job_auto_purge(context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg     = pm.load_config()
    days    = cfg.get("stale_days", 7)
    proxies = await pm.async_load_proxies()
    removed = pm.purge_stale(proxies, max_dead_days=days)
    if removed:
        await pm.async_save_proxies(proxies)
        await _broadcast(context.bot, cfg,
                         _card("🗑", "AUTO-PURGE",
                               f"  Purged {removed} stale proxy(ies) (dead > {days} days).\n  Pool: {len(proxies)} remaining"))
    logger.info("auto-purge: purged %d stale proxies", removed)


# ---------------------------------------------------------------------------
# _job_monitor_active — core auto-rotation engine
# ---------------------------------------------------------------------------

async def _job_monitor_active(context: ContextTypes.DEFAULT_TYPE) -> None:
    global _active_fail_count

    if not conn.is_monitoring():
        return

    cfg    = pm.load_config()
    active = conn.get_active()
    if not active:
        return

    timeout = cfg.get("check_timeout_seconds", 8)
    alive   = await checker.check_one(active, timeout=timeout)
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)
    await pm.async_save_proxies(proxies)

    if alive:
        if _active_fail_count > 0:
            logger.info("monitor: proxy %s:%s recovered after %d failure(s)",
                        active["server"], active["port"], _active_fail_count)
        _active_fail_count = 0

        history     = active.get("latency_history", [])
        current_lat = active.get("latency_ms")
        if history and current_lat is not None and len(history) >= 3:
            sorted_h = sorted(history[:-1])
            mid      = len(sorted_h) // 2
            baseline = float(sorted_h[mid]) if len(sorted_h) % 2 else (sorted_h[mid-1]+sorted_h[mid])/2.0
            if baseline > 0 and current_lat > baseline * 3 and current_lat > 500:
                body = (
                    f"{_proxy_summary(active)}\n\n"
                    f"  Current  :  {current_lat:.0f} ms\n"
                    f"  Baseline :  {baseline:.0f} ms  (3× threshold)"
                )
                await _broadcast(context.bot, cfg,
                                 _card("⚠️", "LATENCY SPIKE", body, "Proxy may degrade soon."))
        return

    _active_fail_count += 1
    threshold = cfg.get("min_failures_to_rotate", 2)

    if _active_fail_count < threshold:
        monitor_min = cfg.get("monitor_interval_minutes", 5)
        body = (
            f"{_proxy_summary(active)}\n\n"
            f"  Failures  :  {_active_fail_count} / {threshold}\n"
            f"  Next check:  ~{monitor_min} min"
        )
        notify_mode = cfg.get("notify_mode", "normal")
        if notify_mode != "silent":
            await _broadcast(context.bot, cfg,
                             _card("🟡", "PROXY CHECK FAILED", body,
                                   "Monitoring continues — not rotating yet."))
        return

    _active_fail_count = 0

    await _broadcast(
        context.bot, cfg,
        _card("🔴", "PROXY FAILED", _proxy_summary(active), "🔄 Searching for replacement…"),
        throttle_rotation=True,  # #13 — respect cooldown
    )

    dead_key = pm.proxy_key(active)
    for p in proxies:
        if pm.proxy_key(p) == dead_key:
            p["recently_failed_at"] = datetime.now(timezone.utc).isoformat()
            break

    if cfg.get("auto_remove_blocked", True):
        pm.remove_blocked(proxies)
        await pm.async_save_proxies(proxies)

    candidates = [
        p for p in pm.sort_by_score(proxies)
        if p.get("alive") is True and pm.proxy_key(p) != pm.proxy_key(active)
    ][:8]
    if candidates:
        await asyncio.gather(
            *[checker.check_one(p, timeout=5) for p in candidates],
            return_exceptions=True
        )
        await pm.async_save_proxies(proxies)

    channels = cfg.get("source_channels", [])
    urls     = cfg.get("source_urls", [])

    async def _no_notify(msg):
        pass

    new_proxy = await conn.auto_rotate(
        proxies, channels, urls=urls, timeout=timeout, notify_fn=_no_notify,
    )

    if new_proxy:
        state    = conn.get_state()
        rot_body = (
            f"{_proxy_summary(new_proxy)}\n\n"
            f"  Rotation  :  #{state.get('rotations', 0)}"
        )
        deeplink = pm.proxy_to_tg_deeplink(new_proxy)
        kb = None
        if deeplink:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔗 Apply Proxy in Telegram", url=deeplink)
            ]])
        await _broadcast(
            context.bot, cfg,
            _card("✅", "NEW PROXY ACTIVE", rot_body),
            reply_markup=kb,
            throttle_rotation=True,
        )
    else:
        await _broadcast(
            context.bot, cfg,
            _card("🚨", "ROTATION FAILED",
                  "  All proxies are exhausted.\n  Run /fetch to pull fresh ones.")
        )


# ---------------------------------------------------------------------------
# _job_check — periodic full TCP check
# ---------------------------------------------------------------------------

async def _job_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg     = pm.load_config()
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)

    if not proxies:
        return

    timeout = cfg.get("check_timeout_seconds", 8)
    summary = await checker.check_all(proxies, timeout=timeout)
    await pm.async_save_proxies(proxies)

    removed = 0
    if cfg.get("auto_remove_blocked", True):
        removed = pm.remove_blocked(proxies)
        if removed:
            await pm.async_save_proxies(proxies)

    stale = pm.purge_stale(proxies, cfg.get("stale_days", 7))
    if stale:
        await pm.async_save_proxies(proxies)

    asyncio.create_task(_enrich_and_save(proxies))

    alive_now = sum(1 for p in proxies if p.get("alive") is True)
    threshold = cfg.get("low_pool_threshold", 15)
    if alive_now < threshold:
        asyncio.create_task(_background_emergency_fetch(context.bot, cfg, proxies))

    logger.info("auto-check: %d total %d alive %d dead %d removed %d purged",
                summary["total"], summary["alive"], summary["dead"], removed, stale)

    extras = []
    if removed:
        extras.append(f"  🗑 Removed  :  {removed} dead")
    if stale:
        extras.append(f"  🕰 Purged   :  {stale} stale")

    body = (
        f"  Checked   :  {summary['total']}\n"
        f"  ✅ Alive   :  {summary['alive']}\n"
        f"  ❌ Dead    :  {summary['dead']}\n"
        f"  📦 Pool    :  {len(proxies)} remaining"
        + ("\n\n" + "\n".join(extras) if extras else "")
    )
    await _broadcast(context.bot, cfg, _card("🤖", "BACKGROUND CHECK", body))


# ---------------------------------------------------------------------------
# _job_fetch — periodic channel + URL scrape  (#19 source stats)
# ---------------------------------------------------------------------------

async def _job_fetch(context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg      = pm.load_config()
    channels = cfg.get("source_channels", [])
    urls     = cfg.get("source_urls", [])

    if not channels and not urls:
        return

    found   = await fetcher.fetch_all(channels, urls)
    proxies = await pm.async_load_proxies()
    pm.backfill_fields(proxies)
    max_p   = cfg.get("max_proxies", 1000)
    added   = 0
    for p in found:
        if len(proxies) >= max_p:
            break
        if pm.add_proxy(proxies, p):
            added += 1
    if added:
        await pm.async_save_proxies(proxies)

    # Check new proxies and update source stats
    if added:
        timeout  = cfg.get("check_timeout_seconds", 8)
        new_ones = [p for p in proxies if p.get("alive") is None]
        new_alive = 0
        if new_ones:
            summary   = await checker.check_all(new_ones, timeout=timeout)
            await pm.async_save_proxies(proxies)
            new_alive = summary["alive"]

        # Update source stats (#19) — rough split
        n_sources = max(len(channels) + len(urls), 1)
        for ch in channels:
            pm.update_source_stats(cfg, f"@{ch}",
                                   fetched=len(found) // n_sources,
                                   alive_added=new_alive // n_sources)
        for u in urls:
            pm.update_source_stats(cfg, u,
                                   fetched=len(found) // n_sources,
                                   alive_added=new_alive // n_sources)
        pm.save_config(cfg)

        body = (
            f"  Sources    :  {len(channels)} ch  +  {len(urls)} URLs\n"
            f"  Found      :  {len(found)}\n"
            f"  New added  :  {added}\n"
            f"  ✅ Alive    :  {new_alive}  (checked immediately)\n"
            f"  📦 Total   :  {len(proxies)}"
        )
        await _broadcast(context.bot, cfg, _card("📥", "AUTO-FETCH COMPLETE", body))

    logger.info("auto-fetch: found %d added %d", len(found), added)


# ---------------------------------------------------------------------------
# _job_daily_digest
# ---------------------------------------------------------------------------

async def _job_daily_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg     = pm.load_config()
    proxies = await pm.async_load_proxies()

    total = len(proxies)
    alive = sum(1 for p in proxies if p.get("alive") is True)
    dead  = sum(1 for p in proxies if p.get("alive") is False)

    alive_list = [p for p in proxies if p.get("alive") is True]
    best_line  = ""
    if alive_list:
        best  = pm.sort_by_score(alive_list)[0]
        lat   = best.get("latency_ms")
        lat_s = f"{lat:.0f}ms" if lat is not None else "?"
        flag  = pm.country_flag(best.get("country_code"))
        best_line = (
            f"\nBest proxy  : {flag} <code>{best['server']}:{best['port']}</code>"
            f"  {lat_s}"
        )

    state     = conn.get_state()
    rotations = state.get("rotations", 0)
    running, _ = _daemon_running()
    daemon_ico = "🟢" if running else "🔴"
    bl_count  = len(pm.load_blacklist())

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    text = (
        f"📊 <b>Daily Summary — {date_str}</b>\n\n"
        f"Pool        : {total} total  (✅{alive}  ❌{dead})\n"
        f"Daemon      : {daemon_ico}  rotations: {rotations} total\n"
        f"Blacklist   : {bl_count} entries"
        + best_line
    )
    await _broadcast(context.bot, cfg, text)


# ---------------------------------------------------------------------------
# Inline proxy paste handler
# ---------------------------------------------------------------------------

@require_admin
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text  = update.message.text or ""
    proxy = pm.parse_proxy(text)
    if not proxy:
        return

    if pm.is_blacklisted(proxy):
        await update.message.reply_text("🚫 That proxy is blacklisted.")
        return

    cfg     = pm.load_config()
    proxies = await pm.async_load_proxies()

    if len(proxies) >= cfg.get("max_proxies", 1000):
        await update.message.reply_text("⚠️ Proxy list is full. Remove some first.")
        return

    if not pm.add_proxy(proxies, proxy):
        await update.message.reply_text("ℹ️ That proxy is already in the list.")
        return

    await pm.async_save_proxies(proxies)
    link = pm.proxy_to_tg_link(proxy)
    await update.message.reply_text(
        f"✅ Proxy detected and added (#{len(proxies)}):\n<code>{link}</code>",
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# #16 — Optional web dashboard
# ---------------------------------------------------------------------------

async def _start_web_dashboard(cfg: dict) -> None:
    """Start a lightweight aiohttp status dashboard on localhost (#16)."""
    try:
        import aiohttp.web as _web
    except ImportError:
        logger.warning("aiohttp not installed — web dashboard unavailable")
        return

    port = cfg.get("web_dashboard_port", 8080)

    async def _handler(request: _web.Request) -> _web.Response:
        proxies = pm.load_proxies()
        state   = conn.get_state()
        alive   = [p for p in proxies if p.get("alive") is True]
        dead    = [p for p in proxies if p.get("alive") is False]
        ranked  = pm.sort_by_score(alive)[:10]
        active  = state.get("active_proxy")

        def _row(p: dict, idx: int) -> str:
            flag   = pm.country_flag(p.get("country_code")) or ""
            lat    = p.get("latency_ms")
            lat_s  = f"{lat:.0f}" if lat is not None else "—"
            chk    = p.get("check_count", 0)
            suc    = p.get("success_count", 0)
            up_s   = f"{100*suc//chk}%" if chk > 0 else "—"
            status = "🟢" if p.get("alive") else "🔴"
            return (
                f"<tr><td>{idx}</td><td>{status}</td>"
                f"<td>{flag} {p.get('country_name','')}</td>"
                f"<td><code>{p['server']}:{p['port']}</code></td>"
                f"<td>{p['type'].upper()}</td>"
                f"<td>{lat_s} ms</td><td>{up_s}</td></tr>"
            )

        top_rows = "".join(_row(p, i) for i, p in enumerate(ranked, 1))
        active_s = (
            f"<code>{active['server']}:{active['port']}</code>"
            if active else "None"
        )

        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Proxy Manager Dashboard</title>
<meta http-equiv="refresh" content="30">
<style>
body{{font-family:monospace;margin:2em;background:#111;color:#eee}}
table{{border-collapse:collapse;width:100%}}
th,td{{padding:6px 12px;border:1px solid #333;text-align:left}}
th{{background:#222}}
code{{background:#222;padding:2px 4px;border-radius:3px}}
.stats{{display:flex;gap:2em;margin:1em 0}}
.stat{{background:#1a1a2e;padding:1em;border-radius:8px;min-width:100px;text-align:center}}
.stat .val{{font-size:2em;font-weight:bold}}
</style></head><body>
<h1>🛡 Proxy Manager Dashboard</h1>
<p>Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC
 · Auto-refreshes every 30s</p>
<div class="stats">
  <div class="stat"><div class="val" style="color:#4ade80">{len(alive)}</div>Alive</div>
  <div class="stat"><div class="val" style="color:#f87171">{len(dead)}</div>Dead</div>
  <div class="stat"><div class="val">{len(proxies)}</div>Total</div>
  <div class="stat"><div class="val">{state.get('rotations',0)}</div>Rotations</div>
</div>
<p>Active proxy: {active_s} &nbsp; Monitoring: {'✅' if state.get('monitoring') else '❌'}</p>
<h2>Top 10 Proxies</h2>
<table><tr><th>#</th><th>Status</th><th>Country</th><th>Address</th>
<th>Type</th><th>Latency</th><th>Uptime</th></tr>
{top_rows}
</table>
</body></html>"""
        return _web.Response(text=html, content_type="text/html")

    app  = _web.Application()
    app.router.add_get("/", _handler)
    runner = _web.AppRunner(app)
    await runner.setup()
    site = _web.TCPSite(runner, "127.0.0.1", port)
    try:
        await site.start()
        logger.info("Web dashboard running at http://127.0.0.1:%d", port)
    except Exception as exc:
        logger.warning("Web dashboard failed to start on port %d: %s", port, exc)


# ---------------------------------------------------------------------------
# Startup notification
# ---------------------------------------------------------------------------

async def _post_init(app: Application) -> None:
    cfg     = pm.load_config()
    _setup_logging(cfg)

    proxies = await pm.async_load_proxies()
    alive   = sum(1 for p in proxies if p.get("alive") is True)
    active  = conn.get_active()

    active_line = ""
    if active:
        flag    = pm.country_flag(active.get("country_code")) or ""
        country = active.get("country_name") or ""
        geo     = f"{flag} {country}".strip()
        geo_str = f"  ·  {geo}" if geo else ""
        active_line = (
            f"\n\n  🔌 Restored  :  <code>{active['server']} : {active['port']}</code>"
            f"{geo_str}"
        )

    body = (
        f"  📦 Pool     :  {len(proxies)} proxies  ({alive} alive)"
        f"{active_line}\n\n"
        f"  🔁 Monitor every {cfg.get('monitor_interval_minutes', 2)} min"
    )
    await _broadcast(app.bot, cfg, _card("🟢", "BOT ONLINE", body))

    # Start web dashboard if enabled (#16)
    if cfg.get("web_dashboard_enabled", False):
        asyncio.create_task(_start_web_dashboard(cfg))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    cfg = pm.load_config()
    _setup_logging(cfg)

    token = cfg.get("bot_token", "").strip()
    if not token:
        print(
            "No bot token found in config.json.\n"
            "Edit config.json, set bot_token, then restart."
        )
        sys.exit(1)

    app = Application.builder().token(token).post_init(_post_init).build()

    # Command handlers
    app.add_handler(CommandHandler("start",         cmd_start))
    app.add_handler(CommandHandler("help",          cmd_help))
    app.add_handler(CommandHandler("connect",       cmd_connect))
    app.add_handler(CommandHandler("disconnect",    cmd_disconnect))
    app.add_handler(CommandHandler("status",        cmd_status))
    app.add_handler(CommandHandler("top",           cmd_top))
    app.add_handler(CommandHandler("info",          cmd_info))
    app.add_handler(CommandHandler("filter",        cmd_filter))
    app.add_handler(CommandHandler("add",           cmd_add))
    app.add_handler(CommandHandler("remove",        cmd_remove))
    app.add_handler(CommandHandler("list",          cmd_list))
    app.add_handler(CommandHandler("check",         cmd_check))
    app.add_handler(CommandHandler("clean",         cmd_clean))
    app.add_handler(CommandHandler("purge",         cmd_purge))
    app.add_handler(CommandHandler("export",        cmd_export))
    app.add_handler(CommandHandler("import",        cmd_import_help))
    app.add_handler(CommandHandler("share",         cmd_share))
    app.add_handler(CommandHandler("blacklist",     cmd_blacklist))
    app.add_handler(CommandHandler("unblacklist",   cmd_unblacklist))
    app.add_handler(CommandHandler("tag",           cmd_tag))
    app.add_handler(CommandHandler("untag",         cmd_untag))
    app.add_handler(CommandHandler("backup",        cmd_backup))
    app.add_handler(CommandHandler("addchannel",    cmd_addchannel))
    app.add_handler(CommandHandler("removechannel", cmd_removechannel))
    app.add_handler(CommandHandler("channels",      cmd_channels))
    app.add_handler(CommandHandler("addsource",     cmd_addsource))
    app.add_handler(CommandHandler("removesource",  cmd_removesource))
    app.add_handler(CommandHandler("sources",       cmd_sources))
    app.add_handler(CommandHandler("fetch",         cmd_fetch))
    app.add_handler(CommandHandler("settings",      cmd_settings))
    app.add_handler(CommandHandler("setinterval",   cmd_setinterval))
    app.add_handler(CommandHandler("setfetch",      cmd_setfetch))
    app.add_handler(CommandHandler("setmonitor",    cmd_setmonitor))
    app.add_handler(CommandHandler("setnotify",     cmd_setnotify))
    app.add_handler(CommandHandler("reload",        cmd_reload))
    app.add_handler(CommandHandler("daemon",        cmd_daemon))
    app.add_handler(CommandHandler("stats",         cmd_stats))
    app.add_handler(CommandHandler("logs",          cmd_logs))
    app.add_handler(CommandHandler("crawl",         cmd_crawl))

    # Inline keyboard callbacks
    app.add_handler(CallbackQueryHandler(cb_list_page, pattern=r"^list_page:"))
    app.add_handler(CallbackQueryHandler(cb_settings,  pattern=r"^settings:"))

    # File upload for /import and /restore (#18)
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Inline proxy paste
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Schedule background jobs
    check_min   = cfg.get("check_interval_minutes",       30)
    fetch_min   = cfg.get("auto_fetch_interval_minutes",  60)
    monitor_min = cfg.get("monitor_interval_minutes",      2)
    digest_hour = cfg.get("daily_digest_hour",             9)

    _reschedule_check(app,   check_min)
    _reschedule_fetch(app,   fetch_min)
    _reschedule_monitor(app, monitor_min)
    _reschedule_digest(app,  digest_hour)
    _reschedule_maintenance(app, cfg)   # #20

    logger.info(
        "Bot started. Check=%dmin Fetch=%dmin Monitor=%dmin Digest=%d:00UTC",
        check_min, fetch_min, monitor_min, digest_hour,
    )

    if conn.is_monitoring() and conn.get_active():
        active = conn.get_active()
        logger.info("Restored active proxy: %s:%s", active["server"], active["port"])

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

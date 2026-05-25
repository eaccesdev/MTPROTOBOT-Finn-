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
/top            — Show top 10 proxies ranked by latency + uptime
/check          — Re-check all proxies right now
/clean          — Delete all dead proxies
/purge          — Remove stale proxies (dead > N days)
/export         — Download the proxy pool as a JSON file
/import         — Send a .json or .txt file to import proxies

Channel sources
/addchannel <@ch>    — Add a Telegram channel to auto-fetch from
/removechannel <@ch> — Remove a source channel
/channels            — List source channels

URL sources
/addsource <url>     — Add an external URL source (GitHub raw, APIs)
/removesource <url>  — Remove a URL source
/sources             — List URL sources

Fetch
/fetch               — Pull proxies from all channels + URL sources

Settings
/settings            — Show current config values
/setinterval <min>   — Set automatic check interval (minutes)
/setfetch <min>      — Set automatic fetch interval (minutes)
/setmonitor <min>    — Set active-proxy monitor interval (minutes, default 2)
/reload              — Hot-reload config.json without restart

System
/daemon              — Show daemon process status and active proxy
/stats               — Pool statistics (latency, uptime, type breakdown)

Compatibility: Python 3.8+, Windows 7+, Linux / Termux (Android)
"""

import asyncio
import functools
import io
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone

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
        "Install it with:  pip install python-telegram-bot aiohttp\n"
    )

import proxy_manager as pm
import checker
import fetcher
import connection as conn
import geoip

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Proxies per page in /list
_LIST_PAGE_SIZE = 10

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
        await update.message.reply_text(text[i:i + MAX],
                                        parse_mode=parse_mode, **kwargs)


async def _broadcast(bot, cfg: dict, text: str, reply_markup=None):
    """Send a message to every configured admin."""
    for uid in cfg.get("admin_ids", []):
        try:
            await bot.send_message(uid, text, parse_mode=ParseMode.HTML,
                                   reply_markup=reply_markup)
        except Exception as exc:
            logger.warning("broadcast to %s failed: %s", uid, exc)


# ---------------------------------------------------------------------------
# Notification card helpers
# ---------------------------------------------------------------------------

_DIV = "─" * 32


def _card(icon: str, title: str, body: str, footer: str = "") -> str:
    """Render a clean bordered notification card."""
    parts = [_DIV, f"{icon}  <b>{title}</b>", "", body]
    if footer:
        parts += ["", footer]
    parts.append(_DIV)
    return "\n".join(parts)


def _proxy_summary(p: dict) -> str:
    """Compact multi-line proxy description for use inside a card."""
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
    lines = [
        f"  🌐 <code>{p['server']} : {p['port']}</code>",
        f"  📡 {ptype}  ·  {geo}",
    ]
    if stats:
        lines.append(f"  {stats}")
    return "\n".join(lines)


def _fmt_proxy_detail(p: dict) -> str:
    """Multi-line detail block for a single proxy."""
    link      = pm.proxy_to_tg_link(p)
    ptype     = p["type"].upper()
    alive_ico = {True: "✅ Alive", False: "❌ Dead", None: "❓ Unchecked"}[p.get("alive")]

    lat = p.get("latency_ms")
    lat_str = f"{lat:.0f} ms" if lat is not None else "—"

    chk = p.get("check_count", 0)
    suc = p.get("success_count", 0)
    up  = f"{100*suc//chk}%" if chk > 0 else "—"

    flag = pm.country_flag(p.get("country_code"))
    country = p.get("country_name") or "—"
    country_str = f"{flag} {country}" if flag else country

    return (
        f"🔌 <b>[{ptype}]</b>  {country_str}\n"
        f"  Address  : <code>{p['server']}:{p['port']}</code>\n"
        f"  Status   : {alive_ico}\n"
        f"  Latency  : {lat_str}\n"
        f"  Uptime   : {up} ({suc}/{chk} checks)\n\n"
        f"  <code>{link}</code>"
    )


def _fmt_active(p: dict, state: dict) -> str:
    """Format a connected-proxy card for /status."""
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
    """Return (is_running, pid). Uses ps to avoid requiring psutil."""
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
        "The bot monitors your active proxy every 2 min and\n"
        "auto-rotates to a fresh one if it goes down.\n\n"
        "Type /help for the full command list."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


@require_admin
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "📋 <b>Command Reference</b>\n\n"
        "<b>Connection &amp; monitoring</b>\n"
        "  /connect [n]   — connect to proxy n, or auto-pick best\n"
        "  /disconnect    — stop monitoring, clear active proxy\n"
        "  /status        — active proxy health &amp; state\n\n"
        "<b>Proxy management</b>\n"
        "  /add &lt;proxy&gt;   — add a proxy\n"
        "  /remove &lt;id&gt;  — remove by number or host:port\n"
        "  /list [page]   — list proxies (paginated, 10 per page)\n"
        "  /top           — top 10 fastest &amp; most reliable\n"
        "  /check         — test all proxies now\n"
        "  /clean         — remove all dead proxies\n"
        "  /purge         — remove stale proxies (dead &gt; 7 days)\n"
        "  /export        — download proxy pool as JSON\n"
        "  /import        — send a .json or .txt file to import\n\n"
        "<b>Channel sources</b>\n"
        "  /addchannel &lt;@ch&gt;    — add auto-fetch channel\n"
        "  /removechannel &lt;@ch&gt; — remove channel\n"
        "  /channels              — list channels\n\n"
        "<b>URL sources</b>\n"
        "  /addsource &lt;url&gt;    — add external URL source\n"
        "  /removesource &lt;url&gt; — remove URL source\n"
        "  /sources               — list URL sources\n\n"
        "<b>Fetch</b>\n"
        "  /fetch   — fetch from all channels + URL sources\n\n"
        "<b>Settings</b>\n"
        "  /settings           — show current config\n"
        "  /setinterval &lt;min&gt; — full-check interval\n"
        "  /setfetch &lt;min&gt;    — fetch interval\n"
        "  /setmonitor &lt;min&gt;  — active-proxy monitor interval\n"
        "  /reload             — hot-reload config without restart\n\n"
        "<b>System</b>\n"
        "  /daemon  — show daemon process status\n"
        "  /stats   — pool statistics (latency, uptime, type breakdown)"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# /connect
# ---------------------------------------------------------------------------

@require_admin
async def cmd_connect(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg     = pm.load_config()
    proxies = pm.load_proxies()
    pm.backfill_fields(proxies)

    if not proxies:
        await update.message.reply_text(
            "📭 No proxies in the list.\n"
            "Use /fetch to pull from channels, or /add to add one manually."
        )
        return

    # Resolve which proxy to connect to
    target = None
    if ctx.args and ctx.args[0].isdigit():
        idx = int(ctx.args[0]) - 1
        if 0 <= idx < len(proxies):
            target = proxies[idx]
        else:
            await update.message.reply_text(
                f"❌ No proxy #{ctx.args[0]}. Use /list to see numbers."
            )
            return
    else:
        target = conn.pick_best(proxies)
        if not target:
            await update.message.reply_text(
                "❌ No live or unchecked proxies available.\n"
                "Run /check to test your list, or /fetch to pull new ones."
            )
            return

    # Always do a live TCP check before connecting — cached alive status may be stale
    timeout = cfg.get("check_timeout_seconds", 8)
    msg = await update.message.reply_text(
        f"🔍 Verifying {target['server']}:{target['port']} before connecting…"
    )
    alive = await checker.check_one(target, timeout=timeout)
    pm.save_proxies(proxies)

    if not alive:
        # Try up to 5 next-best alive proxies before giving up
        tried = {pm.proxy_key(target)}
        found = False
        candidates = [p for p in pm.sort_by_score(proxies)
                      if p.get("alive") is not False and pm.proxy_key(p) not in tried]
        for candidate in candidates[:5]:
            await msg.edit_text(
                f"❌ Dead. Trying {candidate['server']}:{candidate['port']}…"
            )
            alive = await checker.check_one(candidate, timeout=timeout)
            pm.save_proxies(proxies)
            tried.add(pm.proxy_key(candidate))
            if alive:
                target = candidate
                found = True
                break
        if not found:
            await msg.edit_text(
                "❌ All tried proxies are unreachable.\n"
                "Run /fetch then /check to refresh the pool."
            )
            return

    await msg.delete()

    conn.set_active(target)
    interval = cfg.get("monitor_interval_minutes", 2)
    body = (
        f"{_proxy_summary(target)}\n\n"
        f"  🔁 Auto-monitor every {interval} min"
    )
    text = _card("✅", "PROXY CONNECTED", body)
    deeplink = pm.proxy_to_tg_deeplink(target)
    keyboard = None
    if deeplink:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Apply Proxy in Telegram", url=deeplink)
        ]])
    await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                    reply_markup=keyboard)


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
    state  = conn.get_state()
    active = state.get("active_proxy")
    proxies = pm.load_proxies()

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
    await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                    reply_markup=keyboard)


# ---------------------------------------------------------------------------
# /top — top 10 proxies by latency + reliability score
# ---------------------------------------------------------------------------

@require_admin
async def cmd_top(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = pm.load_proxies()
    pm.backfill_fields(proxies)

    alive = [p for p in proxies if p.get("alive") is True]
    if not alive:
        await update.message.reply_text(
            "📭 No alive proxies to rank. Run /check first."
        )
        return

    ranked = pm.sort_by_score(alive)[:10]
    active = conn.get_active()

    lines = ["🏆 <b>Top 10 Proxies</b> (latency + uptime score)\n"]
    for i, p in enumerate(ranked, 1):
        flag    = pm.country_flag(p.get("country_code"))
        lat     = p.get("latency_ms")
        lat_s   = f"{lat:.0f}ms" if lat is not None else "?"
        chk     = p.get("check_count", 0)
        suc     = p.get("success_count", 0)
        up_s    = f"{100*suc//chk}%" if chk > 0 else "?"
        marker  = " 🔌" if (active and pm.proxy_key(p) == pm.proxy_key(active)) else ""
        lines.append(
            f"{i}. {flag} <code>{p['server']}:{p['port']}</code>"
            f"  <b>{lat_s}</b>  {up_s} uptime{marker}"
        )

    lines.append("\nTap a button below to apply a proxy directly in Telegram:")

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
        await update.message.reply_text(
            "❌ Could not parse the proxy. Check the format and try again."
        )
        return

    cfg     = pm.load_config()
    proxies = pm.load_proxies()

    if len(proxies) >= cfg.get("max_proxies", 1000):
        await update.message.reply_text(
            f"⚠️ Maximum proxy limit ({cfg['max_proxies']}) reached."
        )
        return

    if not pm.add_proxy(proxies, proxy):
        await update.message.reply_text("ℹ️ That proxy is already in the list.")
        return

    pm.save_proxies(proxies)
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
    proxies    = pm.load_proxies()
    removed    = pm.remove_proxy(proxies, identifier)
    if removed is None:
        await update.message.reply_text("❌ Proxy not found.")
        return

    pm.save_proxies(proxies)

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
# /list  (paginated with inline buttons)
# ---------------------------------------------------------------------------

def _build_list_page(proxies: list, page: int, active) -> tuple[str, InlineKeyboardMarkup]:
    """Build text and inline keyboard for page `page` of /list."""
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

    # One connect button per proxy (tg:// deep link opens proxy dialog in Telegram)
    rows = []
    for j, p in enumerate(chunk):
        link = pm.proxy_to_tg_deeplink(p)
        if link:
            label = f"🔗 #{start+j+1} — {p['server']}:{p['port']}"
            rows.append([InlineKeyboardButton(label, url=link)])

    # Pagination row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"list_page:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="list_page:noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶ Next", callback_data=f"list_page:{page+1}"))
    rows.append(nav)

    keyboard = InlineKeyboardMarkup(rows)
    return text, keyboard


@require_admin
async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = pm.load_proxies()
    pm.backfill_fields(proxies)

    if not proxies:
        await update.message.reply_text(
            "📭 No proxies in the list. Use /add to add one."
        )
        return

    page   = 0
    if ctx.args and ctx.args[0].isdigit():
        page = max(0, int(ctx.args[0]) - 1)

    active        = conn.get_active()
    text, keyboard = _build_list_page(proxies, page, active)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                    reply_markup=keyboard)


async def cb_list_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle inline pagination button presses for /list."""
    query = update.callback_query
    await query.answer()

    data = query.data  # "list_page:N"
    if data == "list_page:noop":
        return

    cfg = pm.load_config()
    if not is_admin(query.from_user.id, cfg):
        return

    page    = int(data.split(":")[1])
    proxies = pm.load_proxies()
    pm.backfill_fields(proxies)
    active  = conn.get_active()

    text, keyboard = _build_list_page(proxies, page, active)
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                      reply_markup=keyboard)
    except Exception:
        pass   # message unchanged — Telegram raises if content is identical


# ---------------------------------------------------------------------------
# /check
# ---------------------------------------------------------------------------

@require_admin
async def cmd_check(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = pm.load_proxies()
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

    summary = await checker.check_all(proxies, timeout=timeout,
                                      progress_callback=progress)
    pm.save_proxies(proxies)

    removed = 0
    if cfg.get("auto_remove_blocked", True):
        removed = pm.remove_blocked(proxies)
        if removed:
            pm.save_proxies(proxies)

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

    # Trigger background geo-enrichment for newly alive proxies
    asyncio.create_task(_enrich_and_save(proxies))

    await msg.edit_text(text, parse_mode=ParseMode.HTML)


async def _enrich_and_save(proxies: list):
    """Background task: geolocate proxies missing country info, then save."""
    try:
        enriched = await geoip.enrich_proxies(proxies, max_lookups=50)
        if enriched:
            pm.save_proxies(proxies)
            logger.info("Geo-enriched %d proxies", enriched)
    except Exception as exc:
        logger.debug("geo enrichment error: %s", exc)


async def _background_emergency_fetch(bot, cfg: dict, proxies: list):
    """
    Emergency fetch triggered when the alive pool drops below the configured
    threshold.  Fetches, checks new proxies, and notifies admins of the result.
    """
    channels = cfg.get("source_channels", [])
    urls     = cfg.get("source_urls", [])
    if not channels and not urls:
        return

    try:
        found   = await fetcher.fetch_all(channels, urls)
        max_p   = cfg.get("max_proxies", 1000)
        added   = 0
        for p in found:
            if len(proxies) >= max_p:
                break
            if pm.add_proxy(proxies, p):
                added += 1

        if added:
            pm.save_proxies(proxies)
            timeout  = cfg.get("check_timeout_seconds", 8)
            new_ones = [p for p in proxies if p.get("alive") is None]
            if new_ones:
                summary = await checker.check_all(new_ones, timeout=timeout)
                pm.save_proxies(proxies)
                new_alive = summary["alive"]
            else:
                new_alive = 0

            threshold = cfg.get("low_pool_threshold", 15)
            body = (
                f"  Fetched    :  {len(found)} found, {added} new\n"
                f"  ✅ Checked  :  {new_alive} alive\n"
                f"  Trigger    :  alive pool was below {threshold}"
            )
            await _broadcast(bot, cfg, _card("⚠️", "LOW POOL — AUTO-FETCH", body))
            logger.info("emergency fetch: added %d, %d now alive", added, new_alive)
    except Exception as exc:
        logger.warning("emergency fetch error: %s", exc)


# ---------------------------------------------------------------------------
# /clean  /purge
# ---------------------------------------------------------------------------

@require_admin
async def cmd_clean(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    proxies = pm.load_proxies()
    removed = pm.remove_blocked(proxies)
    pm.save_proxies(proxies)
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
    proxies = pm.load_proxies()
    removed = pm.purge_stale(proxies, max_dead_days=days)
    pm.save_proxies(proxies)
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
    proxies = pm.load_proxies()
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
    """Tell user how to import (send a file)."""
    await update.message.reply_text(
        "📥 <b>Import proxies</b>\n\n"
        "Send a <code>.json</code> or <code>.txt</code> file directly to this chat.\n\n"
        "Accepted formats:\n"
        "  • JSON array of proxy dicts (from /export)\n"
        "  • JSON array of tg://proxy or https://t.me/proxy links\n"
        "  • Plain text — one proxy per line (tg links, socks5://, IP:PORT:secret)",
        parse_mode=ParseMode.HTML,
    )


async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle file uploads for /import."""
    cfg = pm.load_config()
    if not is_admin(update.effective_user.id, cfg):
        return

    doc = update.message.document
    if not doc:
        return

    fname = (doc.file_name or "").lower()
    if not (fname.endswith(".json") or fname.endswith(".txt")):
        await update.message.reply_text(
            "⚠️ Please send a .json or .txt file."
        )
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

    # Try JSON first
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
    except json.JSONDecodeError:
        # Plain text: one proxy per line
        for line in text.splitlines():
            line = line.strip()
            if line:
                p = pm.parse_proxy(line)
                if p:
                    imported.append(p)

    if not imported:
        await msg.edit_text("❌ No parseable proxies found in the file.")
        return

    proxies = pm.load_proxies()
    max_p   = cfg.get("max_proxies", 1000)
    added   = 0
    for p in imported:
        if len(proxies) >= max_p:
            break
        if pm.add_proxy(proxies, p):
            added += 1

    pm.save_proxies(proxies)
    await msg.edit_text(
        f"✅ Import complete!\n\n"
        f"Parsed : {len(imported)}\n"
        f"Added  : {added}\n"
        f"Total  : {len(proxies)}"
    )


# ---------------------------------------------------------------------------
# Channel management
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
    if not channels:
        await update.message.reply_text(
            "No source channels configured.\nUse /addchannel @ChannelName to add one."
        )
        return
    lines = ["📡 <b>Source Channels</b>\n"]
    for i, ch in enumerate(channels, 1):
        lines.append(f"  {i}. @{ch}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# URL source management
# ---------------------------------------------------------------------------

@require_admin
async def cmd_addsource(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: /addsource &lt;url&gt;\n\n"
            "Example:\n"
            "  /addsource https://raw.githubusercontent.com/user/repo/main/socks5.txt",
            parse_mode=ParseMode.HTML,
        )
        return
    url  = ctx.args[0].strip()
    if not url.startswith(("http://", "https://")):
        await update.message.reply_text("❌ URL must start with http:// or https://")
        return
    cfg   = pm.load_config()
    urls  = cfg.setdefault("source_urls", [])
    if url in urls:
        await update.message.reply_text("ℹ️ That URL is already in the source list.")
        return
    urls.append(url)
    pm.save_config(cfg)
    await update.message.reply_text(f"✅ Added URL source:\n<code>{url}</code>",
                                    parse_mode=ParseMode.HTML)


@require_admin
async def cmd_removesource(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /removesource &lt;url&gt;",
                                        parse_mode=ParseMode.HTML)
        return
    url   = ctx.args[0].strip()
    cfg   = pm.load_config()
    urls  = cfg.get("source_urls", [])
    if url not in urls:
        await update.message.reply_text("❌ URL not found in sources.")
        return
    urls.remove(url)
    pm.save_config(cfg)
    await update.message.reply_text(f"🗑 Removed URL source:\n<code>{url}</code>",
                                    parse_mode=ParseMode.HTML)


@require_admin
async def cmd_sources(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg  = pm.load_config()
    urls = cfg.get("source_urls", [])
    if not urls:
        await update.message.reply_text(
            "No URL sources configured.\nUse /addsource &lt;url&gt; to add one.",
            parse_mode=ParseMode.HTML,
        )
        return
    lines = ["🌐 <b>URL Sources</b>\n"]
    for i, u in enumerate(urls, 1):
        lines.append(f"  {i}. <code>{u}</code>")
    await reply(update, "\n".join(lines))


# ---------------------------------------------------------------------------
# /fetch
# ---------------------------------------------------------------------------

@require_admin
async def cmd_fetch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg      = pm.load_config()
    channels = cfg.get("source_channels", [])
    urls     = cfg.get("source_urls", [])

    if not channels and not urls:
        await update.message.reply_text(
            "No sources set.\n"
            "Use /addchannel @ChannelName or /addsource <url> first."
        )
        return

    parts = []
    if channels:
        parts.append(f"{len(channels)} channel(s)")
    if urls:
        parts.append(f"{len(urls)} URL source(s)")
    msg = await update.message.reply_text(f"📡 Fetching from {', '.join(parts)}…")

    found   = await fetcher.fetch_all(channels, urls)
    proxies = pm.load_proxies()
    pm.backfill_fields(proxies)
    max_p   = cfg.get("max_proxies", 1000)
    added   = 0
    for p in found:
        if len(proxies) >= max_p:
            break
        if pm.add_proxy(proxies, p):
            added += 1
    pm.save_proxies(proxies)

    await msg.edit_text(
        f"📥 Fetch complete!\n\n"
        f"Found  : {len(found)}\n"
        f"New    : {added}\n"
        f"Total  : {len(proxies)}"
    )


# ---------------------------------------------------------------------------
# /settings  /setinterval  /setfetch  /setmonitor  /reload
# ---------------------------------------------------------------------------

@require_admin
async def cmd_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg   = pm.load_config()
    state = conn.get_state()
    active = state.get("active_proxy")
    urls   = cfg.get("source_urls", [])
    text = (
        "⚙️ <b>Current Settings</b>\n\n"
        f"Full-check interval   : {cfg.get('check_interval_minutes', 30)} min\n"
        f"Fetch interval        : {cfg.get('auto_fetch_interval_minutes', 60)} min\n"
        f"Monitor interval      : {cfg.get('monitor_interval_minutes', 2)} min\n"
        f"Daily digest (UTC)    : {cfg.get('daily_digest_hour', 9)}:00\n"
        f"Check timeout         : {cfg.get('check_timeout_seconds', 8)} s\n"
        f"Max proxies           : {cfg.get('max_proxies', 1000)}\n"
        f"Stale purge threshold : {cfg.get('stale_days', 7)} days\n"
        f"Auto-remove dead      : {cfg.get('auto_remove_blocked', True)}\n"
        f"Admin IDs             : {cfg.get('admin_ids') or 'all users'}\n"
        f"Source channels       : {len(cfg.get('source_channels', []))}\n"
        f"Source URLs           : {len(urls)}\n"
        f"Active proxy          : {'yes 🔌' if active else 'none'}\n"
        f"Monitoring            : {'ON ✅' if state.get('monitoring') else 'OFF ❌'}\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


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
async def cmd_reload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Hot-reload config.json and reschedule all jobs at new intervals."""
    cfg = pm.load_config()
    _reschedule_check(ctx.application,   cfg.get("check_interval_minutes",      30))
    _reschedule_fetch(ctx.application,   cfg.get("auto_fetch_interval_minutes", 60))
    _reschedule_monitor(ctx.application, cfg.get("monitor_interval_minutes",     2))
    _reschedule_digest(ctx.application,  cfg.get("daily_digest_hour",            9))
    await update.message.reply_text(
        "✅ Config reloaded.\n\n"
        f"Check    : {cfg.get('check_interval_minutes', 30)} min\n"
        f"Fetch    : {cfg.get('auto_fetch_interval_minutes', 60)} min\n"
        f"Monitor  : {cfg.get('monitor_interval_minutes', 2)} min\n"
        f"Digest   : daily at {cfg.get('daily_digest_hour', 9)}:00 UTC"
    )


# ---------------------------------------------------------------------------
# /daemon
# ---------------------------------------------------------------------------

@require_admin
async def cmd_daemon(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    running, pid = _daemon_running()
    state        = conn.get_state()
    daemon_proxy = state.get("active_proxy")

    if running:
        status_line = f"🟢 Running  (PID {pid})"
    else:
        status_line = "🔴 Not running"

    if daemon_proxy:
        lat = daemon_proxy.get("latency_ms")
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
    """Show detailed pool statistics: type breakdown, latency, uptime."""
    proxies = pm.load_proxies()
    total   = len(proxies)
    alive   = [p for p in proxies if p.get("alive") is True]
    dead    = [p for p in proxies if p.get("alive") is False]
    unk     = [p for p in proxies if p.get("alive") is None]

    # Type breakdown across entire pool
    types: dict[str, int] = {}
    for p in proxies:
        t = p.get("type", "unknown")
        types[t] = types.get(t, 0) + 1

    # Latency stats (alive proxies only)
    lats = [p["latency_ms"] for p in alive if p.get("latency_ms") is not None]
    if lats:
        avg_lat = sum(lats) / len(lats)
        min_lat = min(lats)
        max_lat = max(lats)
        lat_str = f"{avg_lat:.0f} ms avg  ({min_lat:.0f}–{max_lat:.0f} ms range)"
    else:
        lat_str = "N/A"

    # Average uptime across checked proxies
    uptimes = []
    for p in proxies:
        chk = p.get("check_count", 0)
        suc = p.get("success_count", 0)
        if chk > 0:
            uptimes.append(suc / chk)
    avg_uptime = (sum(uptimes) / len(uptimes) * 100) if uptimes else 0.0

    # Rotation stats
    state     = conn.get_state()
    rotations = state.get("rotations", 0)
    last_rot  = state.get("last_rotated") or "never"

    # Type rows
    type_lines = "\n".join(
        f"    {t.upper():<10}  {c}" for t, c in sorted(types.items())
    )

    body = (
        f"  Total      :  {total}\n"
        f"  ✅ Alive    :  {len(alive)}\n"
        f"  ❌ Dead     :  {len(dead)}\n"
        f"  ❓ Unchecked:  {len(unk)}\n\n"
        f"  Latency    :  {lat_str}\n"
        f"  Avg Uptime :  {avg_uptime:.1f}%\n"
        f"  Rotations  :  {rotations}  (last: {last_rot})\n\n"
        f"  Types:\n{type_lines}"
    )
    await reply(update, _card("📊", "POOL STATISTICS", body))


# ---------------------------------------------------------------------------
# Scheduled jobs
# ---------------------------------------------------------------------------

_CHECK_JOB   = "auto_check"
_FETCH_JOB   = "auto_fetch"
_MONITOR_JOB = "monitor_active"
_DIGEST_JOB  = "daily_digest"


def _reschedule_check(app, minutes: int):
    jq = app.job_queue
    for job in jq.get_jobs_by_name(_CHECK_JOB):
        job.schedule_removal()
    jq.run_repeating(_job_check, interval=minutes * 60,
                     first=minutes * 60, name=_CHECK_JOB)


def _reschedule_fetch(app, minutes: int):
    jq = app.job_queue
    for job in jq.get_jobs_by_name(_FETCH_JOB):
        job.schedule_removal()
    jq.run_repeating(_job_fetch, interval=minutes * 60,
                     first=minutes * 60, name=_FETCH_JOB)


def _reschedule_monitor(app, minutes: int):
    jq = app.job_queue
    for job in jq.get_jobs_by_name(_MONITOR_JOB):
        job.schedule_removal()
    jq.run_repeating(_job_monitor_active, interval=minutes * 60,
                     first=30, name=_MONITOR_JOB)


def _reschedule_digest(app, hour_utc: int):
    jq = app.job_queue
    for job in jq.get_jobs_by_name(_DIGEST_JOB):
        job.schedule_removal()
    # Run daily at the configured UTC hour
    import datetime as dt_mod
    jq.run_daily(_job_daily_digest,
                 time=dt_mod.time(hour=hour_utc % 24, minute=0,
                                  tzinfo=timezone.utc),
                 name=_DIGEST_JOB)


# ---------------------------------------------------------------------------
# _job_monitor_active — core auto-rotation engine
# ---------------------------------------------------------------------------

async def _job_monitor_active(context: ContextTypes.DEFAULT_TYPE):
    if not conn.is_monitoring():
        return

    cfg    = pm.load_config()
    active = conn.get_active()
    if not active:
        return

    timeout = cfg.get("check_timeout_seconds", 8)
    alive   = await checker.check_one(active, timeout=timeout)
    proxies = pm.load_proxies()
    pm.backfill_fields(proxies)
    pm.save_proxies(proxies)

    if alive:
        logger.debug("monitor: active proxy %s:%s is alive", active["server"], active["port"])

        # Latency spike detection: warn if current reading is 3× the rolling
        # median and above 500 ms — an early sign of degradation.
        history = active.get("latency_history", [])
        current_lat = active.get("latency_ms")
        if history and current_lat is not None and len(history) >= 3:
            sorted_h = sorted(history[:-1])   # exclude the just-recorded value
            mid      = len(sorted_h) // 2
            if len(sorted_h) % 2 == 0:
                baseline = (sorted_h[mid - 1] + sorted_h[mid]) / 2.0
            else:
                baseline = float(sorted_h[mid])
            if baseline > 0 and current_lat > baseline * 3 and current_lat > 500:
                body = (
                    f"{_proxy_summary(active)}\n\n"
                    f"  Current  :  {current_lat:.0f} ms\n"
                    f"  Baseline :  {baseline:.0f} ms  (3× threshold)"
                )
                await _broadcast(
                    context.bot, cfg,
                    _card("⚠️", "LATENCY SPIKE", body, "Proxy may degrade soon.")
                )
        return

    logger.info("monitor: active proxy %s:%s died — rotating",
                active["server"], active["port"])

    # Notify: proxy died card
    await _broadcast(
        context.bot, cfg,
        _card("🔴", "PROXY FAILED", _proxy_summary(active), "🔄 Searching for replacement…")
    )

    # Stamp recently_failed_at so pick_best skips this proxy for 5 minutes
    dead_key = pm.proxy_key(active)
    for p in proxies:
        if pm.proxy_key(p) == dead_key:
            p["recently_failed_at"] = datetime.now(timezone.utc).isoformat()
            break

    # Aggressively clean dead proxies before rotating
    if cfg.get("auto_remove_blocked", True):
        pm.remove_blocked(proxies)
        pm.save_proxies(proxies)

    # Re-verify top candidates with a fresh check before picking the winner
    channels  = cfg.get("source_channels", [])
    urls      = cfg.get("source_urls", [])
    candidates = [
        p for p in pm.sort_by_score(proxies)
        if p.get("alive") is True and pm.proxy_key(p) != pm.proxy_key(active)
    ][:8]
    if candidates:
        await asyncio.gather(
            *[checker.check_one(p, timeout=5) for p in candidates],
            return_exceptions=True
        )
        pm.save_proxies(proxies)

    async def notify(msg):
        pass  # suppress intermediate rotation messages — we send our own cards

    new_proxy = await conn.auto_rotate(
        proxies, channels, urls=urls,
        timeout=timeout,
        notify_fn=notify,
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
        )
    else:
        fail_body = (
            "  All proxies are exhausted.\n"
            "  Run /fetch to pull fresh ones,\n"
            "  or /add to add proxies manually."
        )
        await _broadcast(
            context.bot, cfg,
            _card("🚨", "ROTATION FAILED", fail_body)
        )


# ---------------------------------------------------------------------------
# _job_check — periodic full TCP check
# ---------------------------------------------------------------------------

async def _job_check(context: ContextTypes.DEFAULT_TYPE):
    cfg     = pm.load_config()
    proxies = pm.load_proxies()
    pm.backfill_fields(proxies)

    if not proxies:
        return

    timeout = cfg.get("check_timeout_seconds", 8)
    summary = await checker.check_all(proxies, timeout=timeout)
    pm.save_proxies(proxies)

    removed = 0
    if cfg.get("auto_remove_blocked", True):
        removed = pm.remove_blocked(proxies)
        if removed:
            pm.save_proxies(proxies)

    # Purge stale proxies
    stale = pm.purge_stale(proxies, cfg.get("stale_days", 7))
    if stale:
        pm.save_proxies(proxies)

    # Background geo-enrichment
    asyncio.create_task(_enrich_and_save(proxies))

    # Low pool alarm: if alive proxies drop below threshold, emergency-fetch
    alive_now = sum(1 for p in proxies if p.get("alive") is True)
    threshold = cfg.get("low_pool_threshold", 15)
    if alive_now < threshold:
        logger.info("auto-check: alive pool (%d) below threshold (%d) — emergency fetch",
                    alive_now, threshold)
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
# _job_fetch — periodic channel + URL scrape
# ---------------------------------------------------------------------------

async def _job_fetch(context: ContextTypes.DEFAULT_TYPE):
    cfg      = pm.load_config()
    channels = cfg.get("source_channels", [])
    urls     = cfg.get("source_urls", [])

    if not channels and not urls:
        return

    found   = await fetcher.fetch_all(channels, urls)
    proxies = pm.load_proxies()
    pm.backfill_fields(proxies)
    max_p   = cfg.get("max_proxies", 1000)
    added   = 0
    for p in found:
        if len(proxies) >= max_p:
            break
        if pm.add_proxy(proxies, p):
            added += 1
    if added:
        pm.save_proxies(proxies)

    logger.info("auto-fetch: found %d added %d", len(found), added)
    if added:
        # Immediately check the new unchecked proxies so they're ready to use
        timeout  = cfg.get("check_timeout_seconds", 8)
        new_ones = [p for p in proxies if p.get("alive") is None]
        new_alive = 0
        if new_ones:
            summary = await checker.check_all(new_ones, timeout=timeout)
            pm.save_proxies(proxies)
            new_alive = summary["alive"]

        body = (
            f"  Sources    :  {len(channels)} ch  +  {len(urls)} URLs\n"
            f"  Found      :  {len(found)}\n"
            f"  New added  :  {added}\n"
            f"  ✅ Alive    :  {new_alive}  (checked immediately)\n"
            f"  📦 Total   :  {len(proxies)}"
        )
        await _broadcast(context.bot, cfg, _card("📥", "AUTO-FETCH COMPLETE", body))


# ---------------------------------------------------------------------------
# _job_daily_digest — daily stats broadcast
# ---------------------------------------------------------------------------

async def _job_daily_digest(context: ContextTypes.DEFAULT_TYPE):
    cfg     = pm.load_config()
    proxies = pm.load_proxies()

    total  = len(proxies)
    alive  = sum(1 for p in proxies if p.get("alive") is True)
    dead   = sum(1 for p in proxies if p.get("alive") is False)

    # Best proxy by score
    alive_list = [p for p in proxies if p.get("alive") is True]
    best_line  = ""
    if alive_list:
        best = pm.sort_by_score(alive_list)[0]
        lat  = best.get("latency_ms")
        lat_s = f"{lat:.0f}ms" if lat is not None else "?"
        flag  = pm.country_flag(best.get("country_code"))
        best_line = (
            f"\nBest proxy  : {flag} <code>{best['server']}:{best['port']}</code>"
            f"  {lat_s}"
        )

    # Daemon rotation count
    state     = conn.get_state()
    rotations = state.get("rotations", 0)

    # Daemon status
    running, _ = _daemon_running()
    daemon_ico = "🟢" if running else "🔴"

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    text = (
        f"📊 <b>Daily Summary — {date_str}</b>\n\n"
        f"Pool        : {total} total  (✅{alive}  ❌{dead})\n"
        f"Daemon      : {daemon_ico}  rotations: {rotations} total\n"
        + best_line
    )
    await _broadcast(context.bot, cfg, text)


# ---------------------------------------------------------------------------
# Inline proxy paste handler
# ---------------------------------------------------------------------------

@require_admin
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text  = update.message.text or ""
    proxy = pm.parse_proxy(text)
    if not proxy:
        return

    cfg     = pm.load_config()
    proxies = pm.load_proxies()

    if len(proxies) >= cfg.get("max_proxies", 1000):
        await update.message.reply_text("⚠️ Proxy list is full. Remove some first.")
        return

    if not pm.add_proxy(proxies, proxy):
        await update.message.reply_text("ℹ️ That proxy is already in the list.")
        return

    pm.save_proxies(proxies)
    link = pm.proxy_to_tg_link(proxy)
    await update.message.reply_text(
        f"✅ Proxy detected and added (#{len(proxies)}):\n<code>{link}</code>",
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# Startup notification
# ---------------------------------------------------------------------------

async def _post_init(app: Application):
    """Send an online card to all admins when the bot starts up."""
    cfg     = pm.load_config()
    proxies = pm.load_proxies()
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    cfg = pm.load_config()
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
    app.add_handler(CommandHandler("add",           cmd_add))
    app.add_handler(CommandHandler("remove",        cmd_remove))
    app.add_handler(CommandHandler("list",          cmd_list))
    app.add_handler(CommandHandler("check",         cmd_check))
    app.add_handler(CommandHandler("clean",         cmd_clean))
    app.add_handler(CommandHandler("purge",         cmd_purge))
    app.add_handler(CommandHandler("export",        cmd_export))
    app.add_handler(CommandHandler("import",        cmd_import_help))
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
    app.add_handler(CommandHandler("reload",        cmd_reload))
    app.add_handler(CommandHandler("daemon",        cmd_daemon))
    app.add_handler(CommandHandler("stats",         cmd_stats))

    # Inline keyboard callbacks
    app.add_handler(CallbackQueryHandler(cb_list_page, pattern=r"^list_page:"))

    # File upload for /import
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

    logger.info(
        "Bot started. Check=%dmin Fetch=%dmin Monitor=%dmin Digest=%d:00UTC",
        check_min, fetch_min, monitor_min, digest_hour,
    )

    # Restore monitoring state on restart
    if conn.is_monitoring() and conn.get_active():
        active = conn.get_active()
        logger.info(
            "Restored active proxy from previous session: %s:%s",
            active["server"], active["port"],
        )

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

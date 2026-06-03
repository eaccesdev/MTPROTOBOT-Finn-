"""
crawler.py — Discover, join, and scrape Telegram proxy channels.

Four discovery strategies
--------------------------
1. **Directory scraping** (no Telegram account required)
   Queries public Telegram channel index sites (tgstat, telemetr, lyzem, tchannels)
   and Telegram's own t.me/s/ public preview pages.

2. **Telethon join & fetch** (requires daemon session)
   Joins discovered public channels/groups via Telethon, reads their full
   message history, and extracts proxies of all types (MTProto, SOCKS5, HTTP).
   Handles join errors gracefully (already joined, private, flood-wait).

3. **Similar channel discovery**
   After joining each channel, queries Telegram's GetChannelRecommendations API
   to find related channels and crawls those too (one additional hop).

4. **Telethon global search**
   Searches the full Telegram index (including non-public groups) with
   SearchGlobalRequest and pulls proxy links directly from matching messages.

Public API
----------
    crawl(keywords, use_telethon, limit_channels, pages_per_channel,
          api_id, api_hash, session_file, progress_callback)
        Full pipeline: discover → join → fetch → deduplicate.

    search_directories(keywords) -> list[channel_username]
        Web-directory discovery only (no Telegram account needed).

    telethon_join_and_fetch(usernames, api_id, api_hash, session_file,
                            messages_per_channel, discover_similar,
                            progress_callback)
        Join channels, read messages, discover similar channels.
        Returns (joined_channel_names, proxy_dicts).

    telethon_search(keywords, api_id, api_hash, session_file)
        Global Telegram search; returns parsed proxy dicts.
"""

from __future__ import annotations

import asyncio
import html as html_mod
import json
import logging
import os
import re
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import aiohttp as _aiohttp
    _HAS_AIOHTTP = True
except ImportError:
    _HAS_AIOHTTP = False

# ---------------------------------------------------------------------------
# Default search keywords
# ---------------------------------------------------------------------------

DEFAULT_KEYWORDS = [
    "mtproto proxy",
    "socks5 proxy telegram",
    "free mtproto",
    "telegram proxy free",
    "proxy mtproto",
]

# ---------------------------------------------------------------------------
# Known-good seed channels to bootstrap discovery
# ---------------------------------------------------------------------------

_SEED_CHANNELS = [
    "ProxyMTProto", "MTProxies", "mtproxy_ir", "proxy_mtproto",
    "lol_mtproto", "socks5proxies", "freesocks5proxies",
    "MTProto_Proxy", "proxyforTelegram", "proxy_configs",
    "v2ray_free", "xray_proxy", "v2rayfree", "freev2ray",
    "free_v2ray", "freeipv6", "freeip",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Domains/names that should never be treated as proxy servers or channel names
_FAKE_CHANNELS = {
    "telegram", "telegramdesktop", "t", "me", "tg",
    "google", "github", "youtube", "twitter", "facebook",
    "instagram", "tiktok", "vk", "ok",
}
_FAKE_SERVERS = {
    "t.me", "telegram.me", "telegram.org", "telegra.ph",
    "telesco.pe", "google.com", "youtube.com", "www.google.com",
    "github.com", "raw.githubusercontent.com",
}

# Regex to extract @username / t.me/username references from HTML
_RE_USERNAME = re.compile(
    r'(?:@|t\.me/|tg://resolve\?domain=)([\w]{4,32})',
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Regexes for raw (non-HTML) message text
# ---------------------------------------------------------------------------

# Proxy URL patterns in plain text
_RE_PROXY_RAW = re.compile(
    r'(?:https?://t\.me/proxy\?|tg://proxy\?|socks5://|socks4://)[^\s"\'<>]+',
    re.IGNORECASE,
)

# Plain server:port:secret MTProto triple
_RE_PLAIN_MTPROTO = re.compile(
    r'\b([\w.\-]+):(\d{2,5}):([0-9a-fA-F]{32,})\b',
)

# Plain IP:PORT (for bare SOCKS lists)
_RE_IP_PORT_RAW = re.compile(
    r'\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b',
)


# ---------------------------------------------------------------------------
# Raw text proxy extractor (for Telethon message.text — not HTML)
# ---------------------------------------------------------------------------

def _extract_from_text(text: str) -> list[dict]:
    """
    Extract all recognisable proxy types from a raw (non-HTML) message string.

    Handles: tg://proxy, t.me/proxy, socks5://, socks4://, server:port:secret.
    """
    from proxy_manager import parse_proxy, _make_proxy
    found: list[dict] = []
    seen:  set        = set()

    def _add(p):
        if p is None:
            return
        srv = p.get("server", "").lower()
        if srv in _FAKE_SERVERS or srv.endswith(".telegram.org"):
            return
        key = (srv, p["port"])
        if key not in seen:
            seen.add(key)
            found.append(p)

    # 1. URL-style proxy links (tg://proxy, t.me/proxy, socks5://, socks4://)
    for m in _RE_PROXY_RAW.finditer(text):
        raw = html_mod.unescape(m.group(0).rstrip(".,;)>"))
        _add(parse_proxy(raw))

    # 2. Plain server:port:secret MTProto triples
    for m in _RE_PLAIN_MTPROTO.finditer(text):
        server, port_s, secret = m.group(1), m.group(2), m.group(3)
        try:
            port = int(port_s)
            if 1 <= port <= 65535 and server.lower() not in _FAKE_SERVERS:
                _add(_make_proxy(server, port, "mtproto", secret=secret))
        except ValueError:
            pass

    return found


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

async def _get(session, url: str, timeout: float = 15.0) -> str:
    """Fetch URL and return HTML text (empty string on failure)."""
    try:
        async with session.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"},
            timeout=_aiohttp.ClientTimeout(total=timeout),
            ssl=False,
            allow_redirects=True,
        ) as resp:
            if resp.status == 200:
                return await resp.text(errors="replace")
            logger.debug("GET %s → HTTP %s", url, resp.status)
    except Exception as exc:
        logger.debug("GET %s failed: %s", url, exc)
    return ""


def _extract_usernames(html: str) -> list[str]:
    """Pull Telegram channel usernames from an HTML page."""
    raw  = _RE_USERNAME.findall(html)
    seen: set[str]   = set()
    result: list[str] = []
    for name in raw:
        lname = name.lower()
        if lname not in _FAKE_CHANNELS and len(name) >= 4 and name not in seen:
            seen.add(name)
            result.append(name)
    return result


# ---------------------------------------------------------------------------
# Directory-specific scrapers
# ---------------------------------------------------------------------------

async def _search_tgstat(session, query: str) -> list[str]:
    q    = query.replace(" ", "+")
    url  = f"https://tgstat.ru/en/search?q={q}&type=channel"
    html = await _get(session, url)
    if not html:
        return []
    names  = re.findall(r'/channel/@([\w]{4,32})', html)
    names += _extract_usernames(html)
    seen: set[str]   = set()
    result: list[str] = []
    for n in names:
        if n.lower() not in _FAKE_CHANNELS and n not in seen:
            seen.add(n)
            result.append(n)
    logger.debug("tgstat '%s': %d channel(s)", query, len(result))
    return result


async def _search_telemetr(session, query: str) -> list[str]:
    q    = query.replace(" ", "+")
    url  = f"https://telemetr.io/en/channels?search={q}&sort=subscribers"
    html = await _get(session, url)
    if not html:
        return []
    names = _extract_usernames(html)
    logger.debug("telemetr '%s': %d channel(s)", query, len(names))
    return names


async def _search_lyzem(session, query: str) -> list[str]:
    q    = query.replace(" ", "+")
    url  = f"https://lyzem.com/search?q={q}&lang=en&categories="
    html = await _get(session, url)
    if not html:
        return []
    names = _extract_usernames(html)
    logger.debug("lyzem '%s': %d channel(s)", query, len(names))
    return names


async def _search_tchannels(session, query: str) -> list[str]:
    q    = query.replace(" ", "+")
    url  = f"https://tchannels.me/en/search?q={q}&category=other"
    html = await _get(session, url)
    if not html:
        return []
    names  = re.findall(r'/en/channel/([\w]{4,32})', html)
    names += _extract_usernames(html)
    seen: set[str]   = set()
    result: list[str] = []
    for n in names:
        if n.lower() not in _FAKE_CHANNELS and n not in seen:
            seen.add(n)
            result.append(n)
    logger.debug("tchannels '%s': %d channel(s)", query, len(result))
    return result


async def _search_telegram_channel_pages(session, query: str) -> list[str]:
    """Try common proxy-channel name patterns derived from the search query."""
    words = query.lower().split()
    names: list[str] = []
    for w in words:
        if len(w) < 4:
            continue
        for pattern in [w, f"{w}_proxy", f"proxy_{w}", f"free_{w}", f"{w}free"]:
            if len(pattern) >= 4:
                html = await _get(session, f"https://t.me/s/{pattern}", timeout=8.0)
                if html and "tg-page-container" in html:
                    names.append(pattern)
    return names


# ---------------------------------------------------------------------------
# Combined directory search
# ---------------------------------------------------------------------------

async def search_directories(keywords: list[str] | None = None,
                             limit_per_keyword: int = 10) -> list[str]:
    """
    Search all known Telegram channel directories for the given keywords.
    Returns a deduplicated list of channel usernames.
    Falls back to _SEED_CHANNELS if nothing discovered or aiohttp is absent.
    """
    if not _HAS_AIOHTTP:
        logger.warning("aiohttp not installed — directory search disabled")
        return list(_SEED_CHANNELS)

    if not keywords:
        keywords = DEFAULT_KEYWORDS

    discovered: list[str] = []
    seen: set[str]        = set()

    async with _aiohttp.ClientSession() as session:
        for kw in keywords:
            tasks = [
                _search_tgstat(session, kw),
                _search_telemetr(session, kw),
                _search_lyzem(session, kw),
                _search_tchannels(session, kw),
                _search_telegram_channel_pages(session, kw),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, list):
                    for name in r[:limit_per_keyword]:
                        if name not in seen and name.lower() not in _FAKE_CHANNELS:
                            seen.add(name)
                            discovered.append(name)
            await asyncio.sleep(1.0)

    for name in _SEED_CHANNELS:
        if name not in seen:
            seen.add(name)
            discovered.append(name)

    logger.info("Directory search: %d unique channel(s) discovered", len(discovered))
    return discovered


# ---------------------------------------------------------------------------
# Telethon: join channels, read messages, discover similar
# ---------------------------------------------------------------------------

async def telethon_join_and_fetch(
    usernames:            list[str],
    api_id:               int,
    api_hash:             str,
    session_file:         str,
    messages_per_channel: int  = 300,
    discover_similar:     bool = True,
    progress_callback           = None,
) -> tuple[list[str], list[dict]]:
    """
    For each username in *usernames*:
      1. Resolve the entity with Telethon.
      2. Join the channel/group (skip if already joined, private, etc.).
      3. Read up to *messages_per_channel* recent messages and extract proxies
         of all types (MTProto, SOCKS5, SOCKS4, HTTP).
      4. If *discover_similar* is True, call GetChannelRecommendations to find
         related channels and crawl those too (one additional hop).

    Returns (joined_channel_names: list[str], proxies: list[dict]).
    """
    try:
        from telethon import TelegramClient
        from telethon.tl.functions.channels import JoinChannelRequest
        from telethon.errors import (
            FloodWaitError,
            UserAlreadyParticipantError,
            ChannelPrivateError,
            ChatAdminRequiredError,
            UsernameInvalidError,
            UsernameNotOccupiedError,
        )
        from telethon.tl.types import Channel
    except ImportError:
        logger.warning("telethon not installed — join+fetch disabled")
        return [], []

    # GetChannelRecommendationsRequest was added in Telethon 1.26; optional
    try:
        from telethon.tl.functions.channels import GetChannelRecommendationsRequest
        _HAS_RECOMMENDATIONS = True
    except ImportError:
        _HAS_RECOMMENDATIONS = False
        logger.debug("GetChannelRecommendationsRequest not available in this Telethon version")

    if not api_id or not api_hash or not session_file:
        return [], []

    if session_file.endswith(".session"):
        session_file = session_file[:-8]
    if not os.path.exists(session_file + ".session"):
        logger.warning("Telethon session not found: %s.session", session_file)
        return [], []

    async def _prog(stage: str, cur: int, total: int) -> None:
        if progress_callback:
            try:
                await progress_callback(stage, cur, total)
            except Exception:
                pass

    joined_names: list[str] = []
    all_proxies:  list[dict] = []
    seen_keys:    set        = set()

    def _dedup_add(proxies: list[dict]) -> None:
        for p in proxies:
            key = (p["server"].lower(), p["port"])
            if key not in seen_keys:
                seen_keys.add(key)
                all_proxies.append(p)

    async def _fetch_entity(client, entity, limit: int) -> None:
        """Read messages from entity and add extracted proxies."""
        try:
            async for msg in client.iter_messages(entity, limit=limit):
                text = msg.text or ""
                if text:
                    _dedup_add(_extract_from_text(text))
        except Exception as exc:
            name = getattr(entity, "username", str(entity))
            logger.debug("iter_messages @%s: %s", name, exc)

    async def _try_join(client, entity) -> bool:
        """Attempt to join; return True if we can read messages."""
        try:
            await client(JoinChannelRequest(entity))
            return True
        except UserAlreadyParticipantError:
            return True
        except FloodWaitError as exc:
            wait = min(exc.seconds, 60)
            logger.warning("FloodWait joining: sleeping %ds", wait)
            await asyncio.sleep(wait)
            return True  # still try to read
        except (ChannelPrivateError, ChatAdminRequiredError):
            return False
        except Exception as exc:
            logger.debug("JoinChannelRequest: %s", exc)
            return False

    similar_queue: list[str] = []

    try:
        client = TelegramClient(session_file, api_id, api_hash, connection_retries=2)
        await client.connect()

        if not await client.is_user_authorized():
            logger.warning("Telethon session not authorized — run daemon to log in first")
            await client.disconnect()
            return [], []

        logger.info("Telethon join+fetch: %d channel(s) to process", len(usernames))
        all_seen_names = set(u.lower() for u in usernames)

        for i, username in enumerate(usernames):
            await _prog("join_fetch", i, len(usernames))
            username = username.strip().lstrip("@")
            if not username:
                continue

            try:
                entity = await client.get_entity(username)
            except (UsernameInvalidError, UsernameNotOccupiedError):
                logger.debug("@%s: username not found", username)
                continue
            except Exception as exc:
                logger.debug("get_entity @%s: %s", username, exc)
                continue

            can_read = await _try_join(client, entity)
            if not can_read:
                logger.debug("@%s: skipped (private/restricted)", username)
                continue

            joined_names.append(username)
            await _fetch_entity(client, entity, limit=messages_per_channel)
            logger.debug("@%s: %d total proxies collected so far", username, len(all_proxies))

            # Discover similar/recommended channels
            if discover_similar and _HAS_RECOMMENDATIONS and isinstance(entity, Channel):
                try:
                    result = await client(GetChannelRecommendationsRequest(channel=entity))
                    for ch in getattr(result, "chats", []):
                        uname = getattr(ch, "username", None)
                        if uname and uname.lower() not in all_seen_names:
                            all_seen_names.add(uname.lower())
                            similar_queue.append(uname)
                            logger.debug("Similar channel discovered: @%s", uname)
                except Exception as exc:
                    logger.debug("GetChannelRecommendations @%s: %s", username, exc)

            await asyncio.sleep(1.5)

        # Process similar/recommended channels (one additional hop)
        if similar_queue:
            logger.info("Processing %d similar channel(s) discovered via recommendations",
                        len(similar_queue))
            for i, username in enumerate(similar_queue):
                await _prog("similar", i, len(similar_queue))
                username = username.strip().lstrip("@")
                try:
                    entity = await client.get_entity(username)
                except Exception as exc:
                    logger.debug("get_entity similar @%s: %s", username, exc)
                    continue

                can_read = await _try_join(client, entity)
                if not can_read:
                    continue

                joined_names.append(username)
                await _fetch_entity(client, entity, limit=messages_per_channel // 2)
                await asyncio.sleep(1.5)

        await _prog("join_fetch", len(usernames), len(usernames))
        await client.disconnect()

    except Exception as exc:
        logger.warning("telethon_join_and_fetch failed: %s", exc)

    logger.info("Join+fetch complete: %d channels joined, %d proxy(ies) found",
                len(joined_names), len(all_proxies))
    return joined_names, all_proxies


# ---------------------------------------------------------------------------
# Telethon-powered global group/channel search
# ---------------------------------------------------------------------------

async def telethon_search(
    keywords:             list[str] | None = None,
    api_id:               int              = 0,
    api_hash:             str              = "",
    session_file:         str              = "",
    messages_per_keyword: int              = 200,
) -> list[dict]:
    """
    Use an existing Telethon session to search Telegram globally for proxy links.

    Searches the full Telegram index (including private groups) via
    SearchGlobalRequest and extracts proxy links from matching messages.

    Returns a list of parsed proxy dicts.
    """
    if not keywords:
        keywords = DEFAULT_KEYWORDS

    try:
        from telethon import TelegramClient
        from telethon.tl.functions.messages import SearchGlobalRequest
        from telethon.tl.types import InputMessagesFilterEmpty, InputPeerEmpty
    except ImportError:
        logger.warning("telethon not installed — Telethon search disabled")
        return []

    if not api_id or not api_hash or not session_file:
        logger.warning("Telethon credentials not provided — search skipped")
        return []

    if session_file.endswith(".session"):
        session_file = session_file[:-8]
    if not os.path.exists(session_file + ".session"):
        logger.warning("Telethon session file not found at %s.session", session_file)
        return []

    proxies:   list[dict] = []
    seen_keys: set        = set()

    try:
        client = TelegramClient(session_file, api_id, api_hash, connection_retries=1)
        await client.connect()

        if not await client.is_user_authorized():
            logger.warning("Telethon session is not authorized")
            await client.disconnect()
            return []

        logger.info("Telethon search: %d keyword(s)", len(keywords))

        for kw in keywords:
            offset_rate = 0
            offset_id   = 0
            fetched     = 0

            while fetched < messages_per_keyword:
                try:
                    result = await client(SearchGlobalRequest(
                        q=kw,
                        filter=InputMessagesFilterEmpty(),
                        min_date=None,
                        max_date=None,
                        offset_rate=offset_rate,
                        offset_peer=InputPeerEmpty(),
                        offset_id=offset_id,
                        limit=min(100, messages_per_keyword - fetched),
                    ))
                except Exception as exc:
                    logger.debug("SearchGlobal '%s' failed: %s", kw, exc)
                    break

                messages = getattr(result, "messages", [])
                if not messages:
                    break

                for msg in messages:
                    text = getattr(msg, "message", "") or ""
                    if text:
                        for p in _extract_from_text(text):
                            key = (p["server"].lower(), p["port"])
                            if key not in seen_keys:
                                seen_keys.add(key)
                                proxies.append(p)

                fetched     += len(messages)
                offset_rate  = getattr(result, "next_rate", 0)
                offset_id    = messages[-1].id

                if not offset_rate and len(messages) < 100:
                    break

                await asyncio.sleep(1.0)

            logger.info("Telethon search '%s': %d proxy(ies) so far", kw, len(proxies))
            await asyncio.sleep(2.0)

        await client.disconnect()

    except Exception as exc:
        logger.warning("Telethon search failed: %s", exc)

    logger.info("Telethon global search complete: %d unique proxy(ies)", len(proxies))
    return proxies


# ---------------------------------------------------------------------------
# Main entry point: full crawl pipeline
# ---------------------------------------------------------------------------

async def crawl(
    keywords:          list[str] | None = None,
    use_telethon:      bool             = True,
    limit_channels:    int              = 30,
    pages_per_channel: int              = 5,
    api_id:            int              = 0,
    api_hash:          str              = "",
    session_file:      str              = "",
    progress_callback                    = None,
) -> tuple[list[str], list[dict]]:
    """
    Full proxy crawl pipeline.

    Steps:
      1. Search web directories for proxy channel usernames.
      2a. If Telethon session is available: join all discovered channels,
          read their messages, discover similar channels via recommendations,
          and crawl those too.
      2b. Fallback (no session): scrape public t.me/s/ pages per channel.
      3. Telethon global keyword search (when session available).

    Returns (discovered_channels: list[str], proxies: list[dict]).
    """
    if not keywords:
        keywords = DEFAULT_KEYWORDS

    import fetcher as _fetcher

    async def _progress(stage: str, cur: int, total: int) -> None:
        if progress_callback:
            try:
                await progress_callback(stage, cur, total)
            except Exception:
                pass

    # Stage 1: Directory search
    await _progress("directories", 0, 1)
    discovered     = await search_directories(keywords)
    unique_channels = list(dict.fromkeys(discovered))[:limit_channels]
    await _progress("directories", 1, 1)
    logger.info("Crawl: discovered %d channel(s) to process", len(unique_channels))

    all_proxies: list[dict] = []
    seen_keys:   set        = set()

    def _add_proxies(proxies: list[dict]) -> None:
        for p in proxies:
            key = (p["server"].lower(), p["port"])
            if key not in seen_keys:
                seen_keys.add(key)
                all_proxies.append(p)

    # Determine if we have a usable Telethon session
    _sf = session_file
    if _sf.endswith(".session"):
        _sf = _sf[:-8]
    has_session = (
        use_telethon and api_id and api_hash and session_file
        and os.path.exists(_sf + ".session")
    )

    if has_session:
        # Stage 2a: Join channels via Telethon + read messages + discover similar
        joined, tg_proxies = await telethon_join_and_fetch(
            usernames            = unique_channels,
            api_id               = api_id,
            api_hash             = api_hash,
            session_file         = session_file,
            messages_per_channel = 300,
            discover_similar     = True,
            progress_callback    = _progress,
        )
        _add_proxies(tg_proxies)

        # Merge any extra channels discovered via recommendations into the list
        for ch in joined:
            if ch not in unique_channels:
                unique_channels.append(ch)

        # Stage 3: Telethon global search
        await _progress("telethon", 0, 1)
        global_proxies = await telethon_search(
            keywords             = keywords,
            api_id               = api_id,
            api_hash             = api_hash,
            session_file         = session_file,
        )
        _add_proxies(global_proxies)
        await _progress("telethon", 1, 1)

    else:
        # Stage 2b fallback: public t.me/s/ page scraping
        for i, ch in enumerate(unique_channels):
            await _progress("channels", i, len(unique_channels))
            try:
                found = await _fetcher.fetch_from_channel(ch, limit_pages=pages_per_channel)
                _add_proxies(found)
            except Exception as exc:
                logger.debug("Channel @%s fetch error: %s", ch, exc)
            await asyncio.sleep(0.5)
        await _progress("channels", len(unique_channels), len(unique_channels))

    logger.info("Crawl complete: %d unique proxy(ies) from %d channel(s)",
                len(all_proxies), len(unique_channels))
    return unique_channels, all_proxies

"""
fetcher.py — Scrape Telegram channels and external sources for proxy links.

Sources supported:
  1. Telegram public channel pages  (t.me/s/<channel>)
  2. Generic URLs                   (GitHub raw, plain-text lists)
  3. Geonode proxy API              (proxylist.geonode.com)

Fixes applied:
  • HTML-unescape &amp; → & before parsing tg://proxy links
  • Parse structured post format: Server/Port/Secret in <code> tags
  • Filter out t.me and telegram.org as false-positive proxy servers
  • Scrape up to limit_pages (default 10) per channel
  • Broader regex catches both tg:// and https://t.me/proxy links in href attrs
"""

import asyncio
import html as html_mod
import logging
import re
import urllib.parse

logger = logging.getLogger(__name__)

try:
    import aiohttp
    _AIOHTTP = True
except ImportError:
    _AIOHTTP = False

# ---------------------------------------------------------------------------
# Default Telegram channel list
# ---------------------------------------------------------------------------

DEFAULT_CHANNELS = [
    "ProxyMTProto",
    "MTProxies",
    "mtproxy_ir",
    "proxy_mtproto",
    "lol_mtproto",
    "socks5proxies",
    "freesocks5proxies",
]

# Default external URL sources (GitHub raw lists, APIs)
DEFAULT_SOURCE_URLS = [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
]

# ---------------------------------------------------------------------------
# Domains that must never be treated as proxy servers
# ---------------------------------------------------------------------------

_FAKE_SERVERS = {
    "t.me", "telegram.me", "telegram.org", "telegra.ph",
    "telesco.pe", "google.com", "youtube.com", "www.google.com",
    "github.com", "raw.githubusercontent.com",
}

# Ports that strongly suggest an HTTP/SOCKS proxy (not a web page)
_PROXY_PORTS = {80, 443, 1080, 3128, 8080, 8443, 9050, 9150, 4145, 1088}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Regexes
# ---------------------------------------------------------------------------

# Match tg:// or https://t.me/proxy links inside href="..." attributes
_RE_TG_HREF = re.compile(
    r'href=["\']'
    r'((?:tg://proxy|https?://t\.me/proxy)\?[^"\'<>\s]+)',
    re.IGNORECASE,
)

# Same links free-standing in post text (unencoded form)
_RE_TG_TEXT = re.compile(
    r'(?:https?://t\.me/proxy\?|tg://proxy\?)'
    r'[^\s"\'<>]+',
    re.IGNORECASE,
)

# SOCKS5/4 URI
_RE_SOCKS = re.compile(
    r'socks[45]://[^\s"\'<>]+',
    re.IGNORECASE,
)

# HTTP proxy URI (port-selective to avoid false positives)
_RE_HTTP = re.compile(
    r'https?://[^\s"\'<>]+',
    re.IGNORECASE,
)

# Structured post format:
#   Server: <code>HOST</code>
#   Port:   <code>PORT</code>
#   Secret: <code>SECRET</code>
_RE_STRUCTURED = re.compile(
    r'[Ss]erver\s*:?\s*<code>([^<]+)</code>'
    r'.{0,200}?'
    r'[Pp]ort\s*:?\s*<code>(\d+)</code>'
    r'.{0,400}?'
    r'[Ss]ecret\s*:?\s*<code>([^<]+)</code>',
    re.DOTALL | re.IGNORECASE,
)

# Plain "server:port:secret" triple (hex secret >= 32 chars)
_RE_PLAIN_MTPROTO = re.compile(
    r'\b([\w.\-]+):(\d{2,5}):([0-9a-fA-F]{32,})\b',
)

# Plain "IP:PORT" pairs (for SOCKS lists from GitHub)
_RE_IP_PORT = re.compile(
    r'\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b',
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def _fetch_html(session, url: str, timeout: float = 20.0) -> str:
    try:
        async with session.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=aiohttp.ClientTimeout(total=timeout),
            ssl=False,
        ) as resp:
            if resp.status == 200:
                return await resp.text(errors="replace")
            logger.debug("fetch %s -> HTTP %s", url, resp.status)
    except Exception as exc:
        logger.debug("fetch %s failed: %s", url, exc)
    return ""

# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _is_fake(server: str) -> bool:
    s = server.lower()
    return s in _FAKE_SERVERS or s.endswith(".telegram.org")


def _parse_tg_url(raw: str):
    """Parse a tg://proxy or https://t.me/proxy URL (may contain &amp;)."""
    from proxy_manager import _make_proxy
    raw = html_mod.unescape(raw)   # &amp; -> &
    try:
        parsed = urllib.parse.urlparse(raw)
        params = urllib.parse.parse_qs(parsed.query)
        server = params.get("server", [None])[0]
        port   = params.get("port",   [None])[0]
        secret = params.get("secret", [None])[0]
        if server and port and not _is_fake(server):
            return _make_proxy(server.strip(), int(port), "mtproto", secret=secret)
    except Exception:
        pass
    return None


def _extract_from_html(html: str, default_type: str = "mtproto") -> list:
    """Pull every recognisable proxy out of a page of HTML or plain text."""
    from proxy_manager import _make_proxy, parse_proxy
    found = []
    seen  = set()

    def _add(p):
        if p is None:
            return
        key = (p["server"].lower(), p["port"])
        if key not in seen:
            seen.add(key)
            found.append(p)

    # 1. tg:// / t.me/proxy links inside href="..." (HTML-encoded &amp;)
    for m in _RE_TG_HREF.finditer(html):
        _add(_parse_tg_url(m.group(1)))

    # 2. Same links free-standing in post text
    for m in _RE_TG_TEXT.finditer(html):
        _add(_parse_tg_url(m.group(0)))

    # 3. Structured post format (Server / Port / Secret in <code> tags)
    for m in _RE_STRUCTURED.finditer(html):
        server = m.group(1).strip()
        port_s = m.group(2).strip()
        secret = m.group(3).strip()
        try:
            port = int(port_s)
            if 1 <= port <= 65535 and not _is_fake(server):
                _add(_make_proxy(server, port, "mtproto", secret=secret))
        except ValueError:
            pass

    # 4. SOCKS5/4 URIs
    for m in _RE_SOCKS.finditer(html):
        raw = html_mod.unescape(m.group(0)).rstrip(".,;)")
        p = parse_proxy(raw)
        if p and not _is_fake(p["server"]):
            _add(p)

    # 5. HTTP proxy URIs — only if port looks like a proxy port
    for m in _RE_HTTP.finditer(html):
        raw = html_mod.unescape(m.group(0)).rstrip(".,;)")
        try:
            parsed = urllib.parse.urlparse(raw)
            if parsed.port in _PROXY_PORTS and not _is_fake(parsed.hostname or ""):
                p = parse_proxy(raw)
                if p:
                    _add(p)
        except Exception:
            pass

    # 6. Plain server:port:secret MTProto triples
    for m in _RE_PLAIN_MTPROTO.finditer(html):
        server = m.group(1)
        port_s = m.group(2)
        secret = m.group(3)
        try:
            port = int(port_s)
            if 1 <= port <= 65535 and not _is_fake(server):
                _add(_make_proxy(server, port, "mtproto", secret=secret))
        except ValueError:
            pass

    # 7. Plain IP:PORT pairs (GitHub SOCKS lists, etc.)
    if default_type == "socks5":
        for m in _RE_IP_PORT.finditer(html):
            server = m.group(1)
            port_s = m.group(2)
            try:
                port = int(port_s)
                if 1 <= port <= 65535 and not _is_fake(server):
                    _add(_make_proxy(server, port, "socks5"))
            except ValueError:
                pass

    return found

# ---------------------------------------------------------------------------
# Telegram channel scraper
# ---------------------------------------------------------------------------

async def fetch_from_channel(channel: str, limit_pages: int = 10) -> list:
    """
    Scrape a public Telegram channel for proxies.

    `channel` can be a username (@ProxyMTProto / ProxyMTProto) or
    a full t.me/s/... URL.

    Returns a list of parsed proxy dicts (caller dedupes).
    """
    if not _AIOHTTP:
        logger.warning("aiohttp not installed -- channel fetching disabled")
        return []

    # Normalise to plain username
    channel = channel.strip().lstrip("@")
    if channel.startswith("http"):
        channel = channel.rstrip("/").split("/")[-1]
        if channel.startswith("s"):
            channel = channel[1:].lstrip("/")

    all_proxies: list = []
    url = f"https://t.me/s/{channel}"

    async with aiohttp.ClientSession() as session:
        for page_num in range(limit_pages):
            html = await _fetch_html(session, url)
            if not html:
                logger.debug("@%s page %d: no HTML", channel, page_num + 1)
                break

            batch = _extract_from_html(html)
            all_proxies.extend(batch)
            logger.debug("@%s page %d: +%d proxies (total %d)",
                         channel, page_num + 1, len(batch), len(all_proxies))

            # Paginate: find oldest post ID and request ?before=ID
            m = re.search(r'data-before=["\'](\d+)["\']', html)
            if not m:
                m = re.search(r'href=["\'][^"\']*\?before=(\d+)["\']', html)
            if not m:
                m = re.search(r'data-post=["\'][\w/]+?(\d+)["\']', html)
            if not m:
                break

            oldest_id = m.group(1)
            next_url  = f"https://t.me/s/{channel}?before={oldest_id}"
            if next_url == url:
                break
            url = next_url
            await asyncio.sleep(1.2)   # be polite

    logger.info("Channel @%s: %d unique proxy candidates across %d page(s)",
                channel, len(all_proxies), page_num + 1)
    return all_proxies


async def fetch_from_channels(channels: list) -> list:
    """Fetch from all configured channels concurrently."""
    tasks   = [fetch_from_channel(ch) for ch in channels]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    combined = []
    for ch, r in zip(channels, results):
        if isinstance(r, list):
            combined.extend(r)
        else:
            logger.warning("fetch_from_channel @%s raised: %s", ch, r)
    return combined

# ---------------------------------------------------------------------------
# Generic URL source fetcher (GitHub raw, plain-text lists, APIs)
# ---------------------------------------------------------------------------

async def fetch_from_url(url: str) -> list:
    """
    Fetch proxies from an arbitrary URL.

    Handles:
      • Plain text IP:PORT lists (GitHub raw)
      • Plain text tg://proxy or t.me/proxy links
      • JSON responses from proxylist.geonode.com
      • Any HTML with embedded proxy links
    """
    if not _AIOHTTP:
        return []

    url = url.strip()
    if not url:
        return []

    # Detect geonode API
    if "geonode.com/api/proxy-list" in url:
        return await _fetch_geonode(url)

    async with aiohttp.ClientSession() as session:
        text = await _fetch_html(session, url)

    if not text:
        return []

    # Determine default type: SOCKS lists usually live under "socks5" in the path
    default_type = "socks5" if "socks5" in url.lower() else "mtproto"
    found = _extract_from_html(text, default_type=default_type)
    logger.info("URL %s: %d proxy candidates", url, len(found))
    return found


async def _fetch_geonode(url: str) -> list:
    """
    Fetch proxies from the proxylist.geonode.com JSON API.

    API returns: {"data": [{"ip": "1.2.3.4", "port": "1080",
                             "protocols": ["socks5"], ...}, ...]}
    """
    from proxy_manager import _make_proxy

    if not _AIOHTTP:
        return []

    # Ensure we request a reasonable batch
    if "limit=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}limit=500&page=1&sort_by=speed&sort_type=asc"

    found = []
    page  = 1

    async with aiohttp.ClientSession() as session:
        while True:
            page_url = re.sub(r'page=\d+', f'page={page}', url)
            if f'page={page}' not in page_url:
                page_url += f"&page={page}"

            try:
                async with session.get(
                    page_url,
                    headers={"User-Agent": USER_AGENT},
                    timeout=aiohttp.ClientTimeout(total=20),
                    ssl=False,
                ) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json(content_type=None)
            except Exception as exc:
                logger.debug("geonode fetch page %d failed: %s", page, exc)
                break

            items = data.get("data", [])
            if not items:
                break

            for item in items:
                ip        = item.get("ip", "")
                port_s    = item.get("port", "")
                protocols = item.get("protocols", ["socks5"])
                if not ip or not port_s:
                    continue
                try:
                    port  = int(port_s)
                    ptype = protocols[0].lower() if protocols else "socks5"
                    if ptype not in ("socks5", "socks4", "http", "https"):
                        ptype = "socks5"
                    found.append(_make_proxy(ip, port, ptype))
                except Exception:
                    pass

            total_pages = data.get("total", 0) // max(data.get("limit", 500), 1) + 1
            if page >= min(total_pages, 5):   # cap at 5 pages (2500 proxies)
                break
            page += 1
            await asyncio.sleep(1.0)   # polite paging

    logger.info("Geonode API: %d proxies fetched", len(found))
    return found


async def fetch_from_urls(urls: list) -> list:
    """Fetch from all configured source URLs concurrently."""
    if not urls:
        return []
    tasks   = [fetch_from_url(u) for u in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    combined = []
    for url, r in zip(urls, results):
        if isinstance(r, list):
            combined.extend(r)
        else:
            logger.warning("fetch_from_url %s raised: %s", url, r)
    return combined


async def fetch_all(channels: list, urls: list | None = None) -> list:
    """
    Fetch from all channels AND all URL sources in parallel.

    Returns a combined, deduplicated-ready list (dedup happens in the caller
    via pm.add_proxy which checks proxy_key).
    """
    tasks = []
    if channels:
        tasks.append(fetch_from_channels(channels))
    if urls:
        tasks.append(fetch_from_urls(urls))

    if not tasks:
        return []

    results = await asyncio.gather(*tasks, return_exceptions=True)
    combined = []
    for r in results:
        if isinstance(r, list):
            combined.extend(r)
    return combined

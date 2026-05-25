"""
geoip.py — Async geolocation for proxy servers.

Uses ip-api.com (free tier: 45 req/min, no API key required).

Public API:
    lookup(host)              -> (country_code, country_name) | (None, None)
    enrich_proxies(proxies)   -> annotates country_code/country_name in-place
"""

import asyncio
import logging
import socket

logger = logging.getLogger(__name__)

# In-process cache: hostname/IP -> (country_code, country_name)
_cache: dict[str, tuple[str | None, str | None]] = {}

# ip-api.com free tier limit
_RATE_LIMIT     = 40          # requests per minute (stay below 45 cap)
_RATE_WINDOW    = 60.0        # seconds
_BATCH_DELAY    = _RATE_WINDOW / _RATE_LIMIT   # ~1.5 s between requests

# ip-api.com endpoint (HTTP only on free tier)
_BASE_URL = "http://ip-api.com/json/{host}?fields=status,countryCode,country"

try:
    import aiohttp as _aiohttp
    _HAS_AIOHTTP = True
except ImportError:
    _HAS_AIOHTTP = False


async def _resolve_to_ip(host: str) -> str:
    """Resolve hostname to IP; return host unchanged if already an IP."""
    import re
    if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", host):
        return host
    try:
        loop   = asyncio.get_running_loop()
        infos  = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        return infos[0][4][0]
    except Exception:
        return host


async def lookup(host: str) -> tuple[str | None, str | None]:
    """
    Return (country_code, country_name) for a hostname or IP.
    Returns (None, None) on error or if aiohttp is not installed.

    Results are cached in-process for the duration of the run.
    """
    if not _HAS_AIOHTTP:
        return None, None

    # Resolve to IP for a more cache-stable key
    ip = await _resolve_to_ip(host)
    if ip in _cache:
        return _cache[ip]

    url = _BASE_URL.format(host=ip)
    try:
        async with _aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=_aiohttp.ClientTimeout(total=8),
                ssl=False,
            ) as resp:
                if resp.status != 200:
                    _cache[ip] = (None, None)
                    return None, None
                data = await resp.json(content_type=None)

        if data.get("status") == "success":
            code = data.get("countryCode") or None
            name = data.get("country")     or None
            _cache[ip] = (code, name)
            return code, name

    except Exception as exc:
        logger.debug("geoip lookup %s failed: %s", host, exc)

    _cache[ip] = (None, None)
    return None, None


async def enrich_proxies(proxies: list, max_lookups: int = 100) -> int:
    """
    Annotate proxies that are alive but lack country info.

    Respects ip-api.com's free-tier rate limit by spacing requests
    ~1.5 s apart.  Processes up to max_lookups proxies per call.

    Returns the number of proxies successfully annotated.
    """
    if not _HAS_AIOHTTP:
        return 0

    to_enrich = [
        p for p in proxies
        if p.get("alive") and p.get("country_code") is None
    ][:max_lookups]

    enriched = 0
    for p in to_enrich:
        code, name = await lookup(p["server"])
        if code:
            p["country_code"] = code
            p["country_name"] = name
            enriched += 1
        await asyncio.sleep(_BATCH_DELAY)   # honour rate limit

    return enriched

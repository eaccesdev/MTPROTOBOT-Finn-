"""
geoip.py — Async geolocation for proxy servers.

Uses ip-api.com (free tier: 45 req/min, no API key required).

Improvements:
  #4  Persistent cache: results are saved to geoip_cache.json and loaded on
      startup — eliminates redundant lookups across restarts.
  #5  Batch endpoint: enrich_proxies() now sends up to 100 IPs per POST
      request instead of one per request, cutting enrichment time from
      ~2.5 minutes to a few seconds for 100 proxies.

Public API:
    lookup(host)              -> (country_code, country_name) | (None, None)
    enrich_proxies(proxies)   -> annotates country_code/country_name in-place
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time

logger = logging.getLogger(__name__)

# In-process cache: IP -> (country_code, country_name)
_cache: dict[str, tuple[str | None, str | None]] = {}
_CACHE_FILE: str | None = None

# ip-api.com free tier
_RATE_LIMIT  = 40          # requests per minute
_RATE_WINDOW = 60.0
_BATCH_SIZE  = 100         # /batch endpoint accepts up to 100 IPs per request
_BATCH_URL   = "http://ip-api.com/batch?fields=status,query,countryCode,country"
_SINGLE_URL  = "http://ip-api.com/json/{host}?fields=status,countryCode,country"

try:
    import aiohttp as _aiohttp
    _HAS_AIOHTTP = True
except ImportError:
    _HAS_AIOHTTP = False


# ---------------------------------------------------------------------------
# #4 — Persistent cache
# ---------------------------------------------------------------------------

def _cache_path() -> str:
    import proxy_manager as _pm
    return os.path.join(_pm.DATA_DIR, "geoip_cache.json")


def _load_cache() -> None:
    """Load persisted GeoIP cache from disk."""
    global _cache, _CACHE_FILE
    _CACHE_FILE = _cache_path()
    if not os.path.exists(_CACHE_FILE):
        return
    try:
        with open(_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        _cache.update(data)
        logger.debug("Loaded %d GeoIP cache entries from disk", len(_cache))
    except Exception as exc:
        logger.debug("geoip_cache load failed: %s", exc)


def _save_cache() -> None:
    """Persist GeoIP cache to disk atomically."""
    path = _CACHE_FILE or _cache_path()
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_cache, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as exc:
        logger.debug("geoip_cache save failed: %s", exc)


try:
    _load_cache()
except Exception:
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _resolve_to_ip(host: str) -> str:
    """Resolve hostname to IP; return host unchanged if already an IP."""
    import re
    if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", host):
        return host
    try:
        loop  = asyncio.get_running_loop()
        infos = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        return infos[0][4][0]
    except Exception:
        return host


# ---------------------------------------------------------------------------
# #5 — Batch enrichment using ip-api.com /batch endpoint
# ---------------------------------------------------------------------------

async def enrich_proxies(proxies: list, max_lookups: int = 100) -> int:
    """
    Annotate proxies that are alive but lack country info.

    Uses the ip-api.com /batch endpoint (up to 100 IPs per POST) instead of
    sequential single lookups — dramatically faster and simpler (#5).

    Returns the number of proxies successfully annotated.
    """
    if not _HAS_AIOHTTP:
        return 0

    to_enrich = [
        p for p in proxies
        if p.get("alive") and p.get("country_code") is None
    ][:max_lookups]

    if not to_enrich:
        return 0

    # Resolve all hostnames to IPs for a stable cache key
    ips = []
    for p in to_enrich:
        ip = await _resolve_to_ip(p["server"])
        ips.append(ip)

    # Filter already cached
    uncached_ips   = [ip for ip in ips if ip not in _cache]
    uncached_ips   = list(dict.fromkeys(uncached_ips))  # deduplicate preserving order

    # Fetch in batches of _BATCH_SIZE
    enriched = 0
    if uncached_ips:
        try:
            async with _aiohttp.ClientSession() as session:
                for batch_start in range(0, len(uncached_ips), _BATCH_SIZE):
                    batch = uncached_ips[batch_start:batch_start + _BATCH_SIZE]
                    try:
                        async with session.post(
                            _BATCH_URL,
                            json=batch,
                            timeout=_aiohttp.ClientTimeout(total=15),
                            ssl=False,
                        ) as resp:
                            if resp.status != 200:
                                break
                            results = await resp.json(content_type=None)
                    except Exception as exc:
                        logger.debug("geoip batch failed: %s", exc)
                        break

                    for item in results:
                        ip   = item.get("query", "")
                        if item.get("status") == "success":
                            code = item.get("countryCode") or None
                            name = item.get("country")     or None
                        else:
                            code, name = None, None
                        _cache[ip] = (code, name)

                    # Respect rate limit: one batch per ~1.5 seconds
                    if batch_start + _BATCH_SIZE < len(uncached_ips):
                        await asyncio.sleep(1.5)

            _save_cache()
        except Exception as exc:
            logger.debug("geoip batch session error: %s", exc)

    # Apply cached results to proxies
    for p, ip in zip(to_enrich, ips):
        if ip in _cache:
            code, name = _cache[ip]
            if code:
                p["country_code"] = code
                p["country_name"] = name
                enriched += 1

    return enriched


# ---------------------------------------------------------------------------
# Single-proxy lookup (used by lookup() for on-demand geo)
# ---------------------------------------------------------------------------

async def lookup(host: str) -> tuple[str | None, str | None]:
    """
    Return (country_code, country_name) for a hostname or IP.
    Results are cached in-process and on disk.
    """
    if not _HAS_AIOHTTP:
        return None, None

    ip = await _resolve_to_ip(host)
    if ip in _cache:
        return _cache[ip]

    url = _SINGLE_URL.format(host=ip)
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
            _save_cache()
            return code, name

    except Exception as exc:
        logger.debug("geoip lookup %s failed: %s", host, exc)

    _cache[ip] = (None, None)
    return None, None

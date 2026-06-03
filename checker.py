"""
checker.py — Async TCP reachability checks for proxies.

Strategy:
  1. DNS pre-check  — resolve hostname first; mark dead immediately on failure.
     DNS results are cached with a 10-minute TTL and persisted to dns_cache.json
     so the cache survives restarts (#4).
  2. Adaptive timeout — alive proxies use a 4 s short timeout; unknown/dead
     proxies use the full configured timeout.
  3. Protocol verification — for SOCKS5 proxies the full SOCKS5 handshake is
     performed (including auth if credentials are present) so we detect proxies
     that accept TCP but are not actually functioning SOCKS5 servers (#8).
  4. TCP connect    — for MTProto and HTTP proxies (and SOCKS5 as fallback).
  5. Double-confirm — first failure on a reliable proxy is re-tried once.
  6. Stats update   — increment check_count / success_count, record last_alive,
     maintain rolling latency_history window (last 5 readings).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# DNS cache: {host: (resolved: bool, timestamp: float)}
_dns_cache: dict[str, tuple[bool, float]] = {}
_DNS_TTL   = 600.0   # 10 minutes
_DNS_CACHE_FILE: str | None = None   # set by _init_dns_cache()

# Latency history window size per proxy
_LAT_HISTORY = 5

# Short timeout for already-alive proxies (speeds bulk re-checks)
_ALIVE_TIMEOUT = 4.0

# Retry timeout for the double-confirm re-check on first failure
_CONFIRM_TIMEOUT = 3.0


def _init_dns_cache() -> None:
    """Load persisted DNS cache from disk (#4)."""
    global _DNS_CACHE_FILE, _dns_cache
    import proxy_manager as _pm
    _DNS_CACHE_FILE = os.path.join(_pm.DATA_DIR, "dns_cache.json")
    if not os.path.exists(_DNS_CACHE_FILE):
        return
    try:
        with open(_DNS_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        now = time.monotonic()
        # Only load entries that haven't expired yet
        for host, (resolved, ts_wall) in data.items():
            # ts_wall is a Unix timestamp; convert to monotonic-equivalent age
            age = time.time() - ts_wall
            if age < _DNS_TTL:
                _dns_cache[host] = (resolved, now - age)
        logger.debug("Loaded %d DNS cache entries from disk", len(_dns_cache))
    except Exception as exc:
        logger.debug("dns_cache load failed: %s", exc)


def _save_dns_cache() -> None:
    """Persist DNS cache to disk (#4)."""
    if not _DNS_CACHE_FILE:
        return
    try:
        now_mono = time.monotonic()
        now_wall = time.time()
        out: dict = {}
        for host, (resolved, ts_mono) in _dns_cache.items():
            age = now_mono - ts_mono
            if age < _DNS_TTL:
                out[host] = (resolved, now_wall - age)
        tmp = _DNS_CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(out, f)
        os.replace(tmp, _DNS_CACHE_FILE)
    except Exception as exc:
        logger.debug("dns_cache save failed: %s", exc)


# Initialize cache on first import (non-fatal if DATA_DIR not yet set)
try:
    _init_dns_cache()
except Exception:
    pass


async def _dns_ok(host: str) -> bool:
    """
    Return True if the hostname resolves.
    Results are cached with a 10-minute TTL and persisted across restarts.
    """
    import re as _re
    now = time.monotonic()

    cached = _dns_cache.get(host)
    if cached is not None:
        result, ts = cached
        if (now - ts) < _DNS_TTL:
            return result

    # IP literals always resolve
    if _re.match(r"^\d{1,3}(\.\d{1,3}){3}$", host):
        _dns_cache[host] = (True, now)
        return True

    try:
        lp = asyncio.get_running_loop()
        await lp.getaddrinfo(host, None)
        _dns_cache[host] = (True, now)
        _save_dns_cache()
        return True
    except Exception:
        _dns_cache[host] = (False, now)
        _save_dns_cache()
        return False


async def _tcp_connect(server: str, port: int, timeout: float) -> float | None:
    """
    Attempt a TCP connection.  Returns latency in ms on success, None on failure.
    """
    t0 = time.monotonic()
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(server, port),
            timeout=timeout,
        )
        latency_ms = (time.monotonic() - t0) * 1000.0
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return latency_ms
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
        return None
    except Exception as exc:
        logger.debug("tcp_connect %s:%s error: %s", server, port, exc)
        return None


# ---------------------------------------------------------------------------
# #8 — SOCKS5 protocol handshake verification
# ---------------------------------------------------------------------------

async def _socks5_check(server: str, port: int, timeout: float,
                        username: str | None = None,
                        password: str | None = None) -> float | None:
    """
    Perform a real SOCKS5 handshake to verify the proxy actually speaks SOCKS5.

    Returns latency in ms on success, None on failure.

    Handshake:
      → Client: VER=5, NAUTH=2, METHODS=[0x00 no-auth, 0x02 user/pass]
      ← Server: VER=5, METHOD chosen
      If METHOD=0x02 (user/pass auth) and credentials provided:
        → Client: auth sub-negotiation
        ← Server: auth result (0x00 = success)
    """
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(server, port),
            timeout=timeout,
        )
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
        return None
    except Exception:
        return None

    try:
        # Greeting: support no-auth (0x00) and user/pass (0x02)
        writer.write(b"\x05\x02\x00\x02")
        await asyncio.wait_for(writer.drain(), timeout=2.0)

        resp = await asyncio.wait_for(reader.read(2), timeout=3.0)
        if len(resp) < 2 or resp[0] != 0x05:
            return None  # not a SOCKS5 server

        method = resp[1]
        if method == 0xFF:
            return None  # no acceptable method

        if method == 0x02:
            # Username/password sub-negotiation
            uname = (username or "").encode()
            upass = (password or "").encode()
            auth_msg = (
                b"\x01"
                + bytes([len(uname)]) + uname
                + bytes([len(upass)]) + upass
            )
            writer.write(auth_msg)
            await asyncio.wait_for(writer.drain(), timeout=2.0)
            auth_resp = await asyncio.wait_for(reader.read(2), timeout=3.0)
            if len(auth_resp) < 2 or auth_resp[1] != 0x00:
                return None  # auth rejected

        latency_ms = (time.monotonic() - t0) * 1000.0
        return latency_ms

    except (asyncio.TimeoutError, ConnectionError, OSError):
        return None
    except Exception as exc:
        logger.debug("socks5_check %s:%s error: %s", server, port, exc)
        return None
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main check function
# ---------------------------------------------------------------------------

async def check_one(proxy: dict, timeout: float = 8.0) -> bool:
    """
    Check a single proxy for reachability.

    For SOCKS5 proxies: performs a full SOCKS5 handshake (#8).
    For MTProto / HTTP: performs a TCP connect and measures RTT.

    Updates in-place:
      proxy['alive']          — True / False
      proxy['last_checked']   — ISO timestamp of this check
      proxy['last_alive']     — ISO timestamp of last successful check
      proxy['latency_ms']     — RTT in ms (on success)
      proxy['latency_history']— Rolling list of last 5 latency readings
      proxy['check_count']    — total checks performed
      proxy['success_count']  — checks that returned alive
      proxy['fail_streak']    — consecutive failures (reset on success)

    Returns True if reachable, False otherwise.
    """
    server = proxy["server"]
    port   = proxy["port"]
    ptype  = proxy.get("type", "mtproto")
    now    = datetime.now(timezone.utc).isoformat()

    proxy.setdefault("check_count",   0)
    proxy.setdefault("success_count", 0)

    # DNS pre-check
    if not await _dns_ok(server):
        proxy["alive"]        = False
        proxy["last_checked"] = now
        proxy["check_count"] += 1
        proxy["fail_streak"]  = proxy.get("fail_streak", 0) + 1
        return False

    was_alive    = proxy.get("alive") is True
    prev_streak  = proxy.get("fail_streak", 0)
    effective_to = min(timeout, _ALIVE_TIMEOUT) if was_alive else timeout

    # Protocol-aware check
    if ptype == "socks5":
        latency_ms = await _socks5_check(
            server, port, effective_to,
            username=proxy.get("username"),
            password=proxy.get("password"),
        )
    else:
        latency_ms = await _tcp_connect(server, port, effective_to)

    # Double-confirm on first failure for previously reliable proxies
    if latency_ms is None and was_alive and prev_streak == 0:
        logger.debug("check_one %s:%s first failure — confirming…", server, port)
        if ptype == "socks5":
            latency_ms = await _socks5_check(
                server, port, _CONFIRM_TIMEOUT,
                username=proxy.get("username"),
                password=proxy.get("password"),
            )
        else:
            latency_ms = await _tcp_connect(server, port, _CONFIRM_TIMEOUT)

    alive = latency_ms is not None

    proxy["alive"]        = alive
    proxy["last_checked"] = now
    proxy["check_count"] += 1

    if alive:
        proxy["success_count"] = proxy["success_count"] + 1
        proxy["last_alive"]    = now
        proxy["latency_ms"]    = latency_ms
        proxy["fail_streak"]   = 0

        hist = proxy.setdefault("latency_history", [])
        hist.append(latency_ms)
        if len(hist) > _LAT_HISTORY:
            proxy["latency_history"] = hist[-_LAT_HISTORY:]
    else:
        proxy["fail_streak"] = proxy.get("fail_streak", 0) + 1

    return alive


async def check_all(proxies: list, timeout: float = 10.0,
                    concurrency: int = 50,
                    progress_callback=None) -> dict:
    """
    Check every proxy in the list concurrently.

    progress_callback(done: int, total: int) is called after each check.

    Returns {"total": N, "alive": A, "dead": D}
    """
    semaphore  = asyncio.Semaphore(concurrency)
    total      = len(proxies)
    done_count = {"n": 0}

    async def _check(proxy):
        async with semaphore:
            result = await check_one(proxy, timeout)
            done_count["n"] += 1
            if progress_callback:
                try:
                    await progress_callback(done_count["n"], total)
                except Exception:
                    pass
            return result

    results = await asyncio.gather(*[_check(p) for p in proxies],
                                   return_exceptions=True)

    alive = sum(1 for r in results if r is True)
    dead  = sum(1 for r in results if r is False)
    return {"total": total, "alive": alive, "dead": dead}

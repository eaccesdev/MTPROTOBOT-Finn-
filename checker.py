"""
checker.py — Async TCP reachability checks for proxies.

Strategy:
  1. DNS pre-check  — resolve hostname first; mark dead immediately on failure
     without burning the full TCP timeout on an invalid host.
  2. TCP connect    — open a connection to server:port, measuring RTT.
  3. Stats update   — increment check_count / success_count, record last_alive.

This is a "port-open" check, not a full protocol handshake, but it reliably
detects blocked / down proxies faster thanks to the DNS pre-check.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# DNS results are cached in-process so repeated checks of the same host
# (common when the pool has many proxies on the same server) hit the OS
# cache instead of the network.
_dns_cache: dict[str, bool] = {}


async def _dns_ok(host: str, loop=None) -> bool:
    """
    Return True if the hostname resolves.  Caches results for the process
    lifetime to avoid redundant look-ups during bulk checks.
    """
    if host in _dns_cache:
        return _dns_cache[host]

    # IP literals always resolve
    import re
    if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", host):
        _dns_cache[host] = True
        return True

    try:
        lp = loop or asyncio.get_running_loop()
        await lp.getaddrinfo(host, None)
        _dns_cache[host] = True
        return True
    except Exception:
        _dns_cache[host] = False
        return False


async def check_one(proxy: dict, timeout: float = 8.0) -> bool:
    """
    Try to open a TCP connection to proxy['server']:proxy['port'].

    Updates in-place:
      proxy['alive']         — True / False
      proxy['last_checked']  — ISO timestamp of this check
      proxy['last_alive']    — ISO timestamp of last successful check
      proxy['latency_ms']    — TCP RTT in ms (only on success)
      proxy['check_count']   — total checks performed
      proxy['success_count'] — checks that returned alive

    Returns True if reachable, False otherwise.
    """
    server = proxy["server"]
    port   = proxy["port"]
    now    = datetime.now(timezone.utc).isoformat()
    alive  = False
    latency_ms = None

    # --- DNS pre-check (skip slow TCP timeout for dead hostnames) ---
    if not await _dns_ok(server):
        proxy["alive"]        = False
        proxy["last_checked"] = now
        proxy.setdefault("check_count",   0)
        proxy.setdefault("success_count", 0)
        proxy["check_count"] += 1
        return False

    # --- TCP connect with latency timing ---
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
        alive = True
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
        alive = False
    except Exception as exc:
        logger.debug("check_one %s:%s error: %s", server, port, exc)
        alive = False

    # --- Update proxy fields ---
    proxy.setdefault("check_count",   0)
    proxy.setdefault("success_count", 0)

    proxy["alive"]        = alive
    proxy["last_checked"] = now
    proxy["check_count"]  = proxy["check_count"] + 1

    if alive:
        proxy["success_count"] = proxy["success_count"] + 1
        proxy["last_alive"]    = now
        proxy["latency_ms"]    = latency_ms
        proxy["fail_streak"]   = 0          # reset consecutive-fail counter
    else:
        proxy["fail_streak"] = proxy.get("fail_streak", 0) + 1

    return alive


async def check_all(proxies: list, timeout: float = 10.0,
                    concurrency: int = 50,
                    progress_callback=None) -> dict:
    """
    Check every proxy in the list concurrently.

    progress_callback(done: int, total: int) is called after each check
    if provided (useful for Telegram progress messages).

    Returns a summary dict:
      {"total": N, "alive": A, "dead": D}
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

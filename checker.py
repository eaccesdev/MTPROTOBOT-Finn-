"""
checker.py — Async TCP reachability checks for proxies.

Strategy:
  1. DNS pre-check  — resolve hostname first; mark dead immediately on failure
     without burning the full TCP timeout on an invalid host. DNS results are
     cached with a 10-minute TTL so stale entries don't mask host changes.
  2. Adaptive timeout — alive proxies use a 4 s short timeout; unknown/dead
     proxies use the full configured timeout.  This makes bulk re-checks of a
     mostly-alive pool dramatically faster.
  3. TCP connect    — open a connection to server:port, measuring RTT.
  4. Double-confirm — if a proxy had no recent failures (fail_streak == 0) and
     this check fails, we immediately retry once with a 3 s timeout before
     declaring it dead.  This prevents false positives from transient blips.
  5. Stats update   — increment check_count / success_count, record last_alive,
     and maintain a rolling latency_history window (last 5 readings).
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# DNS results cached as {host: (resolved: bool, timestamp: float)}
# Entries expire after DNS_TTL seconds so host changes are picked up.
_dns_cache: dict[str, tuple[bool, float]] = {}
_DNS_TTL = 600.0      # 10 minutes

# Latency history window size per proxy
_LAT_HISTORY = 5

# Short timeout used when a proxy is already known-alive (speeds up bulk checks)
_ALIVE_TIMEOUT = 4.0

# Retry timeout for the double-confirm re-check on first failure
_CONFIRM_TIMEOUT = 3.0


async def _dns_ok(host: str) -> bool:
    """
    Return True if the hostname resolves.  Results are cached with a 10-minute
    TTL to avoid redundant look-ups during bulk checks while still detecting
    host changes within a reasonable window.
    """
    now = time.monotonic()

    cached = _dns_cache.get(host)
    if cached is not None:
        result, ts = cached
        if (now - ts) < _DNS_TTL:
            return result
        # Expired — fall through to re-resolve

    # IP literals always resolve
    import re
    if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", host):
        _dns_cache[host] = (True, now)
        return True

    try:
        lp = asyncio.get_running_loop()
        await lp.getaddrinfo(host, None)
        _dns_cache[host] = (True, now)
        return True
    except Exception:
        _dns_cache[host] = (False, now)
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


async def check_one(proxy: dict, timeout: float = 8.0) -> bool:
    """
    Try to open a TCP connection to proxy['server']:proxy['port'].

    Updates in-place:
      proxy['alive']          — True / False
      proxy['last_checked']   — ISO timestamp of this check
      proxy['last_alive']     — ISO timestamp of last successful check
      proxy['latency_ms']     — TCP RTT in ms (only on success)
      proxy['latency_history']— Rolling list of last 5 latency readings
      proxy['check_count']    — total checks performed
      proxy['success_count']  — checks that returned alive
      proxy['fail_streak']    — consecutive failures (reset on success)

    Returns True if reachable, False otherwise.
    """
    server = proxy["server"]
    port   = proxy["port"]
    now    = datetime.now(timezone.utc).isoformat()

    proxy.setdefault("check_count",   0)
    proxy.setdefault("success_count", 0)

    # --- DNS pre-check ---
    if not await _dns_ok(server):
        proxy["alive"]        = False
        proxy["last_checked"] = now
        proxy["check_count"] += 1
        proxy["fail_streak"]  = proxy.get("fail_streak", 0) + 1
        return False

    # --- Adaptive timeout: shorter for already-alive proxies ---
    was_alive      = proxy.get("alive") is True
    prev_streak    = proxy.get("fail_streak", 0)
    effective_to   = min(timeout, _ALIVE_TIMEOUT) if was_alive else timeout

    latency_ms = await _tcp_connect(server, port, effective_to)

    # --- Double-confirm on first failure for previously reliable proxies ---
    if latency_ms is None and was_alive and prev_streak == 0:
        logger.debug("check_one %s:%s first failure — confirming…", server, port)
        latency_ms = await _tcp_connect(server, port, _CONFIRM_TIMEOUT)

    alive = latency_ms is not None

    # --- Update proxy fields ---
    proxy["alive"]        = alive
    proxy["last_checked"] = now
    proxy["check_count"] += 1

    if alive:
        proxy["success_count"] = proxy["success_count"] + 1
        proxy["last_alive"]    = now
        proxy["latency_ms"]    = latency_ms
        proxy["fail_streak"]   = 0

        # Maintain rolling latency history (last _LAT_HISTORY readings)
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

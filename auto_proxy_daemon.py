#!/usr/bin/env python3
"""
auto_proxy_daemon.py — Zero-tap Telegram proxy auto-switcher
=============================================================

Runs as a local daemon alongside the proxy manager bot.
Uses Telethon (MTProto user-account client) to maintain a live
Telegram connection through the best available proxy.

Pipeline:
  1. Read alive proxies from the shared proxies.json pool
  2. Connect to Telegram through the best one
  3. Every N seconds: ping Telegram to verify the connection
  4. If the ping fails → pick next alive proxy → reconnect silently
  5. If no alive proxies → trigger a check / fetch cycle
  6. Update connection.json so the bot stays in sync

SETUP (one-time):
  1. Get your api_id and api_hash from https://my.telegram.org
  2. Copy daemon_config.example.json → daemon_config.json
  3. Fill in api_id and api_hash
  4. Run:  python3 auto_proxy_daemon.py
  5. On first run: enter your phone number + code when prompted
  6. After login the session is saved — fully automatic from then on

Requirements:
  pip install telethon PySocks
"""

import asyncio
import base64
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

# Suppress "coroutine ignored GeneratorExit" noise from Telethon's generator-
# based coroutines when asyncio.timeout() cancels them mid-flight.
# These are benign cleanup errors that don't affect functionality.
_orig_unraisablehook = sys.unraisablehook

def _quiet_unraisable(unraisable):
    if (isinstance(unraisable.exc_value, RuntimeError) and
            "GeneratorExit" in str(unraisable.exc_value)):
        return
    _orig_unraisablehook(unraisable)

sys.unraisablehook = _quiet_unraisable

try:
    from telethon import TelegramClient, errors
    from telethon.network.connection.tcpmtproxy import (
        ConnectionTcpMTProxyRandomizedIntermediate,
    )
except ImportError:
    sys.exit(
        "Missing dependencies.\n"
        "Run:  pip install telethon 'python-socks[asyncio]'\n"
        "  or:  venv/bin/pip install telethon 'python-socks[asyncio]'"
    )

# SOCKS proxy support — python-socks preferred (Telethon native), PySocks fallback
try:
    import python_socks  # noqa: F401 — presence check only
    _SOCKS_VIA_PYTHON_SOCKS = True
except ImportError:
    _SOCKS_VIA_PYTHON_SOCKS = False
    try:
        import socks as _pysocks
    except ImportError:
        _pysocks = None

# Share the proxy pool with the bot
import proxy_manager as pm
import checker
import connection as conn
import fetcher

import shutil
import subprocess
import threading

# ---------------------------------------------------------------------------
# Telegram Desktop auto-apply
# ---------------------------------------------------------------------------

def _apply_to_telegram_desktop(proxy: dict) -> None:
    """
    Push the proxy into Telegram Desktop automatically.

    Mechanism:
      1. Build a tg://proxy?... deep-link and open it with xdg-open.
         Telegram Desktop intercepts the protocol and shows its
         "Use this proxy?" confirmation dialog.
      2. If xdotool is installed, wait 2.5 s for the dialog to render,
         then activate the Telegram window and press Return to confirm
         (the primary button in the proxy dialog is pre-focused).

    Runs in a daemon thread so it never blocks the asyncio event loop.
    Silently does nothing if xdg-open is not available (e.g. headless server).
    """
    if not shutil.which("xdg-open"):
        return

    secret = proxy.get("secret", "")
    url = (
        f"tg://proxy?server={proxy['server']}"
        f"&port={proxy['port']}"
        f"&secret={secret}"
    )

    def _run() -> None:
        try:
            subprocess.Popen(
                ["xdg-open", url],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            return

        if not shutil.which("xdotool"):
            return

        # Give Telegram time to render the confirmation dialog.
        time.sleep(2.5)
        try:
            # Bring the Telegram window to the front, then confirm the dialog.
            # The "Use this proxy" button has keyboard focus when the dialog
            # opens, so Return accepts it without moving the mouse.
            subprocess.run(
                [
                    "xdotool",
                    "search", "--sync", "--limit", "1", "--name", "Telegram",
                    "windowactivate", "--sync",
                    "key", "--clearmodifiers", "Return",
                ],
                timeout=10,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

    threading.Thread(target=_run, daemon=True).start()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("auto_proxy_daemon")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DAEMON_CONFIG_FILE = "daemon_config.json"
SESSION_FILE       = "daemon_session"   # Telethon saves .session here

_DEFAULT_DAEMON_CFG = {
    "api_id":                  0,
    "api_hash":                "",
    "check_interval_seconds":  30,
    "reconnect_delay_seconds": 5,
    "max_rotate_attempts":     10,
}


def _load_daemon_config() -> dict:
    if not os.path.exists(DAEMON_CONFIG_FILE):
        with open(DAEMON_CONFIG_FILE, "w") as f:
            json.dump(_DEFAULT_DAEMON_CFG, f, indent=2)
        print(
            f"\n[!] Created {DAEMON_CONFIG_FILE}\n"
            "    Fill in your api_id and api_hash from https://my.telegram.org\n"
            "    then run this script again.\n"
        )
        sys.exit(0)

    with open(DAEMON_CONFIG_FILE) as f:
        cfg = json.load(f)
    for k, v in _DEFAULT_DAEMON_CFG.items():
        cfg.setdefault(k, v)
    return cfg

# ---------------------------------------------------------------------------
# Secret conversion — handles hex, base64url, and prefixed formats
# ---------------------------------------------------------------------------

def _secret_to_bytes(secret: str) -> bytes:
    """
    Convert an MTProto proxy secret string to bytes.

    Formats handled:
      • Plain hex:       "deadbeef..."      (any length divisible by 2)
      • 'dd' prefixed:   "dd<32 hex>"       (obfuscation flag)
      • 'ee' prefixed:   "ee<62+ hex>"      (fake-TLS, domain embedded)
      • Base64url:       "abc_XYZ-..."      (URL-safe, no padding needed)
    """
    s = secret.strip()

    # Try hex first (most common)
    if all(c in "0123456789abcdefABCDEF" for c in s) and len(s) % 2 == 0:
        return bytes.fromhex(s)

    # Try base64url (Telegram uses URL-safe base64, no padding)
    try:
        padded = s + "=" * (4 - len(s) % 4)
        return base64.urlsafe_b64decode(padded)
    except Exception:
        pass

    # Last resort: raw UTF-8 bytes
    return s.encode("utf-8")


# ---------------------------------------------------------------------------
# Build Telethon client for a given proxy
# ---------------------------------------------------------------------------

def _make_client(api_id: int, api_hash: str, proxy: dict) -> TelegramClient:
    """
    Create a TelegramClient configured to use the given proxy.
    The session file is reused so re-auth is not needed.

    MTProto proxies use Telethon's native ConnectionTcpMTProxyRandomizedIntermediate.
    SOCKS5/HTTP proxies use python-socks (preferred) or PySocks fallback.

    connection_retries=1 prevents Telethon from internally retrying a bad proxy
    multiple times, so we can cycle through proxies quickly.
    """
    ptype  = proxy["type"]
    host   = proxy["server"]
    port   = proxy["port"]

    _fast = dict(connection_retries=1, retry_delay=0)

    if ptype == "mtproto" and proxy.get("secret"):
        # Telethon's normalize_secret() expects a hex string and handles
        # the ee/dd prefix stripping and bytes conversion internally.
        return TelegramClient(
            SESSION_FILE, api_id, api_hash,
            connection=ConnectionTcpMTProxyRandomizedIntermediate,
            proxy=(host, port, proxy["secret"]),
            **_fast,
        )

    # SOCKS5 / SOCKS4 / HTTP — via python-socks or PySocks
    user = proxy.get("username") or None
    pwd  = proxy.get("password") or None

    if _SOCKS_VIA_PYTHON_SOCKS:
        # python-socks format: ("socks5", host, port) or with auth tuple
        scheme = {"socks5": "socks5", "socks4": "socks4", "http": "http"}.get(ptype, "socks5")
        if user:
            proxy_tuple = (scheme, host, port, user, pwd)
        else:
            proxy_tuple = (scheme, host, port)
        return TelegramClient(SESSION_FILE, api_id, api_hash, proxy=proxy_tuple, **_fast)

    elif _pysocks is not None:
        sock_type = {
            "socks5": _pysocks.SOCKS5,
            "socks4": _pysocks.SOCKS4,
            "http":   _pysocks.HTTP,
        }.get(ptype, _pysocks.SOCKS5)
        return TelegramClient(
            SESSION_FILE, api_id, api_hash,
            proxy=(sock_type, host, port, True, user, pwd),
            **_fast,
        )

    raise ValueError(
        f"Cannot use {ptype} proxy: install python-socks or PySocks"
    )


# ---------------------------------------------------------------------------
# Connectivity check
# ---------------------------------------------------------------------------

async def _ping(client: TelegramClient, timeout: float = 5.0) -> bool:
    """Return True if the Telegram session is alive and reachable."""
    try:
        await asyncio.wait_for(client.get_me(), timeout=timeout)
        return True
    except (asyncio.TimeoutError, errors.RPCError, ConnectionError, OSError):
        return False
    except Exception as exc:
        logger.debug("_ping unexpected error: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Connect through one proxy — returns live client or None
# ---------------------------------------------------------------------------

async def _try_connect(api_id: int, api_hash: str,
                       proxy: dict, connect_timeout: float = 6.0):
    """
    Attempt to connect through a single proxy (MTProto or SOCKS5/HTTP).
    Returns a connected TelegramClient or None.

    On success, records MTProto handshake latency in proxy['latency_ms']
    so the daemon can later prefer faster proxies.

    Uses asyncio.timeout() (Python 3.11+) for the hard deadline — this
    attaches directly to the running task and is more reliable than
    asyncio.wait_for() when Telethon's internal reconnect logic is active.
    connection_retries=1 prevents Telethon from internally retrying.
    """
    import time as _time
    client = None
    try:
        client = _make_client(api_id, api_hash, proxy)

        logger.info("Connecting via %s:%s [%s]…",
                    proxy["server"], proxy["port"], proxy["type"].upper())

        t0 = _time.monotonic()
        async with asyncio.timeout(connect_timeout):
            await client.connect()

        async with asyncio.timeout(4):
            if not await client.is_user_authorized():
                logger.warning("Session not authorized for %s:%s — skipping",
                               proxy["server"], proxy["port"])
                await client.disconnect()
                return None

        if await _ping(client):
            latency_ms = (_time.monotonic() - t0) * 1000.0
            proxy["latency_ms"] = latency_ms
            logger.info("Connected  via %s:%s [%s]  %.0f ms",
                        proxy["server"], proxy["port"],
                        proxy["type"].upper(), latency_ms)
            return client

        await client.disconnect()
        return None

    except (TimeoutError, asyncio.TimeoutError, ConnectionError, OSError):
        logger.debug("Timeout/connection error via %s:%s",
                     proxy["server"], proxy["port"])
    except RuntimeError as exc:
        # Telethon generator cleanup may raise "coroutine ignored GeneratorExit"
        # when asyncio.timeout() cancels the task.  Treat as a failed attempt.
        logger.debug("Generator error via %s:%s — %s",
                     proxy["server"], proxy["port"], exc)
    except Exception as exc:
        logger.debug("Error connecting via %s:%s — %s",
                     proxy["server"], proxy["port"], exc)

    if client:
        try:
            await client.disconnect()
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# Parallel proxy probe — tries up to batch_size proxies concurrently
# ---------------------------------------------------------------------------

async def _find_working_proxy(api_id: int, api_hash: str,
                              candidates: list,
                              connect_timeout: float = 6.0):
    """
    Find the fastest working proxy via parallel probing.

    Probes candidates in parallel batches of BATCH_SIZE.  Previously-verified
    proxies (mtproto_ok) are placed at the front of the queue.

    Latency-aware selection:
      When the first task in a batch succeeds, a short GRACE_WINDOW (0.5 s)
      is applied to allow other tasks in the same batch to also complete.
      If multiple proxies succeed during that window, the one with the lowest
      recorded latency_ms wins.  This avoids always picking the fastest TCP
      responder when a slightly slower one has much better MTProto throughput.

    Supports both MTProto (native) and SOCKS5/HTTP (via python-socks) proxies.

    Returns (connected_client, proxy_dict) or (None, None).
    """
    # Separate by type: MTProto first (native support), then SOCKS5/HTTP
    mtproto = [p for p in candidates
               if p.get("type") == "mtproto" and p.get("secret")]
    socks   = [p for p in candidates
               if p.get("type") in ("socks5", "socks4", "http")]

    # Verified proxies go first within each group
    def _sorted(lst):
        verified = [p for p in lst if p.get("mtproto_ok")]
        others   = [p for p in lst if not p.get("mtproto_ok")]
        return verified + others

    ordered = _sorted(mtproto) + _sorted(socks)

    BATCH_SIZE   = 5     # keep event-loop load manageable
    GRACE_WINDOW = 0.5   # seconds to wait after first success for faster rivals

    for start in range(0, len(ordered), BATCH_SIZE):
        batch = ordered[start:start + BATCH_SIZE]

        task_map: dict = {
            asyncio.create_task(_try_connect(api_id, api_hash, p, connect_timeout)): p
            for p in batch
        }
        pending  = set(task_map)
        winners: list[tuple] = []   # (client, proxy) pairs from this batch

        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:
                    result = task.result()
                except Exception:
                    result = None

                if isinstance(result, TelegramClient):
                    winners.append((result, task_map[task]))

            if winners and not pending:
                break

            if winners:
                # Grace window: wait a short moment for faster rivals to land
                try:
                    grace_done, pending = await asyncio.wait(
                        pending, timeout=GRACE_WINDOW,
                        return_when=asyncio.ALL_COMPLETED,
                    )
                    for task in grace_done:
                        try:
                            r = task.result()
                        except Exception:
                            r = None
                        if isinstance(r, TelegramClient):
                            winners.append((r, task_map[task]))
                except Exception:
                    pass
                break   # exit while loop — we have at least one winner

        if winners:
            # Pick the winner with the lowest recorded latency
            winners.sort(key=lambda t: t[1].get("latency_ms") or 9999.0)
            best_client, best_proxy = winners[0]

            # Disconnect all runners-up
            for cl, _ in winners[1:]:
                try:
                    await cl.disconnect()
                except Exception:
                    pass

            # Wait for still-running tasks (no cancel — Telethon + cancel = RuntimeError)
            if pending:
                rest = await asyncio.gather(*pending, return_exceptions=True)
                for r in rest:
                    if isinstance(r, TelegramClient):
                        try:
                            await r.disconnect()
                        except Exception:
                            pass

            return best_client, best_proxy

    return None, None


# ---------------------------------------------------------------------------
# Main daemon loop
# ---------------------------------------------------------------------------

async def run_daemon():
    # Silence Telethon's internal connection noise completely.
    # Our own logger (auto_proxy_daemon) is unaffected.
    logging.getLogger("telethon").setLevel(logging.CRITICAL)

    # Suppress all unhandled asyncio task exceptions.
    # Telethon's internal background tasks (_recv_loop, _reconnect, etc.)
    # raise benign errors during proxy probing that we handle in _try_connect.
    asyncio.get_running_loop().set_exception_handler(lambda loop, ctx: None)

    daemon_cfg    = _load_daemon_config()
    api_id        = int(daemon_cfg["api_id"])
    api_hash      = daemon_cfg["api_hash"].strip()

    if not api_id or not api_hash:
        print(
            "\n[!] api_id and/or api_hash not set in daemon_config.json\n"
            "    Get them from https://my.telegram.org → App Configuration\n"
        )
        sys.exit(1)

    interval      = daemon_cfg["check_interval_seconds"]
    reconnect_dly = daemon_cfg["reconnect_delay_seconds"]
    max_attempts  = daemon_cfg["max_rotate_attempts"]

    client        = None
    active_proxy  = None
    failure_count = 0

    print("\n" + "=" * 52)
    print("  Telegram Auto-Proxy Daemon")
    print("  Press Ctrl+C to stop")
    print("=" * 52 + "\n")

    # ----------------------------------------------------------------
    # First-time login — runs only if no session file exists
    # ----------------------------------------------------------------
    session_path = SESSION_FILE + ".session"

    # Trust the session file if it exists and is non-empty.
    # Connecting directly (no proxy) to verify auth would fail on restricted
    # networks and incorrectly trigger the first-time login flow.
    # Real auth validity is checked later via is_user_authorized() per proxy.
    session_ok = os.path.exists(session_path) and os.path.getsize(session_path) > 0
    if not session_ok:
        try:
            print("=" * 52)
            print("  FIRST-TIME LOGIN")
            print("=" * 52)
            print("\nNo saved session found. You need to log in once.")
            print("Your credentials are stored locally and never sent anywhere.\n")

            # Try direct login first (no proxy), then fall back to a proxy
            login_ok = False
            try:
                print("Attempting login without proxy...")
                tmp = TelegramClient(SESSION_FILE, api_id, api_hash)
                await tmp.start()       # interactive: asks phone + code + 2FA
                await tmp.disconnect()
                login_ok = True
                print("\n✓ Login successful. Session saved.\n")
            except Exception as exc:
                print(f"\nDirect login failed ({exc}).")
                print("Trying login through a proxy from the pool...\n")

            if not login_ok:
                # Fall back: try login through each alive proxy
                proxies_for_login = pm.load_proxies()
                alive_for_login   = pm.get_alive(proxies_for_login)
                if not alive_for_login:
                    # Quick TCP check to find any working proxy
                    candidates = proxies_for_login[:30]
                    await checker.check_all(candidates, timeout=10, concurrency=30)
                    pm.save_proxies(proxies_for_login)
                    alive_for_login = pm.get_alive(proxies_for_login)

                for px in alive_for_login[:5]:
                    try:
                        print(f"  Trying {px['server']}:{px['port']} [{px['type'].upper()}]...")
                        tmp = _make_client(api_id, api_hash, px)
                        await asyncio.wait_for(tmp.connect(), timeout=15)
                        await tmp.start()
                        await tmp.disconnect()
                        login_ok = True
                        print(f"\n✓ Login successful via {px['server']}:{px['port']}. Session saved.\n")
                        break
                    except Exception as exc2:
                        print(f"  Failed: {exc2}")
                        try:
                            await tmp.disconnect()
                        except Exception:
                            pass

            if not login_ok:
                print(
                    "\n[!] Could not complete login through any proxy.\n"
                    "    Make sure you have run /fetch and /check in the bot\n"
                    "    to populate the proxy pool, then try again.\n"
                )
                sys.exit(1)

        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\n\nLogin cancelled by user.")
            sys.exit(0)

    def _ts():
        return datetime.now().strftime("%H:%M:%S")

    try:
        while True:
            # ----------------------------------------------------------------
            # 1. Load proxy pool (shared with the bot)
            # ----------------------------------------------------------------
            proxies = pm.load_proxies()
            alive   = pm.get_alive(proxies)

            # ----------------------------------------------------------------
            # 2. No alive proxies → reset dead ones and run a TCP check
            # ----------------------------------------------------------------
            if not alive:
                logger.info("No confirmed alive proxies — running TCP check…")
                cfg         = pm.load_config()
                timeout_chk = cfg.get("check_timeout_seconds", 10)
                # Reset dead proxies so they get another TCP chance
                for p in proxies:
                    if p.get("alive") is False:
                        p["alive"] = None
                candidates = proxies  # re-check everything
                await checker.check_all(
                    candidates[:100], timeout=timeout_chk, concurrency=100
                )
                pm.save_proxies(proxies)
                alive = pm.get_alive(proxies)

                if not alive:
                    print(f"[{_ts()}] ⚠ No alive proxies after re-check. Retrying in {interval}s…")
                    await asyncio.sleep(interval)
                    continue

            # ----------------------------------------------------------------
            # 3. Check if current connection is still healthy
            # ----------------------------------------------------------------
            if client and client.is_connected():
                if await _ping(client):
                    logger.debug("[%s] ✅ %s:%s alive",
                                 _ts(),
                                 active_proxy["server"], active_proxy["port"])
                    failure_count = 0
                    await asyncio.sleep(interval)
                    continue

                # Connection lost
                print(f"\n[{_ts()}] ❌ Proxy died: "
                      f"{active_proxy['server']}:{active_proxy['port']}")
                active_proxy["alive"] = False
                active_proxy.pop("mtproto_ok", None)   # no longer verified
                pm.save_proxies(proxies)
                try:
                    await client.disconnect()
                except Exception:
                    pass
                client       = None
                active_proxy = None

            # ----------------------------------------------------------------
            # 4. Rotate — try verified proxies first, then probe the rest
            #    in small batches of 5 to avoid event-loop congestion.
            # ----------------------------------------------------------------
            verified_count = sum(1 for p in alive if p.get("mtproto_ok"))
            print(f"[{_ts()}] ↺ Probing {len(alive)} alive proxies "
                  f"({verified_count} previously verified)…")
            new_client, new_proxy = await _find_working_proxy(
                api_id, api_hash, alive, connect_timeout=6
            )

            def _activate(nc, np_):
                nonlocal client, active_proxy, failure_count
                client       = nc
                active_proxy = np_
                np_["mtproto_ok"] = True          # remember this proxy works
                pm.save_proxies(proxies)
                conn.set_active(np_)
                link = pm.proxy_to_tg_link(np_)
                print(f"\n[{_ts()}] 🔌 Active proxy → "
                      f"{np_['server']}:{np_['port']} "
                      f"[{np_['type'].upper()}]")
                print(f"         Link : {link}\n")
                failure_count = 0
                _apply_to_telegram_desktop(np_)

            if new_client:
                _activate(new_client, new_proxy)
            else:
                # All TCP-alive proxies failed MTProto — fetch fresh ones
                print(f"[{_ts()}] ↺ All alive proxies failed — fetching fresh proxies…")
                cfg      = pm.load_config()
                channels = cfg.get("source_channels", [])
                urls     = cfg.get("source_urls", [])
                try:
                    new_proxies = await fetcher.fetch_all(channels, urls)
                    before_len  = len(proxies)
                    for np_ in new_proxies:
                        pm.add_proxy(proxies, np_)
                    added = len(proxies) - before_len
                    if added:
                        fresh = proxies[before_len:]
                        await checker.check_all(
                            fresh, timeout=10, concurrency=50
                        )
                        pm.save_proxies(proxies)
                        fresh_alive = pm.get_alive(fresh)
                        logger.info("Fetched %d new, %d TCP-alive", added, len(fresh_alive))
                        nc, np2 = await _find_working_proxy(
                            api_id, api_hash, fresh_alive, connect_timeout=6
                        )
                        if nc:
                            _activate(nc, np2)
                except Exception as exc:
                    logger.warning("Fetch cycle failed: %s", exc)

            if not client:
                failure_count += 1
                print(f"[{_ts()}] ⚠ No working proxy found "
                      f"(attempt {failure_count}). Retrying in {interval}s…")
                conn.clear_active()
                await asyncio.sleep(interval)

    except KeyboardInterrupt:
        print("\n\nDaemon stopped by user.")
    finally:
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        print("Disconnected. Goodbye.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(run_daemon())

"""
connection.py — Active proxy connection state & auto-rotation engine.

Improvements:
  #2  All disk saves now use atomic writes (proxy_manager._atomic_write).
  #6  get_state() / is_monitoring() keep an in-process cache so the common
      "is monitoring active?" hot-path never hits the disk.
  #24 Respects proxy_manager.DATA_DIR for the connection.json location.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import proxy_manager as pm

logger = logging.getLogger(__name__)

CONNECTION_FILE = os.path.join(pm.DATA_DIR, "connection.json")

_EMPTY_STATE: dict = {
    "active_proxy": None,
    "connected_at": None,
    "rotations":    0,
    "last_rotated": None,
    "monitoring":   False,
}

# ---------------------------------------------------------------------------
# #6 — In-process state cache (avoid disk reads on every is_monitoring() call)
# ---------------------------------------------------------------------------

_state_cache: dict | None = None   # None means "not yet loaded"


def _load() -> dict:
    global _state_cache
    if _state_cache is not None:
        return _state_cache
    if not os.path.exists(CONNECTION_FILE):
        _state_cache = _EMPTY_STATE.copy()
        return _state_cache
    try:
        with open(CONNECTION_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for k, v in _EMPTY_STATE.items():
            data.setdefault(k, v)
        _state_cache = data
        return _state_cache
    except Exception:
        _state_cache = _EMPTY_STATE.copy()
        return _state_cache


def _save(state: dict) -> None:
    """Atomic write so a crash never leaves a partial file (#2)."""
    global _state_cache
    _state_cache = state  # update cache before writing
    pm._atomic_write(CONNECTION_FILE, json.dumps(state, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_state() -> dict:
    return dict(_load())   # return a shallow copy so callers can't mutate cache


def get_active() -> dict | None:
    return _load().get("active_proxy")


def set_active(proxy: dict) -> None:
    """Mark a proxy as active and start monitoring flag."""
    state = _load()
    now   = datetime.now(timezone.utc).isoformat()

    is_rotation         = state.get("active_proxy") is not None
    state["active_proxy"] = proxy
    state["connected_at"] = now
    state["monitoring"]   = True
    if is_rotation:
        state["rotations"]    = state.get("rotations", 0) + 1
        state["last_rotated"] = now
    _save(state)


def clear_active() -> None:
    state = _load()
    state["active_proxy"] = None
    state["monitoring"]   = False
    _save(state)


def set_monitoring(enabled: bool) -> None:
    state = _load()
    state["monitoring"] = enabled
    _save(state)


def is_monitoring() -> bool:
    return bool(_load().get("monitoring", False))


# ---------------------------------------------------------------------------
# Rotation logic
# ---------------------------------------------------------------------------

def pick_best(proxies: list) -> dict | None:
    """
    Pick the best available proxy for connection.

    Priority:
      1. Alive proxies outside the 5-minute rotation cooldown window,
         sorted best-first by latency+uptime score.
      2. If all alive proxies are in cooldown, fall back to any alive proxy.
      3. Unchecked proxies (alive == None).
      4. Nothing → None.
    """
    from datetime import timedelta

    now      = datetime.now(timezone.utc)
    cooldown = timedelta(minutes=5)

    def _not_in_cooldown(p: dict) -> bool:
        rfa = p.get("recently_failed_at")
        if not rfa:
            return True
        try:
            dt = datetime.fromisoformat(rfa)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return (now - dt) > cooldown
        except Exception:
            return True

    alive = [p for p in proxies if p.get("alive") is True]
    if alive:
        cooled = [p for p in alive if _not_in_cooldown(p)]
        pool   = cooled if cooled else alive
        return pm.sort_by_score(pool)[0]

    unk = [p for p in proxies if p.get("alive") is None]
    return unk[0] if unk else None


async def auto_rotate(proxies: list, channels: list, timeout: float,
                      urls: list | None = None,
                      notify_fn=None) -> dict | None:
    """
    Full autonomous rotation pipeline. Returns the new active proxy or None.

    Uses async_save_proxies (#2) instead of blocking save_proxies.
    """
    import checker
    import fetcher

    async def _notify(msg: str) -> None:
        if notify_fn:
            try:
                await notify_fn(msg)
            except Exception as exc:
                logger.warning("notify_fn error: %s", exc)

    # Step 1: try existing alive proxies
    candidate = pick_best(proxies)
    if candidate:
        set_active(candidate)
        logger.info("auto_rotate: switched to existing alive proxy %s:%s",
                    candidate["server"], candidate["port"])
        return candidate

    # Step 2: re-check all
    await _notify("⚠️ No live proxy available — re-checking all proxies…")
    if proxies:
        await checker.check_all(proxies, timeout=timeout)
        await pm.async_save_proxies(proxies)   # #2 — non-blocking

        removed = pm.remove_blocked(proxies)
        if removed:
            await pm.async_save_proxies(proxies)

        candidate = pick_best(proxies)
        if candidate:
            set_active(candidate)
            logger.info("auto_rotate: found alive proxy after re-check: %s:%s",
                        candidate["server"], candidate["port"])
            return candidate

    # Step 3: fetch from all sources
    if channels or urls:
        await _notify("📡 Fetching fresh proxies from all sources…")
        fresh = await fetcher.fetch_all(channels or [], urls or [])
        added = 0
        for p in fresh:
            if pm.add_proxy(proxies, p):
                added += 1
        if added:
            await pm.async_save_proxies(proxies)
            await _notify(f"📥 Fetched {added} new proxy(ies) — checking…")
            new_ones = [p for p in proxies if p.get("alive") is None]
            if new_ones:
                await checker.check_all(new_ones, timeout=timeout)
                await pm.async_save_proxies(proxies)

            candidate = pick_best(proxies)
            if candidate:
                set_active(candidate)
                logger.info("auto_rotate: connected to freshly fetched proxy %s:%s",
                            candidate["server"], candidate["port"])
                return candidate

    # Step 4: nothing works
    logger.warning("auto_rotate: exhausted all options — no proxy available")
    clear_active()
    return None

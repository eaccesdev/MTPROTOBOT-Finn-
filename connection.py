"""
connection.py — Active proxy connection state & auto-rotation engine.

The "active proxy" is the one currently designated for use.
State is persisted in connection.json so it survives restarts.

Auto-rotation pipeline (triggered when active proxy dies):
  1. Scan existing proxies → pick first alive one.
  2. If none alive → run a full connectivity check on all proxies.
  3. If still none → fetch new proxies from source channels → check those.
  4. If something was found → set it as active → notify admin.
  5. If nothing at all → notify admin: manual intervention required.
"""

import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

CONNECTION_FILE = "connection.json"

_EMPTY_STATE = {
    "active_proxy": None,
    "connected_at": None,
    "rotations": 0,
    "last_rotated": None,
    "monitoring": False,
}


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _load() -> dict:
    if not os.path.exists(CONNECTION_FILE):
        return _EMPTY_STATE.copy()
    try:
        with open(CONNECTION_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # back-fill missing keys
        for k, v in _EMPTY_STATE.items():
            data.setdefault(k, v)
        return data
    except Exception:
        return _EMPTY_STATE.copy()


def _save(state: dict):
    with open(CONNECTION_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_state() -> dict:
    return _load()


def get_active() -> dict | None:
    """Return the currently active proxy dict, or None."""
    return _load().get("active_proxy")


def set_active(proxy: dict):
    """Mark a proxy as active and start monitoring flag."""
    state = _load()
    now   = datetime.now(timezone.utc).isoformat()

    is_rotation = state.get("active_proxy") is not None
    state["active_proxy"] = proxy
    state["connected_at"] = now
    state["monitoring"]   = True
    if is_rotation:
        state["rotations"]     = state.get("rotations", 0) + 1
        state["last_rotated"]  = now
    _save(state)


def clear_active():
    """Stop monitoring / disconnect."""
    state = _load()
    state["active_proxy"] = None
    state["monitoring"]   = False
    _save(state)


def set_monitoring(enabled: bool):
    state = _load()
    state["monitoring"] = enabled
    _save(state)


def is_monitoring() -> bool:
    return _load().get("monitoring", False)


# ---------------------------------------------------------------------------
# Rotation logic
# ---------------------------------------------------------------------------

def pick_best(proxies: list) -> dict | None:
    """
    Pick the best available proxy for connection.

    Priority:
      1. Alive proxies that are NOT in the 5-minute rotation cooldown window,
         sorted best-first by latency+uptime score.
      2. If all alive proxies are in cooldown, fall back to any alive proxy
         (cooldown is advisory, not hard).
      3. Unchecked proxies (alive == None)  — will be validated before use.
      4. Nothing → None.

    Rotation cooldown: a proxy gets `recently_failed_at` stamped when it is
    removed as the active proxy due to failure.  Skipping it for 5 minutes
    prevents the bot from immediately rotating back onto a flaky proxy.
    """
    import proxy_manager as pm
    from datetime import datetime, timezone, timedelta

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
        pool   = cooled if cooled else alive   # fallback: ignore cooldown if all expired
        return pm.sort_by_score(pool)[0]

    unk = [p for p in proxies if p.get("alive") is None]
    return unk[0] if unk else None


async def auto_rotate(proxies: list, channels: list, timeout: float,
                      urls: list | None = None,
                      notify_fn=None) -> dict | None:
    """
    Full autonomous rotation pipeline. Returns the new active proxy or None.

    notify_fn(message: str) — async callable to send a status update to the admin.
    urls                    — additional external URL sources beyond channels.
    """
    import checker
    import fetcher
    import proxy_manager as pm

    async def _notify(msg):
        if notify_fn:
            try:
                await notify_fn(msg)
            except Exception as exc:
                logger.warning("notify_fn error: %s", exc)

    # Step 1: try existing alive proxies (best-first by score)
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
        pm.save_proxies(proxies)

        removed = pm.remove_blocked(proxies)
        if removed:
            pm.save_proxies(proxies)

        candidate = pick_best(proxies)
        if candidate:
            set_active(candidate)
            logger.info("auto_rotate: found alive proxy after re-check: %s:%s",
                        candidate["server"], candidate["port"])
            return candidate

    # Step 3: fetch from all sources (channels + URLs)
    if channels or urls:
        await _notify("📡 Fetching fresh proxies from all sources…")
        fresh = await fetcher.fetch_all(channels or [], urls or [])
        added = 0
        for p in fresh:
            if pm.add_proxy(proxies, p):
                added += 1
        if added:
            pm.save_proxies(proxies)
            await _notify(f"📥 Fetched {added} new proxy(ies) — checking…")
            new_ones = [p for p in proxies if p.get("alive") is None]
            if new_ones:
                await checker.check_all(new_ones, timeout=timeout)
                pm.save_proxies(proxies)

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

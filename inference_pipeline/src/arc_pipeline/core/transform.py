from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y", "active", "current"}
    return False


def extract_sessions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    sessionlist = payload.get("sessions")
    if sessionlist is None:
        sessionlist = payload.get("sessionlist", {})
    sessions: list[dict[str, Any]] = []
    stack: list[Any] = [sessionlist]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            if "session_id" in item and isinstance(item.get("session_id"), (int, str)):
                sessions.append(item)
            for value in item.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(item, list):
            for value in item:
                if isinstance(value, (dict, list)):
                    stack.append(value)
    return sessions


def active_sessions(sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    now_year = datetime.now(timezone.utc).year
    active: list[dict[str, Any]] = []
    for session in sessions:
        session_id = session.get("session_id")
        if not isinstance(session_id, (int, str)):
            continue
        prior = as_int(session.get("prior", 1), 1)
        sine_die = as_int(session.get("sine_die", 1), 1)
        year_end = as_int(session.get("year_end", 0), 0)
        active_by_state = prior == 0 and sine_die == 0 and year_end >= (now_year - 1)
        if active_by_state or any(
            is_truthy(session.get(flag)) for flag in ("active", "current", "is_current", "is_active")
        ):
            active.append(session)
    return sorted(active, key=lambda value: as_int(value.get("session_id")))

from __future__ import annotations

import json
from pathlib import Path

from arc_pipeline.core.transform import active_sessions, extract_sessions


def test_extract_sessions_reads_nested_payload() -> None:
    fixture_path = Path(__file__).parent / "fixtures" / "session_payload.json"
    payload = json.loads(fixture_path.read_text())

    sessions = extract_sessions(payload)

    assert len(sessions) == 2
    assert {int(s["session_id"]) for s in sessions} == {88, 101}


def test_active_sessions_filters_to_current_legislative_sessions() -> None:
    fixture_path = Path(__file__).parent / "fixtures" / "session_payload.json"
    payload = json.loads(fixture_path.read_text())

    sessions = extract_sessions(payload)
    active = active_sessions(sessions)

    assert [int(s["session_id"]) for s in active] == [101]


def test_active_sessions_accepts_explicit_active_flags() -> None:
    sessions = [
        {"session_id": "9", "prior": 1, "sine_die": 1, "year_end": 2000, "is_current": "true"},
        {"session_id": "11", "prior": 1, "sine_die": 1, "year_end": 2000, "active": "0"},
    ]

    active = active_sessions(sessions)

    assert [int(s["session_id"]) for s in active] == [9]


from __future__ import annotations

from arc_pipeline.definitions import defs, arc_weekly_schedule


def test_definitions_load() -> None:
    assert defs is not None
    assert arc_weekly_schedule.execution_timezone == "UTC"


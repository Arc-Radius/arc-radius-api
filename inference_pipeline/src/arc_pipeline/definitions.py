from __future__ import annotations

from dagster import Definitions, ScheduleDefinition, define_asset_job

from arc_pipeline.assets.arc_assets import (
    detect_changed_sessions,
    fetch_changed_datasets,
    fetch_sessions_and_dataset_list,
    arc_run_params,
    update_session_state,
)
from arc_pipeline.resources.aws_resources import AwsClientsResource
from arc_pipeline.resources.legiscan_resource import LegiScanResource


arc_assets = [
    arc_run_params,
    fetch_sessions_and_dataset_list,
    detect_changed_sessions,
    fetch_changed_datasets,
    update_session_state,
]

arc_job = define_asset_job(name="arc_weekly_job", selection=arc_assets)

arc_weekly_schedule = ScheduleDefinition(
    job=arc_job,
    cron_schedule="0 9 * * 1",
    execution_timezone="UTC",
    run_config={
        "ops": {
            "arc_run_params": {
                "config": {},
            }
        }
    },
)

defs = Definitions(
    assets=arc_assets,
    jobs=[arc_job],
    schedules=[arc_weekly_schedule],
    resources={
        "aws": AwsClientsResource(),
        "legiscan": LegiScanResource(),
    },
)


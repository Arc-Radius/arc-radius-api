from typing import Any

from dagster import AssetExecutionContext, RetryPolicy, asset

from arc_pipeline.config import ArcRunConfig
from arc_pipeline.core.io import put_json, run_prefix, s3_key
from arc_pipeline.core.state import (
    batch_get_previous_session_hashes,
    put_session_state,
    put_watermark,
)
from arc_pipeline.core.transform import (
    active_sessions,
    as_int,
    extract_sessions,
    now_iso,
)
from arc_pipeline.resources.aws_resources import AwsClientsResource
from arc_pipeline.resources.legiscan_resource import LegiScanResource


@asset
def arc_run_params(context: AssetExecutionContext, config: ArcRunConfig) -> dict[str, Any]:
    run_id = context.run.run_id
    run_started_at = now_iso()
    artifact_prefix = run_prefix(run_started_at, run_id)
    return {
        "pipeline_name": config.pipeline_name,
        "run_id": run_id,
        "run_started_at": run_started_at,
        "artifact_bucket": config.artifact_bucket,
        "artifact_root_prefix": config.artifact_root_prefix,
        "artifact_prefix": artifact_prefix,
        "session_state_table": config.session_state_table,
        "watermark_table": config.watermark_table,
        "force_refresh": config.force_refresh,
        "max_api_calls_per_run": config.max_api_calls_per_run,
    }


@asset(retry_policy=RetryPolicy(max_retries=3, delay=2))
def fetch_sessions_and_dataset_list(
    context: AssetExecutionContext,
    arc_run_params: dict[str, Any],
    legiscan: LegiScanResource,
    aws: AwsClientsResource,
) -> dict[str, Any]:
    if not arc_run_params["artifact_bucket"]:
        raise ValueError("artifact_bucket is required")

    legiscan_client = legiscan.client()
    sessions_payload = legiscan_client.get_session_list()
    dataset_list_payload = legiscan_client.get_dataset_list()

    sessions = extract_sessions(sessions_payload)
    active = active_sessions(sessions)
    active_session_ids = [as_int(session.get("session_id")) for session in active if as_int(session.get("session_id")) > 0]

    dataset_lookup: dict[int, dict[str, Any]] = {}
    for row in dataset_list_payload.get("datasetlist", []):
        if not isinstance(row, dict):
            continue
        session_id = as_int(row.get("session_id"))
        if session_id > 0:
            dataset_lookup[session_id] = row

    s3 = aws.s3_client()
    put_json(
        s3,
        arc_run_params["artifact_bucket"],
        s3_key(
            arc_run_params["artifact_root_prefix"],
            "raw/legiscan",
            arc_run_params["artifact_prefix"],
            "sessions.json",
        ),
        sessions_payload,
    )
    put_json(
        s3,
        arc_run_params["artifact_bucket"],
        s3_key(
            arc_run_params["artifact_root_prefix"],
            "raw/legiscan",
            arc_run_params["artifact_prefix"],
            "dataset_list.json",
        ),
        dataset_list_payload,
    )

    context.add_output_metadata({"active_session_count": len(active_session_ids)})
    return {
        "sessions_payload": sessions_payload,
        "dataset_list_payload": dataset_list_payload,
        "active_sessions": active,
        "active_session_ids": active_session_ids,
        "dataset_lookup": dataset_lookup,
    }


@asset(retry_policy=RetryPolicy(max_retries=3, delay=2))
def detect_changed_sessions(
    context: AssetExecutionContext,
    aws: AwsClientsResource,
    fetch_sessions_and_dataset_list: dict[str, Any],
    arc_run_params: dict[str, Any],
) -> dict[str, Any]:
    dynamodb_client = aws.dynamodb_client()
    previous_hashes = batch_get_previous_session_hashes(
        dynamodb_client,
        arc_run_params["session_state_table"],
        fetch_sessions_and_dataset_list["active_session_ids"],
    )

    changed_sessions: list[dict[str, Any]] = []
    for session in fetch_sessions_and_dataset_list["active_sessions"]:
        session_id = as_int(session.get("session_id"))
        if session_id <= 0:
            continue
        dataset_meta = fetch_sessions_and_dataset_list["dataset_lookup"].get(session_id, {})
        dataset_hash = str(dataset_meta.get("dataset_hash") or session.get("dataset_hash") or "")
        current_state = {
            "session_id": session_id,
            "state": str(session.get("state") or dataset_meta.get("state") or "").upper(),
            "dataset_hash": dataset_hash,
            "dataset_date": str(dataset_meta.get("dataset_date", "")),
            "access_key": str(dataset_meta.get("access_key", "")),
        }
        if arc_run_params["force_refresh"] or previous_hashes.get(session_id) != dataset_hash:
            changed_sessions.append(current_state)

    downloadable_sessions = [row for row in changed_sessions if str(row.get("access_key", "")).strip()]
    projected_api_calls = 2 + len(downloadable_sessions)
    max_calls = arc_run_params["max_api_calls_per_run"]
    if max_calls > 0 and projected_api_calls > max_calls:
        raise ValueError(f"Projected API calls {projected_api_calls} exceed max_api_calls_per_run {max_calls}")

    context.add_output_metadata(
        {
            "changed_session_count": len(changed_sessions),
            "downloadable_session_count": len(downloadable_sessions),
            "projected_api_calls": projected_api_calls,
        }
    )
    return {
        "changed_sessions": changed_sessions,
        "downloadable_sessions": downloadable_sessions,
        "projected_api_calls": projected_api_calls,
    }


@asset(retry_policy=RetryPolicy(max_retries=3, delay=2))
def fetch_changed_datasets(
    context: AssetExecutionContext,
    legiscan: LegiScanResource,
    aws: AwsClientsResource,
    arc_run_params: dict[str, Any],
    detect_changed_sessions: dict[str, Any],
) -> dict[str, Any]:
    legiscan_client = legiscan.client()
    s3 = aws.s3_client()
    downloaded_sessions: list[dict[str, Any]] = []

    for session in detect_changed_sessions["downloadable_sessions"]:
        session_id = int(session["session_id"])
        access_key = str(session.get("access_key", ""))
        dataset_payload = legiscan_client.get_dataset(session_id=session_id, access_key=access_key, format="csv")
        zip_bytes = legiscan_client.decode_dataset_zip(dataset_payload)

        dataset_json_key = s3_key(
            arc_run_params["artifact_root_prefix"],
            "raw/legiscan",
            arc_run_params["artifact_prefix"],
            f"dataset_{session_id}.json",
        )
        dataset_zip_key = s3_key(
            arc_run_params["artifact_root_prefix"],
            "raw/legiscan",
            arc_run_params["artifact_prefix"],
            f"dataset_{session_id}.zip",
        )

        downloaded_sessions.append(
            {
                "session_id": session_id,
                "state": str(session.get("state", "")),
                "dataset_hash": str(session.get("dataset_hash", "")),
                "dataset_date": str(session.get("dataset_date", "")),
                "dataset_json_key": dataset_json_key,
                "dataset_zip_key": dataset_zip_key,
            }
        )

        put_json(
            s3,
            arc_run_params["artifact_bucket"],
            dataset_json_key,
            dataset_payload.get("dataset", {}),
        )
        s3.put_object(
            Bucket=arc_run_params["artifact_bucket"],
            Key=dataset_zip_key,
            Body=zip_bytes,
            ContentType="application/zip",
        )

    context.add_output_metadata({"downloaded_session_count": len(downloaded_sessions)})
    return {"downloaded_sessions": downloaded_sessions}


@asset(retry_policy=RetryPolicy(max_retries=3, delay=2))
def update_session_state(
    context: AssetExecutionContext,
    aws: AwsClientsResource,
    arc_run_params: dict[str, Any],
    fetch_changed_datasets: dict[str, Any],
) -> dict[str, Any]:
    dynamodb_resource = aws.dynamodb_resource()
    put_session_state(
        dynamo_resource=dynamodb_resource,
        table_name=arc_run_params["session_state_table"],
        run_id=arc_run_params["run_id"],
        as_of=arc_run_params["run_started_at"],
        rows=fetch_changed_datasets["downloaded_sessions"],
    )
    put_watermark(
        dynamo_resource=dynamodb_resource,
        table_name=arc_run_params["watermark_table"],
        pipeline_name=arc_run_params["pipeline_name"],
        run_id=arc_run_params["run_id"],
        as_of=arc_run_params["run_started_at"],
        updated_at=now_iso(),
    )
    context.add_output_metadata({"updated_session_count": len(fetch_changed_datasets["downloaded_sessions"])})
    return {"updated": True}


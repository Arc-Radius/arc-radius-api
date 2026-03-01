from __future__ import annotations

from typing import Any


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def batch_get_previous_session_hashes(dynamo_client: Any, table_name: str, session_ids: list[int]) -> dict[int, str]:
    if not session_ids:
        return {}

    output: dict[int, str] = {}
    for i in range(0, len(session_ids), 100):
        batch = sorted(set(session_ids[i : i + 100]))
        keys = [{"session_id": {"N": str(session_id)}} for session_id in batch]
        response = dynamo_client.batch_get_item(
            RequestItems={
                table_name: {
                    "Keys": keys,
                    "ProjectionExpression": "session_id,last_dataset_hash",
                }
            }
        )
        rows = response.get("Responses", {}).get(table_name, [])
        for row in rows:
            session_id = as_int(row.get("session_id", {}).get("N"))
            last_hash = row.get("last_dataset_hash", {}).get("S")
            if session_id > 0 and isinstance(last_hash, str):
                output[session_id] = last_hash
    return output


def put_session_state(
    dynamo_resource: Any,
    table_name: str,
    run_id: str,
    as_of: str,
    rows: list[dict[str, Any]],
) -> None:
    table = dynamo_resource.Table(table_name)
    with table.batch_writer(overwrite_by_pkeys=["session_id"]) as batch:
        for row in rows:
            batch.put_item(
                Item={
                    "session_id": int(row["session_id"]),
                    "last_dataset_hash": str(row.get("dataset_hash", "")),
                    "last_dataset_ingested_at": as_of,
                    "last_run_id": run_id,
                    "state": str(row.get("state", "")),
                    "dataset_date": str(row.get("dataset_date", "")),
                }
            )


def put_watermark(
    dynamo_resource: Any,
    table_name: str,
    pipeline_name: str,
    run_id: str,
    as_of: str,
    updated_at: str,
) -> None:
    table = dynamo_resource.Table(table_name)
    table.put_item(
        Item={
            "pipeline_name": pipeline_name,
            "last_successful_run_at": as_of,
            "last_run_id": run_id,
            "updated_at": updated_at,
        }
    )


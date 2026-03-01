from __future__ import annotations

import json
from typing import Any


def run_prefix(run_started_at: str, run_id: str) -> str:
    return f"date={run_started_at[:10]}/run_id={run_id}"


def s3_key(root_prefix: str, section: str, run_path: str, filename: str) -> str:
    root = root_prefix.strip("/")
    return f"{root}/{section.strip('/')}/{run_path}/{filename.lstrip('/')}"


def put_json(s3_client: Any, bucket: str, key: str, payload: Any) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
        ContentType="application/json",
    )


def put_jsonl(s3_client: Any, bucket: str, key: str, rows: list[dict[str, Any]]) -> None:
    body = "\n".join(json.dumps(row, separators=(",", ":"), default=str) for row in rows).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/jsonl")


from __future__ import annotations

from io import BytesIO
from typing import Any

from dagster import materialize

from arc_pipeline.assets.arc_assets import (
    detect_changed_sessions,
    fetch_changed_datasets,
    fetch_sessions_and_dataset_list,
    arc_run_params,
    update_session_state,
)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: list[dict[str, Any]] = []
        self.by_key: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs: Any) -> None:
        self.objects.append(kwargs)
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        body = kwargs.get("Body", b"")
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.by_key[(bucket, key)] = body

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        body = self.by_key[(bucket, key)]
        return {"Body": BytesIO(body)}


class FakeDynamoClient:
    def batch_get_item(self, **_: Any) -> dict[str, Any]:
        return {"Responses": {"session_state": []}}


class FakeBatchWriter:
    def __init__(self, table: "FakeTable") -> None:
        self.table = table

    def __enter__(self) -> "FakeBatchWriter":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def put_item(self, Item: dict[str, Any]) -> None:  # noqa: N803
        self.table.items.append(Item)


class FakeTable:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def batch_writer(self, overwrite_by_pkeys: list[str] | None = None) -> FakeBatchWriter:
        _ = overwrite_by_pkeys
        return FakeBatchWriter(self)

    def put_item(self, Item: dict[str, Any]) -> None:  # noqa: N803
        self.items.append(Item)


class FakeDynamoResource:
    def __init__(self) -> None:
        self.tables: dict[str, FakeTable] = {}

    def Table(self, name: str) -> FakeTable:  # noqa: N802
        if name not in self.tables:
            self.tables[name] = FakeTable()
        return self.tables[name]


class FakeAws:
    def __init__(self) -> None:
        self.s3 = FakeS3Client()
        self.ddb_client = FakeDynamoClient()
        self.ddb_resource = FakeDynamoResource()

    def s3_client(self) -> FakeS3Client:
        return self.s3

    def dynamodb_client(self) -> FakeDynamoClient:
        return self.ddb_client

    def dynamodb_resource(self) -> FakeDynamoResource:
        return self.ddb_resource


class FakeLegiScanClient:
    def get_session_list(self) -> dict[str, Any]:
        return {
            "status": "OK",
            "sessions": {
                "state": "CA",
                "session": [{"session_id": 101, "state": "CA", "prior": 0, "sine_die": 0, "year_end": 2030}],
            },
        }

    def get_dataset_list(self) -> dict[str, Any]:
        return {
            "status": "OK",
            "datasetlist": [
                {
                    "session_id": 101,
                    "state": "CA",
                    "dataset_hash": "hash-a",
                    "dataset_date": "2026-02-01",
                    "access_key": "ak-101",
                }
            ],
        }

    def get_dataset(self, session_id: int, access_key: str, format: str = "csv") -> dict[str, Any]:
        _ = session_id, access_key, format
        return {"status": "OK", "dataset": {"zip": "ZmFrZQ=="}}

    @staticmethod
    def decode_dataset_zip(dataset_payload: dict[str, Any]) -> bytes:
        _ = dataset_payload
        return b"fake-zip-bytes"

class FakeLegiScanResource:
    def client(self) -> FakeLegiScanClient:
        return FakeLegiScanClient()


def test_arc_assets_write_inference_prefix_and_update_state() -> None:
    fake_aws = FakeAws()

    result = materialize(
        [
            arc_run_params,
            fetch_sessions_and_dataset_list,
            detect_changed_sessions,
            fetch_changed_datasets,
            update_session_state,
        ],
        resources={"aws": fake_aws, "legiscan": FakeLegiScanResource()},
        run_config={
            "ops": {
                "arc_run_params": {
                    "config": {
                        "artifact_bucket": "unit-test-bucket",
                        "artifact_root_prefix": "inference",
                    }
                }
            }
        },
    )

    assert result.success
    keys = [obj["Key"] for obj in fake_aws.s3.objects]
    assert keys
    assert all(key.startswith("inference/") for key in keys)
    assert any(key.endswith("/sessions.json") for key in keys)
    assert any(key.endswith("/dataset_list.json") for key in keys)
    assert any(key.endswith("/dataset_101.zip") for key in keys)
    assert any(key.endswith("/dataset_101.json") for key in keys)

    session_table_items = fake_aws.ddb_resource.Table("session_state").items
    watermark_items = fake_aws.ddb_resource.Table("pipeline_watermark").items
    assert len(session_table_items) == 1
    assert len(watermark_items) == 1


def test_missing_bucket_fails() -> None:
    fake_aws = FakeAws()
    result = materialize(
        [arc_run_params, fetch_sessions_and_dataset_list],
        resources={"aws": fake_aws, "legiscan": FakeLegiScanResource()},
        run_config={"ops": {"arc_run_params": {"config": {"artifact_bucket": ""}}}},
        raise_on_error=False,
    )
    assert not result.success


def test_api_call_limit_breach_fails() -> None:
    fake_aws = FakeAws()
    result = materialize(
        [arc_run_params, fetch_sessions_and_dataset_list, detect_changed_sessions],
        resources={"aws": fake_aws, "legiscan": FakeLegiScanResource()},
        run_config={
            "ops": {
                "arc_run_params": {
                    "config": {
                        "artifact_bucket": "unit-test-bucket",
                        "max_api_calls_per_run": 1,
                    }
                }
            }
        },
        raise_on_error=False,
    )
    assert not result.success


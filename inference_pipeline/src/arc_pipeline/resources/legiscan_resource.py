from __future__ import annotations

import base64
import csv
import json
import os
import random
import time
import zipfile
from io import BytesIO, StringIO
from typing import Any

from dagster import ConfigurableResource
import requests


class LegiScanClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout_seconds: int = 30,
        max_retries: int = 4,
    ) -> None:
        self.api_key = api_key or os.environ["LEGISCAN_API_KEY"]
        self.base_url = base_url or os.environ.get("LEGISCAN_BASE_URL", "https://api.legiscan.com/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries

    @staticmethod
    def _is_permanent_http_status(status_code: int) -> bool:
        # 4xx errors (except 429) are usually permanent request/auth issues.
        return 400 <= status_code < 500 and status_code != 429

    def request(self, op: str, **params: Any) -> dict[str, Any]:
        query = {"key": self.api_key, "op": op, **params}
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.get(self.base_url, params=query, timeout=self.timeout_seconds)
                response.raise_for_status()
                data = response.json()
                if data.get("status") != "OK":
                    # API-level ERROR responses still consume query quota; fail fast.
                    alert = data.get("alert", {})
                    message = alert.get("message") if isinstance(alert, dict) else None
                    details = f" ({message})" if isinstance(message, str) and message else ""
                    raise ValueError(f"LegiScan non-OK response{details}: {data}")
                return data
            except ValueError:
                raise
            except requests.RequestException as exc:
                if isinstance(exc, requests.HTTPError) and exc.response is not None:
                    status_code = exc.response.status_code
                    if self._is_permanent_http_status(status_code):
                        raise ValueError(f"LegiScan request failed with permanent HTTP {status_code}") from exc
                if attempt >= self.max_retries:
                    raise
                time.sleep((2 ** attempt) + random.uniform(0.0, 0.5))

    def get_session_list(self) -> dict[str, Any]:
        return self.request("getSessionList")

    def get_dataset_list(self, state: str | None = None, year: int | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if state:
            params["state"] = state
        if year:
            params["year"] = int(year)
        return self.request("getDatasetList", **params)

    def get_dataset(self, session_id: int, access_key: str, format: str = "json") -> dict[str, Any]:
        return self.request("getDataset", id=int(session_id), access_key=access_key, format=format)

    def get_dataset_raw(self, session_id: int, access_key: str, format: str = "json") -> bytes:
        query = {
            "key": self.api_key,
            "op": "getDatasetRaw",
            "id": int(session_id),
            "access_key": access_key,
            "format": format,
        }
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.get(self.base_url, params=query, timeout=self.timeout_seconds)
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                if "application/json" in content_type.lower():
                    # Raw endpoint may return JSON error payload; fail fast to preserve quota.
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = {"raw": response.text}
                    raise ValueError(f"LegiScan getDatasetRaw returned JSON payload: {payload}")
                return response.content
            except requests.HTTPError as exc:
                if attempt >= self.max_retries:
                    status_code = exc.response.status_code if exc.response is not None else "unknown"
                    raise ValueError(f"LegiScan raw request failed with HTTP {status_code}") from exc
                if exc.response is not None and self._is_permanent_http_status(exc.response.status_code):
                    raise ValueError(
                        f"LegiScan raw request failed with permanent HTTP {exc.response.status_code}"
                    ) from exc
                time.sleep((2 ** attempt) + random.uniform(0.0, 0.5))
            except requests.RequestException:
                if attempt >= self.max_retries:
                    raise
                time.sleep((2 ** attempt) + random.uniform(0.0, 0.5))

    @staticmethod
    def decode_dataset_zip(dataset_payload: dict[str, Any]) -> bytes:
        dataset = dataset_payload.get("dataset", {})
        zip_b64 = dataset.get("zip")
        if not isinstance(zip_b64, str) or not zip_b64:
            raise ValueError("dataset.zip is missing from getDataset response")
        return base64.b64decode(zip_b64)

    @staticmethod
    def extract_dataset_archive(zip_bytes: bytes) -> dict[str, list[dict[str, Any]]]:
        extracted: dict[str, list[dict[str, Any]]] = {}
        with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
            for member in archive.namelist():
                if member.endswith("/"):
                    continue
                basename = member.rsplit("/", 1)[-1]
                with archive.open(member) as fh:
                    raw = fh.read()
                if basename.endswith(".json"):
                    parsed = json.loads(raw.decode("utf-8"))
                    rows = parsed if isinstance(parsed, list) else [parsed]
                    extracted[basename] = [row for row in rows if isinstance(row, dict)]
                elif basename.endswith(".csv"):
                    text = raw.decode("utf-8")
                    reader = csv.DictReader(StringIO(text))
                    extracted[basename] = [dict(row) for row in reader]
        return extracted


class LegiScanResource(ConfigurableResource):
    api_key: str | None = None
    base_url: str | None = None
    timeout_seconds: int = 30
    max_retries: int = 4

    def client(self) -> LegiScanClient:
        return LegiScanClient(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            max_retries=self.max_retries,
        )


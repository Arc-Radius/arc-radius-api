"""Shared keyset cursor encoding for bill list pagination (mock + Neo4j)."""

import base64
import json


def decode_cursor(cursor: str | None) -> tuple[str, str] | None:
    if not cursor:
        return None
    payload = base64.urlsafe_b64decode(cursor.encode()).decode()
    data = json.loads(payload)
    return str(data["sortValue"]), str(data["id"])


def encode_cursor(sort_value: str, bill_id: str) -> str:
    payload = json.dumps({"sortValue": sort_value, "id": bill_id}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode()

from fastapi.testclient import TestClient

from fastapi import FastAPI
from src.routers.bills import router as bills_router
from src.routers.states import router as states_router

app = FastAPI()
app.include_router(bills_router)
app.include_router(states_router)

client = TestClient(app)


def test_states_ui_with_counts():
    response = client.get("/states")
    assert response.status_code == 200
    payload = response.json()
    assert "states" in payload
    assert len(payload["states"]) > 0
    first = payload["states"][0]
    assert "abbr" in first
    assert "name" in first
    assert "status" in first
    assert first["status"] in {"supportive", "mixed", "harmful"}
    assert "counts" in first
    assert "activeBills" in first["counts"]
    assert "passedBills" in first["counts"]


def test_states_include_counts_false():
    response = client.get("/states", params={"includeCounts": "false"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["states"]
    assert payload["states"][0].get("counts") is None


def test_bills_list_filters_and_meta():
    response = client.get(
        "/bills",
        params=[
            ("state", "ca"),
            ("tab", "active"),
            ("stances", "supportive"),
            ("categories", "Healthcare"),
            ("years", "2025"),
            ("sortBy", "lastActionDate"),
            ("sortDir", "desc"),
            ("pageSize", "20"),
        ],
    )
    assert response.status_code == 200
    payload = response.json()
    assert "bills" in payload
    assert "meta" in payload
    assert payload["meta"]["sortBy"] == "lastActionDate"
    assert payload["meta"]["sortDir"] == "desc"
    assert payload["meta"]["pageSize"] == 20
    assert "hasMore" in payload["meta"]
    for bill in payload["bills"]:
        assert bill["billTab"] == "active"
        assert bill["stance"] == "supportive"
        assert bill["year"] == 2025
        assert "Healthcare" in bill["issue_categories"]


def test_bills_cursor_pagination():
    first_page = client.get(
        "/bills",
        params={"state": "CA", "sortBy": "lastActionDate", "sortDir": "desc", "pageSize": 1},
    )
    assert first_page.status_code == 200
    first_payload = first_page.json()
    assert len(first_payload["bills"]) == 1
    next_cursor = first_payload["meta"]["nextCursor"]
    assert next_cursor is not None

    second_page = client.get(
        "/bills",
        params={
            "state": "CA",
            "sortBy": "lastActionDate",
            "sortDir": "desc",
            "pageSize": 1,
            "cursor": next_cursor,
        },
    )
    assert second_page.status_code == 200
    second_payload = second_page.json()
    assert len(second_payload["bills"]) == 1
    assert second_payload["bills"][0]["id"] != first_payload["bills"][0]["id"]


def test_bills_validation_errors_use_fastapi_defaults():
    bad_enum = client.get("/bills", params={"state": "CA", "tab": "bad"})
    assert bad_enum.status_code == 422
    assert "detail" in bad_enum.json()

    bad_state = client.get("/bills", params={"state": "C1"})
    assert bad_state.status_code == 400
    assert bad_state.json()["detail"] == "state must be a two-letter code"

    bad_cursor = client.get("/bills", params={"state": "CA", "cursor": "not-base64"})
    assert bad_cursor.status_code == 400
    assert bad_cursor.json()["detail"] == "Invalid cursor"


def test_bill_detail_ui_and_404():
    ok_response = client.get("/bills/1398089")
    assert ok_response.status_code == 200
    payload = ok_response.json()
    assert "bill" in payload
    assert "graphRecord" in payload
    assert payload["bill"]["id"] == "1398089"
    assert payload["bill"]["billTab"] in {"active", "passed"}

    missing_response = client.get("/bills/does-not-exist")
    assert missing_response.status_code == 404
    assert missing_response.json()["detail"] == "Bill not found"

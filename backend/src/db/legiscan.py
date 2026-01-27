"""
LegiScan API - functional with optional connection reuse.
"""
import os
from typing import AsyncGenerator, Optional
import httpx
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.legiscan.com/"


def _get_api_key(api_key: Optional[str] = None) -> str:
    key = api_key or os.getenv("LEGISCAN_API_KEY")
    if not key:
        raise ValueError("LEGISCAN_API_KEY required")
    return key


async def _request(
    op: str,
    params: dict,
    api_key: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """Base request handler."""
    params = {"key": _get_api_key(api_key), "op": op, **params}

    if client:
        resp = await client.get(BASE_URL, params=params)
    else:
        async with httpx.AsyncClient(timeout=10.0) as c:
            resp = await c.get(BASE_URL, params=params)

    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK":
        raise ValueError(f"LegiScan error: {data}")
    return data


async def search_bill(
    state: str,
    bill: str,
    api_key: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """Search for a bill."""
    return await _request("getSearch", {"state": state, "bill": bill}, api_key, client)


async def get_bill(
    bill_id: int,
    api_key: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """Get bill details."""
    data = await _request("getBill", {"id": bill_id}, api_key, client)
    return data.get("bill", {})


async def get_master_list(
    session_id: int,
    api_key: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """Get all bills for a session."""
    data = await _request("getMasterList", {"id": session_id}, api_key, client)
    return data.get("masterlist", {})


async def get_legiscan_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """
    FastAPI Dependency: Yields an HTTP client.
    Auto-closes the connection when the request finishes.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        yield client

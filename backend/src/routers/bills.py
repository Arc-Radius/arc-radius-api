import json
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from src.routers.limiter import limiter
import httpx
from src.db.legiscan import get_legiscan_client, search_bill
from src.db.supabase import get_bills_supabase, get_db
from supabase import Client

router = APIRouter(prefix="/bills", tags=["bills"])


def _data_dir() -> Path:
    """Resolve repo root / datasources path regardless of current working directory."""
    return Path(__file__).resolve().parents[3] / "datasources" / "aclu"


def load_bills_from_json(limit: Optional[int] = None) -> List[Dict]:
    """Load bill records from the packaged JSON snapshot."""
    json_path = _data_dir() / "bill_classification_dict.json"
    if not json_path.exists():
        raise FileNotFoundError(f"Bill JSON not found at {json_path}")
    try:
        # utf-8-sig handles potential BOM; helps avoid control character errors
        with json_path.open(encoding="utf-8-sig") as f:
            data = json.load(f)
    except JSONDecodeError as exc:
        raise ValueError(
            f"Failed to parse JSON at {json_path}: {exc}") from exc
    return data[:limit] if limit else data


# Load from JSON for now (keep a modest cap to avoid huge payloads in dev)
_BILLS = load_bills_from_json(limit=200)
print(f"Loaded {len(_BILLS)} bills")


@router.get("/", summary="List bills")
@limiter.limit("1/second")
async def list_bills(request: Request):
    """Return a list of bills (placeholder data)."""
    return _BILLS


@router.get("/legiscan", summary="Fetch bills from LegiScan API")
@limiter.limit("1/second")
async def legiscan_api_bills(request: Request,
                             client: httpx.AsyncClient = Depends(get_legiscan_client)):
    """
    Example: Take the first 5 local bills and fetch their 
    latest status from LegiScan API in real-time.
    """
    # 1. Grab a slice of local bills
    subset_bills = _BILLS[:5]

    results = []

    # 2. Open the connection ONCE
    for bill in subset_bills:
        bill_data = await search_bill(
            state=bill["state"],
            bill=bill["bill_number"],
            client=client
        )
        results.append(bill_data)

    return results


@router.get("/supabase", summary="Fetch bills from Supabase database")
@limiter.limit("1/second")
async def supabase_bills(
    request: Request,
    limit: int = 20,
    db: Client = Depends(get_db)
):
    """
    Fetch bills from the Supabase ls_bill table.

    Args:
        limit: Maximum number of bills to return (default: 20, max: 100)
        db: Supabase client (injected via dependency)

    Returns:
        List of bill records from the database
    """
    # Cap the limit to prevent huge responses
    limit = min(limit, 100)

    try:
        bills = get_bills_supabase(db, limit=limit)
        return {
            "count": len(bills),
            "limit": limit,
            "bills": bills
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch bills from database: {str(e)}"
        )


@router.get("/{bill_number}", summary="Get bill by number")
@limiter.limit("1/second")
async def get_bill(bill_number: str, request: Request):
    """Return a single bill by its normalized number (e.g., 'HB229')."""
    target = bill_number.lower()
    for bill in _BILLS:
        if str(bill.get("bill_number", "")).lower() == target:
            return bill
    raise HTTPException(status_code=404, detail="Bill not found")

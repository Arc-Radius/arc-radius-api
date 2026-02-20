import json
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from src.routers.limiter import limiter
import httpx
from src.db.legiscan import get_legiscan_client, search_bill
from src.db.supabase import execute_graphql, get_bills_supabase, get_db
from supabase import Client

router = APIRouter(prefix="/bills", tags=["bills"])


def _data_dir() -> Path:
    """Resolve repo root / datasources path regardless of current working directory."""
    return Path(__file__).resolve().parents[3] / "datasources" / "aclu"



@router.get("/legiscan", summary="Fetch bills from LegiScan API")
@limiter.limit("1/second")
async def legiscan_api_bills(request: Request,
                             client: httpx.AsyncClient = Depends(get_legiscan_client)):
    """
    Example: Take the first 5 local bills and fetch their 
    latest status from LegiScan API in real-time.
    """
    # 1. Grab a slice of local bills
    subset_bills = [{"state": "CA", "bill_number": "HB229"}]

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


@router.post("/graphql", summary="Query bills via Supabase GraphQL (pg_graphql)")
@limiter.limit("5/second")
async def graphql_bills(
    request: Request,
    query: str = Body(..., description="GraphQL query string"),
    variables: dict | None = Body(None, description="Optional GraphQL variables"),
):
    """
    Forward a GraphQL query to Supabase's built-in pg_graphql endpoint.

    Supabase auto-generates a GraphQL schema from your Postgres tables.
    Table names become `<table>Collection` (e.g. `ls_billCollection`).

    **Example request body:**
    ```json
    {
        "query": "query { ls_billCollection(first: 5) { edges { node { bill_number title legiscan_url } } } }"
    }
    ```
    """
    try:
        result = await execute_graphql(query=query, variables=variables)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Supabase GraphQL error: {exc.response.text}",
        )
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to reach Supabase GraphQL: {str(exc)}",
        )

    # Surface GraphQL-level errors (Supabase returns 200 even on query errors)
    if "errors" in result and result["errors"]:
        raise HTTPException(
            status_code=400,
            detail={"graphql_errors": result["errors"]},
        )

    return result

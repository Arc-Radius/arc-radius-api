import json
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from src.routers.limiter import limiter
import httpx
from src.db.legiscan import get_legiscan_client, search_bill, get_bill
from src.db.supabase import execute_graphql, get_bills_supabase, get_db
from supabase import Client

router = APIRouter(prefix="/bills", tags=["bills"])


@router.get("/search", summary="Full text search for bills")
@limiter.limit("1/second")
async def legiscan_search_bills(
    request: Request,
    state: str = "ALL",
    text_search: Optional[str] = None,
    bill_number: Optional[str] = None,
    year: Optional[int] = None,
    page: Optional[int] = None,
    client: httpx.AsyncClient = Depends(get_legiscan_client),
):
    """
    Search bills using the LegiScan search engine.

    - **state**: State abbreviation (e.g. CA, TX) or ALL for nationwide
    - **query**: Full text search (e.g. "housing affordability")
    - **bill**: Structured bill number lookup (e.g. "HB229")
    - **year**: 1=all, 2=current (default), 3=recent, 4=prior, or >1900 for exact year
    - **page**: Result page number (default: 1)

    Provide either `query` or `bill` (or both).
    """
    if not text_search and not bill_number:
        raise HTTPException(
            status_code=400, detail="Either text_search or bill_number parameter is required")
    if text_search and bill_number:
        raise HTTPException(
            status_code=400, detail="Provide text_search for full text search or bill_number for structured bill number lookup, not both")

    return await search_bill(
        state=state,
        query=text_search,
        bill=bill_number,
        year=year,
        page=page,
        client=client,
    )


@router.get("/get", summary="Get a bill by ID")
@limiter.limit("1/second")
async def legiscan_get_bill_by_id(request: Request,
                                  bill_id: int,
                                  client: httpx.AsyncClient = Depends(get_legiscan_client)):
    """
    Get a bill by ID from LegiScan API. The bill_id is the internal bill id for the requested bill.
    """
    bill_data = await get_bill(bill_id=bill_id, client=client)
    return bill_data


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
    variables: dict | None = Body(
        None, description="Optional GraphQL variables"),
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

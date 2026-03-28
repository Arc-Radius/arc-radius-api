import asyncio
import binascii
import logging
import httpx
from typing import Optional

logger = logging.getLogger(__name__)
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from src.core.settings import settings
from src.db.legiscan import get_legiscan_client, search_bill, get_bill, get_bill_text, get_bill_with_text
from src.db.supabase import execute_graphql, get_bills_supabase, get_db
from supabase import Client
from src.models.ui import (
    BillDetailResponse,
    BillTab,
    BillsListResponse,
    LegislativeStatus,
    SortBy,
    SortDir,
)
from src.services import neo4j_ui_service as neo_ui
from src.services.mock_data import get_bill_detail, list_bills
from src.services.rag_service import query_and_generate
router = APIRouter(prefix="/bills", tags=["bills"])


@router.get("/search", summary="Full text search for bills")
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


@router.get("", response_model=BillsListResponse, summary="List bills (frontend contract)")
async def get_bills(
    state: str = Query(..., min_length=2, max_length=2, description="Two-letter state code"),
    tab: BillTab | None = None,
    stances: list[LegislativeStatus] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    years: list[int] | None = Query(default=None),
    sortBy: SortBy = SortBy.last_action_date,
    sortDir: SortDir = SortDir.desc,
    cursor: str | None = None,
    pageSize: int = Query(default=20, ge=1, le=100),
):
    normalized_state = state.upper().strip()
    if len(normalized_state) != 2 or not normalized_state.isalpha():
        raise HTTPException(status_code=400, detail="state must be a two-letter code")
    try:
        if settings.neo4j_ui_enabled:
            try:
                return await neo_ui.list_bills_neo4j(
                    state=normalized_state,
                    tab=tab,
                    stances=stances,
                    categories=categories,
                    years=years,
                    sort_by=sortBy,
                    sort_dir=sortDir,
                    cursor=cursor,
                    page_size=pageSize,
                )
            except Exception:
                logger.exception("Neo4j bills list failed")
                raise HTTPException(status_code=503, detail="Neo4j unavailable")
        return list_bills(
            state=normalized_state,
            tab=tab,
            stances=stances,
            categories=categories,
            years=years,
            sort_by=sortBy,
            sort_dir=sortDir,
            cursor=cursor,
            page_size=pageSize,
        )
    except (ValueError, binascii.Error):
        raise HTTPException(status_code=400, detail="Invalid cursor")


@router.get("/get", summary="Get a bill by ID")
async def legiscan_get_bill_by_id(request: Request,
                                  bill_id: int,
                                  client: httpx.AsyncClient = Depends(get_legiscan_client)):
    """
    Get a bill by ID from LegiScan API. The bill_id is the internal bill id for the requested bill.
    """
    bill_data = await get_bill(bill_id=bill_id, client=client)
    return bill_data


@router.get("/rag", summary="Graph RAG query over bill chunks")
async def knowledge_graph_query(
    request: Request,
    query: str,
    top: int = 10,
):
    """
    Takes a natural language question, retrieves relevant bill chunks from the
    knowledge graph, and returns an LLM-generated answer grounded in retrieved data.
    - **query**: Natural language question (e.g. "What bills affect trans youth in Texas?")
    - **top**: Number of top-ranked chunks to retrieve for context (default: 10)
    Returns:
    - **answer**: LLM-generated response from user prompt and bill chunks
    - **sources**: Retrieved bill chunks with relevance scores and metadata
    """
    result = await asyncio.to_thread(query_and_generate, query, top=top)
    return result


@router.get("/get-with-text", summary="Get a bill and its latest text")
async def legiscan_get_bill_with_text(
    request: Request,
    bill_id: int,
    client: httpx.AsyncClient = Depends(get_legiscan_client),
):
    """
    Get bill details and the latest bill text in one call.

    Chains getBill → getBillText using the latest doc_id from the bill's texts array.
    """
    bill_data = await get_bill_with_text(bill_id=bill_id, client=client)
    return bill_data


@router.get("/supabase", summary="Fetch bills from Supabase database")
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


@router.get("/{billPk}", response_model=BillDetailResponse, summary="Get bill detail (frontend contract)")
async def get_bill_by_pk(billPk: str):
    if settings.neo4j_ui_enabled:
        try:
            detail = await neo_ui.get_bill_detail_neo4j(billPk)
        except Exception:
            logger.exception("Neo4j bill detail failed")
            raise HTTPException(status_code=503, detail="Neo4j unavailable")
        if detail is None:
            raise HTTPException(status_code=404, detail="Bill not found")
        return detail
    detail = get_bill_detail(billPk)
    if detail is None:
        raise HTTPException(status_code=404, detail="Bill not found")
    return detail

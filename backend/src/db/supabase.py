"""
Supabase database connection and client management.
"""
import os
from typing import Optional

from fastapi import Depends
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv(
    "SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

# Global client instance (lazy-loaded)
_supabase_client: Optional[Client] = None


def get_supabase_client() -> Client:
    """
    Get or create the Supabase client singleton.

    Raises ValueError if SUPABASE_URL or SUPABASE_KEY are not set.
    """
    global _supabase_client

    if _supabase_client is None:
        if not SUPABASE_URL:
            raise ValueError("SUPABASE_URL environment variable is required")
        if not SUPABASE_KEY:
            raise ValueError(
                "SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY environment variable is required")

        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)

    return _supabase_client


def get_db() -> Client:
    """
    FastAPI dependency: Returns the Supabase client.
    Use in route handlers: async def my_route(db: Client = Depends(get_db))
    """
    return get_supabase_client()


def reset_client():
    """Reset the global client (useful for testing)."""
    global _supabase_client
    _supabase_client = None


# Example query functions for ls_bill table

def get_bills_postgres_sql(client: Client) -> list:
    """
    Example: Using raw PostgreSQL SQL via Supabase RPC or direct query.

    Note: Supabase client doesn't directly support raw SQL, but you can:
    1. Use RPC functions (stored procedures)
    2. Use the PostgREST query builder (recommended - see get_bills_supabase)
    """
    # Option 1: If you have an RPC function defined in Supabase
    # response = client.rpc('get_first_20_bills').execute()
    # return response.data

    # Option 2: Direct SQL via PostgREST (not directly supported, but you can use .select())
    # This is why we use the Supabase query builder instead (see get_bills_supabase)
    raise NotImplementedError(
        "Use Supabase query builder or RPC functions for SQL operations")


def get_bills_supabase(client: Client, limit: int = 20) -> list:
    """
    Fetch bills from Supabase using query builder syntax (recommended).

    This is the idiomatic way to query Supabase tables.
    Equivalent to: SELECT * FROM ls_bill LIMIT 20

    Args:
        client: Supabase client instance
        limit: Maximum number of bills to return

    Returns:
        List of bill records from the database

    Raises:
        Exception: If the query fails or table doesn't exist
    """
    try:
        response = client.table("ls_bill").select("*").limit(limit).execute()
        return response.data if response.data else []
    except Exception as e:
        raise Exception(
            f"Failed to fetch bills from Supabase: {str(e)}") from e


def get_bills_with_filters(client: Client, state: str = None, limit: int = 20) -> list:
    """
    Fetch bills from Supabase with optional state filter.

    Equivalent to: SELECT * FROM ls_bill WHERE state = 'CA' LIMIT 20

    Args:
        client: Supabase client instance
        state: Optional state abbreviation to filter by (e.g., 'CA', 'TX')
        limit: Maximum number of bills to return

    Returns:
        List of bill records matching the filter

    Raises:
        Exception: If the query fails or table doesn't exist
    """
    try:
        query = client.table("ls_bill").select("*")

        if state:
            query = query.eq("state", state.upper())

        response = query.limit(limit).execute()
        return response.data if response.data else []
    except Exception as e:
        raise Exception(
            f"Failed to fetch bills from Supabase with filters: {str(e)}") from e


# Usage examples:
#
# In a FastAPI route:
# @router.get("/bills")
# async def list_bills(db: Client = Depends(get_db)):
#     bills = get_bills_supabase(db, limit=20)
#     return bills
#
# With filters:
# @router.get("/bills/{state}")
# async def list_bills_by_state(state: str, db: Client = Depends(get_db)):
#     bills = get_bills_with_filters(db, state=state, limit=20)
#     return bills

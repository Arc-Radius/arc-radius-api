"""Async Neo4j driver lifecycle + parameterized read helpers for FastAPI."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from neo4j import AsyncGraphDatabase, AsyncDriver, READ_ACCESS

from src.core.settings import settings

logger = logging.getLogger(__name__)

_driver: AsyncDriver | None = None
_lock = asyncio.Lock()


async def get_async_driver() -> AsyncDriver | None:
    """Lazily create a shared driver when Neo4j is configured."""
    global _driver
    if not settings.neo4j_ui_enabled:
        return None
    if _driver is not None:
        return _driver
    async with _lock:
        if _driver is not None:
            return _driver
        assert settings.neo4j_uri is not None
        assert settings.neo4j_user is not None
        _driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
        logger.info("Neo4j async driver initialized")
        return _driver


async def close_async_driver() -> None:
    global _driver
    if _driver is not None:
        await _driver.close()
        _driver = None
        logger.info("Neo4j async driver closed")


async def neo4j_read(cypher: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run a read query with explicit parameters (never string-interpolate user input)."""
    driver = await get_async_driver()
    if driver is None:
        return []
    params = parameters or {}
    async with driver.session(default_access_mode=READ_ACCESS) as session:
        result = await session.run(cypher, params)
        return await result.data()


async def neo4j_read_single(cypher: str, parameters: dict[str, Any] | None = None) -> dict[str, Any] | None:
    rows = await neo4j_read(cypher, parameters)
    if not rows:
        return None
    return rows[0]

import logging

from fastapi import APIRouter, HTTPException, Response

from src.core.settings import settings

logger = logging.getLogger(__name__)
from src.models.ui import StatesResponse
from src.services import neo4j_ui_service as neo_ui
from src.services.mock_data import list_states

router = APIRouter(prefix="/states", tags=["states"])


@router.get("", response_model=StatesResponse, summary="List states for dashboard/search")
async def get_states(response: Response, includeCounts: bool = True):
    response.headers["Cache-Control"] = "public, max-age=300"
    if settings.neo4j_ui_enabled:
        try:
            return await neo_ui.list_states_neo4j(include_counts=includeCounts)
        except Exception:
            logger.exception("Neo4j states query failed")
            raise HTTPException(status_code=503, detail="Neo4j unavailable")
    return list_states(include_counts=includeCounts)

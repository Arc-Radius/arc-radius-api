from fastapi import APIRouter, Response

from src.models.ui import StatesResponse
from src.services.mock_data import list_states

router = APIRouter(prefix="/states", tags=["states"])


@router.get("", response_model=StatesResponse, summary="List states for dashboard/search")
async def get_states(response: Response, includeCounts: bool = True):
    response.headers["Cache-Control"] = "public, max-age=300"
    return list_states(include_counts=includeCounts)

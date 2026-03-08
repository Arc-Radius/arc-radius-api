import asyncio
from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from src.routers.limiter import limiter
from src.services.rag_service import query_and_generate_task

router = APIRouter(prefix="/generate", tags=["generation"])


class BillGenerationRequest(BaseModel):
    task: Literal["bill_summary", "bill_why_matters"] = Field(
        ...,
        description=(
            "Generation task type. Supports 'bill_summary' and 'bill_why_matters'."
        ),
    )
    bill_pk: int = Field(
        ..., gt=0, description="bill_pk for the bill to summarize."
    )


@router.post("/bill", summary="Generate bill explanations from RAG context")
@limiter.limit("1/second")
async def generate_bill(
    request: Request,
    payload: BillGenerationRequest,
):
    result = await asyncio.to_thread(
        query_and_generate_task,
        payload.task,
        payload.bill_pk,
    )
    return result

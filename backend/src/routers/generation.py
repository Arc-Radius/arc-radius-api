import asyncio
import re
from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field, field_validator

from src.services.rag_service import query_and_generate_task

router = APIRouter(prefix="/generate", tags=["generation"])
_BILL_PK_PATTERN = re.compile(r"^[A-Z]{2}:[0-9]+:[0-9]+$")


class BillGenerationRequest(BaseModel):
    task: Literal["bill_summary", "bill_why_matters", "bill_related"] = Field(
        ...,
        description=(
            "Generation task type. Supports 'bill_summary', 'bill_why_matters', and 'bill_related'."
        ),
    )
    bill_pk: str = Field(
        ...,
        min_length=1,
        description="bill_pk for the bill to summarize (format: STATE:SESSION_ID:BILL_ID).",
    )

    @field_validator("bill_pk")
    @classmethod
    def validate_bill_pk(cls, value: str) -> str:
        bill_pk = value.strip()
        if not _BILL_PK_PATTERN.match(bill_pk):
            raise ValueError("bill_pk must match format STATE:SESSION_ID:BILL_ID")
        return bill_pk


@router.post("/bill", summary="Generate bill explanations from RAG context")
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

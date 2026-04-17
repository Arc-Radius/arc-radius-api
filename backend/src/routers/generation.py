import asyncio
import re
from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field, field_validator

from src.services.rag_service import query_and_generate_task

router = APIRouter(prefix="/generate", tags=["generation"])
_BILL_PK_PATTERN = re.compile(r"^[A-Z]{2}:[0-9]+:[0-9]+$")


class LetterParams(BaseModel):
    medium: Literal["email", "phone"] = "email"
    stance: Literal["support", "oppose"] = "support"
    tone: Literal["formal", "conversational"] = "conversational"
    representative_name: str | None = Field(
        default=None,
        description="Representative name from zipcode lookup — injected into the letter greeting.",
    )


class BillGenerationRequest(BaseModel):
    task: Literal["bill_summary", "bill_why_matters", "bill_related", "generate_letter"] = Field(
        ...,
        description=(
            "Generation task type. Supports 'bill_summary', 'bill_why_matters', "
            "'bill_related', and 'generate_letter'."
        ),
    )
    bill_pk: str = Field(
        ...,
        min_length=1,
        description="bill_pk for the bill to summarize (format: STATE:SESSION_ID:BILL_ID).",
    )
    letter_params: LetterParams | None = Field(
        default=None,
        description="Required when task is 'generate_letter'.",
    )

    @field_validator("bill_pk")
    @classmethod
    def validate_bill_pk(cls, value: str) -> str:
        bill_pk = value.strip()
        if not _BILL_PK_PATTERN.match(bill_pk):
            raise ValueError("bill_pk must match format STATE:SESSION_ID:BILL_ID")
        return bill_pk

    @field_validator("letter_params", mode="after")
    @classmethod
    def validate_letter_params(cls, value, info):
        if info.data.get("task") == "generate_letter" and value is None:
            raise ValueError("letter_params is required when task is 'generate_letter'")
        return value


@router.post("/bill", summary="Generate bill explanations from RAG context")
async def generate_bill(
    request: Request,
    payload: BillGenerationRequest,
):
    result = await asyncio.to_thread(
        query_and_generate_task,
        payload.task,
        payload.bill_pk,
        payload.letter_params.model_dump() if payload.letter_params else None,
    )
    return result

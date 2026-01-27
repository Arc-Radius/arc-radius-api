from typing import List

from pydantic import BaseModel, Field, model_validator


class SearchResultSummary(BaseModel):
    page: str
    range: str
    relevancy: str
    count: int = Field(..., description="Total results for the query")
    page_current: int
    page_total: int
    query: str


class SearchResultItem(BaseModel):
    relevance: int
    state: str
    bill_number: str
    bill_id: int
    change_hash: str
    url: str
    text_url: str
    research_url: str
    last_action_date: str
    last_action: str
    title: str


class SearchResult(BaseModel):
    summary: SearchResultSummary
    items: List[SearchResultItem]

    @model_validator(mode="before")
    def _flatten_numeric_keys(cls, value: dict):
        """
        LegiScan returns result items under numeric string keys ("0", "1", ...).
        Normalize to a list under 'items'.
        """
        if not isinstance(value, dict):
            return value

        summary = value.get("summary", {})
        items = [v for k, v in value.items() if isinstance(k, str) and k.isdigit()]
        return {"summary": summary, "items": items}


class LegiScanSearchResponse(BaseModel):
    status: str
    searchresult: SearchResult

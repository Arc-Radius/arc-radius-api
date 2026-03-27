from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class LegislativeStatus(str, Enum):
    supportive = "supportive"
    mixed = "mixed"
    harmful = "harmful"


class BillTab(str, Enum):
    active = "active"
    passed = "passed"


class SortBy(str, Enum):
    last_action_date = "lastActionDate"
    year = "year"
    relevance = "relevance"


class SortDir(str, Enum):
    asc = "asc"
    desc = "desc"


class StateCounts(BaseModel):
    activeBills: int
    passedBills: int


class StateRow(BaseModel):
    abbr: str
    name: str
    status: LegislativeStatus
    legislature: str
    session: str
    sessionWindow: str
    stateLink: str
    lastUpdated: str
    counts: StateCounts | None = None


class StatesResponse(BaseModel):
    states: list[StateRow]


class BillListItem(BaseModel):
    id: str
    bill_number: str
    title: str
    description: str
    stance: LegislativeStatus
    billTab: BillTab
    status: str
    status_desc: str
    last_action: str
    last_action_date: str
    year: int
    primary_sponsor: str
    issue_categories: list[str] = Field(default_factory=list)
    url: str | None = None
    relevance: float | None = None


class BillFacets(BaseModel):
    stances: dict[LegislativeStatus, int]
    categories: dict[str, int]
    years: dict[str, int]


class ListMeta(BaseModel):
    sortBy: SortBy
    sortDir: SortDir
    pageSize: int
    cursor: str | None
    nextCursor: str | None
    hasMore: bool
    totalCount: int | None = None


class BillsListResponse(BaseModel):
    bills: list[BillListItem]
    facets: BillFacets | None = None
    meta: ListMeta


class BillSponsor(BaseModel):
    name: str
    party: Literal["D", "R", "I"]


class BillHistoryItem(BaseModel):
    date: str
    chamber: str
    action: str


class RelatedBill(BaseModel):
    bill_id: str
    summary: str
    confidence: Literal["high", "medium", "low"]


class BillKeyDate(BaseModel):
    date: str
    description: str
    isPast: bool | None = None


class AIAnalysis(BaseModel):
    classification: str
    impactScore: float
    keyProvisions: list[str] = Field(default_factory=list)
    potentialImpact: str
    legalContext: str
    recommendation: str


class Study(BaseModel):
    title: str
    authors: str
    journal: str
    year: int
    finding: str
    impactType: Literal["positive", "negative", "neutral"]
    sampleSize: str | None = None
    doi: str | None = None


class HealthImpact(BaseModel):
    category: str
    direction: Literal["positive", "negative"]
    magnitude: Literal["significant", "moderate", "minor"]
    description: str


class ResearchEvidence(BaseModel):
    studies: list[Study] = Field(default_factory=list)
    healthImpacts: list[HealthImpact] = Field(default_factory=list)
    dsTechnique: str


class SponsorContact(BaseModel):
    name: str
    email: str
    phone: str
    office: str
    district: str


class BillDetail(BaseModel):
    id: str
    number: str
    title: str
    summary: str
    fullText: str
    state: str
    status: str
    progression: float
    lastAction: str
    lastActionDate: str
    pendingCommittee: str
    sponsors: list[BillSponsor] = Field(default_factory=list)
    spectrum: Literal["Supportive", "Neutral", "Harmful"]
    introducedDate: str
    history: list[BillHistoryItem] = Field(default_factory=list)
    subjects: list[str] = Field(default_factory=list)
    similarBills: list[str] = Field(default_factory=list)
    relatedBills: list[RelatedBill] = Field(default_factory=list)
    keyDates: list[BillKeyDate] = Field(default_factory=list)
    aiAnalysis: AIAnalysis | None = None
    researchEvidence: ResearchEvidence | None = None
    sponsorContact: SponsorContact | None = None
    billTab: BillTab


class GraphBillRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    bill_pk: str | None = None
    bill_id: str | None = None
    state: str | None = None
    session_id: str | None = None
    bill_number: str | None = None
    title: str | None = None
    description: str | None = None
    status: str | None = None
    status_desc: str | None = None
    status_date: str | None = None
    url: str | None = None
    state_link: str | None = None
    label: str | None = None
    label_source: str | None = None
    confidence: float | None = None
    relevance_score: float | None = None
    issues: str | None = None
    issue_categories: str | None = None
    sponsor_names: str | None = None
    primary_sponsor: str | None = None
    last_action: str | None = None
    last_action_date: str | None = None
    year: int | None = None
    state_lean: str | None = None
    bill_dominant_party: str | None = None
    passed: bool | None = None
    failed: bool | None = None
    vetoed: bool | None = None
    r_sponsorship_ratio: float | None = None
    pass_rate_gap: float | str | None = None
    overall_pass_rate: float | str | None = None
    bipartisan_ratio: float | None = None
    session_year: int | None = None


class BillDetailResponse(BaseModel):
    bill: BillDetail
    graphRecord: GraphBillRecord

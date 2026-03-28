from copy import deepcopy
from datetime import date
from typing import Any

from src.models.ui import (
    BillDetailResponse,
    BillFacets,
    BillListItem,
    BillTab,
    BillsListResponse,
    LegislativeStatus,
    ListMeta,
    SortBy,
    SortDir,
    StateRow,
    StatesResponse,
)
from src.services.bill_cursor import decode_cursor, encode_cursor


MOCK_STATES: list[dict[str, Any]] = [
    {
        "abbr": "CA",
        "name": "California",
        "status": LegislativeStatus.supportive.value,
        "legislature": "California State Legislature",
        "session": "2025-2026 Regular",
        "sessionWindow": "Jan - Sep",
        "stateLink": "https://leginfo.legislature.ca.gov",
        "lastUpdated": "2025-03-20",
        "counts": {"activeBills": 29, "passedBills": 14},
    },
    {
        "abbr": "TX",
        "name": "Texas",
        "status": LegislativeStatus.harmful.value,
        "legislature": "Texas Legislature",
        "session": "89th Legislature",
        "sessionWindow": "Jan - Jun",
        "stateLink": "https://capitol.texas.gov",
        "lastUpdated": "2025-03-21",
        "counts": {"activeBills": 34, "passedBills": 11},
    },
    {
        "abbr": "WA",
        "name": "Washington",
        "status": LegislativeStatus.mixed.value,
        "legislature": "Washington State Legislature",
        "session": "2025 Regular Session",
        "sessionWindow": "Jan - Apr",
        "stateLink": "https://leg.wa.gov",
        "lastUpdated": "2025-03-19",
        "counts": {"activeBills": 18, "passedBills": 8},
    },
]

MOCK_BILLS: list[dict[str, Any]] = [
    {
        "id": "1398089",
        "bill_number": "HB 903",
        "title": "Health Curriculum Restrictions",
        "description": "Restricts classroom instruction scope in K-12 settings.",
        "stance": LegislativeStatus.harmful.value,
        "billTab": BillTab.active.value,
        "status": "In Committee",
        "status_desc": "Referred to Education",
        "last_action": "Referred to committee",
        "last_action_date": "2025-03-15",
        "year": 2025,
        "primary_sponsor": "Jane Doe",
        "issue_categories": ["Education", "Health"],
        "url": "https://example.gov/tx/hb903",
        "state": "TX",
        "relevance": 0.94,
    },
    {
        "id": "ca-77",
        "bill_number": "SB 77",
        "title": "Youth Safety Resource Access",
        "description": "Expands access to protective resources and reporting channels.",
        "stance": LegislativeStatus.supportive.value,
        "billTab": BillTab.active.value,
        "status": "In Committee",
        "status_desc": "Hearing scheduled",
        "last_action": "Set for hearing",
        "last_action_date": "2025-03-20",
        "year": 2025,
        "primary_sponsor": "Alex Smith",
        "issue_categories": ["Safety", "Healthcare"],
        "url": "https://example.gov/ca/sb77",
        "state": "CA",
        "relevance": 0.88,
    },
    {
        "id": "wa-301",
        "bill_number": "HB 301",
        "title": "School Privacy Policy Updates",
        "description": "Updates district privacy guidance and compliance reporting.",
        "stance": LegislativeStatus.mixed.value,
        "billTab": BillTab.passed.value,
        "status": "Passed Legislature",
        "status_desc": "Sent to governor",
        "last_action": "Passed Senate",
        "last_action_date": "2024-06-05",
        "year": 2024,
        "primary_sponsor": "Jordan Kim",
        "issue_categories": ["Education"],
        "url": "https://example.gov/wa/hb301",
        "state": "WA",
        "relevance": 0.72,
    },
    {
        "id": "ca-12",
        "bill_number": "AB 12",
        "title": "Public Health Inclusion Program",
        "description": "Funds targeted inclusion and outreach support services.",
        "stance": LegislativeStatus.supportive.value,
        "billTab": BillTab.passed.value,
        "status": "Enacted",
        "status_desc": "Chaptered",
        "last_action": "Signed by governor",
        "last_action_date": "2024-09-11",
        "year": 2024,
        "primary_sponsor": "Riley Chen",
        "issue_categories": ["Healthcare", "Public Health"],
        "url": "https://example.gov/ca/ab12",
        "state": "CA",
        "relevance": 0.83,
    },
]

MOCK_BILL_DETAILS_BY_PK: dict[str, dict[str, Any]] = {
    "1398089": {
        "bill": {
            "id": "1398089",
            "number": "HB 903",
            "title": "Health Curriculum Restrictions",
            "summary": "Limits discussion topics and modifies reporting procedures.",
            "fullText": "SECTION 1. Curriculum content requirements are amended...",
            "state": "TX",
            "status": "Engrossed",
            "progression": 0.6,
            "lastAction": "Passed House",
            "lastActionDate": "2025-03-10",
            "pendingCommittee": "Senate Education",
            "sponsors": [{"name": "Jane Doe", "party": "D"}],
            "spectrum": "Harmful",
            "introducedDate": "2025-01-15",
            "history": [
                {"date": "2025-02-01", "chamber": "H", "action": "Referred to committee"},
                {"date": "2025-03-10", "chamber": "H", "action": "Passed chamber"},
            ],
            "subjects": ["Public health", "Education"],
            "similarBills": [],
            "relatedBills": [{"bill_id": "TX HB2758", "summary": "Related scope", "confidence": "high"}],
            "keyDates": [{"date": "2025-04-01", "description": "Committee hearing", "isPast": False}],
            "sponsorContact": {
                "name": "Jane Doe",
                "email": "jane.doe@example.gov",
                "phone": "(555) 010-2201",
                "office": "Capitol Extension E2.314",
                "district": "TX-40",
            },
            "billTab": "active",
        },
        "graphRecord": {
            "bill_pk": "1398089",
            "bill_id": "1398089",
            "state": "TX",
            "confidence": 0.87,
            "label": "harmful",
            "issues": "Public health, education",
            "issue_categories": "Public health|Education",
            "last_action_date": "2025-03-10",
            "bill_number": "HB 903",
            "title": "Health Curriculum Restrictions",
            "status_desc": "Passed House",
            "url": "https://example.gov/tx/hb903",
            "primary_sponsor": "Jane Doe",
            "year": 2025,
        },
    },
    "ca-77": {
        "bill": {
            "id": "ca-77",
            "number": "SB 77",
            "title": "Youth Safety Resource Access",
            "summary": "Improves access to school and community safety resources.",
            "fullText": "SECTION 1. Public schools shall maintain...",
            "state": "CA",
            "status": "In Committee",
            "progression": 0.35,
            "lastAction": "Set for hearing",
            "lastActionDate": "2025-03-20",
            "pendingCommittee": "Assembly Education",
            "sponsors": [{"name": "Alex Smith", "party": "I"}],
            "spectrum": "Supportive",
            "introducedDate": "2025-01-21",
            "history": [{"date": "2025-03-20", "chamber": "S", "action": "Set for hearing"}],
            "subjects": ["Safety", "Healthcare"],
            "similarBills": ["ab-230"],
            "relatedBills": [],
            "keyDates": [],
            "billTab": "active",
        },
        "graphRecord": {
            "bill_pk": "ca-77",
            "bill_id": "ca-77",
            "state": "CA",
            "confidence": 0.79,
            "label": "supportive",
            "issues": "Safety, healthcare",
            "last_action_date": "2025-03-20",
            "bill_number": "SB 77",
            "title": "Youth Safety Resource Access",
            "status_desc": "In Committee",
            "url": "https://example.gov/ca/sb77",
            "primary_sponsor": "Alex Smith",
            "year": 2025,
        },
    },
}


def list_states(include_counts: bool) -> StatesResponse:
    states = deepcopy(MOCK_STATES)
    if not include_counts:
        for state in states:
            state.pop("counts", None)
    rows = [StateRow.model_validate(state) for state in states]
    return StatesResponse(states=rows)


def _date_key(value: str) -> date:
    return date.fromisoformat(value)


def _sort_bills(items: list[dict[str, Any]], sort_by: SortBy, sort_dir: SortDir) -> list[dict[str, Any]]:
    reverse = sort_dir == SortDir.desc
    if sort_by == SortBy.last_action_date:
        return sorted(items, key=lambda x: (_date_key(x["last_action_date"]), x["id"]), reverse=reverse)
    if sort_by == SortBy.year:
        return sorted(items, key=lambda x: (x["year"], x["id"]), reverse=reverse)
    return sorted(items, key=lambda x: (x.get("relevance") or 0.0, x["id"]), reverse=reverse)


def _apply_cursor(
    items: list[dict[str, Any]],
    sort_by: SortBy,
    sort_dir: SortDir,
    cursor: tuple[str, str] | None,
) -> list[dict[str, Any]]:
    if cursor is None:
        return items
    sort_value, bill_id = cursor
    filtered: list[dict[str, Any]] = []
    for item in items:
        if sort_by == SortBy.last_action_date:
            item_value = item["last_action_date"]
        elif sort_by == SortBy.year:
            item_value = str(item["year"])
        else:
            item_value = str(item.get("relevance") or 0.0)

        if sort_dir == SortDir.desc:
            include = (item_value < sort_value) or (item_value == sort_value and item["id"] < bill_id)
        else:
            include = (item_value > sort_value) or (item_value == sort_value and item["id"] > bill_id)
        if include:
            filtered.append(item)
    return filtered


def _build_facets(scope: list[dict[str, Any]]) -> BillFacets:
    stance_counts = {stance: 0 for stance in LegislativeStatus}
    categories: dict[str, int] = {}
    years: dict[str, int] = {}

    for bill in scope:
        stance_counts[LegislativeStatus(bill["stance"])] += 1
        year_key = str(bill["year"])
        years[year_key] = years.get(year_key, 0) + 1
        for category in bill["issue_categories"]:
            categories[category] = categories.get(category, 0) + 1

    return BillFacets(stances=stance_counts, categories=categories, years=years)


def list_bills(
    state: str,
    tab: BillTab | None,
    stances: list[LegislativeStatus] | None,
    categories: list[str] | None,
    years: list[int] | None,
    sort_by: SortBy,
    sort_dir: SortDir,
    cursor: str | None,
    page_size: int,
) -> BillsListResponse:
    normalized_state = state.upper().strip()
    scoped = [bill for bill in MOCK_BILLS if bill["state"] == normalized_state]
    if tab:
        scoped = [bill for bill in scoped if bill["billTab"] == tab.value]
    if stances:
        allowed = {stance.value for stance in stances}
        scoped = [bill for bill in scoped if bill["stance"] in allowed]
    if categories:
        category_set = {c.strip() for c in categories if c.strip()}
        scoped = [bill for bill in scoped if category_set.intersection(set(bill["issue_categories"]))]
    if years:
        year_set = set(years)
        scoped = [bill for bill in scoped if bill["year"] in year_set]

    sorted_items = _sort_bills(scoped, sort_by=sort_by, sort_dir=sort_dir)
    cursor_tuple = decode_cursor(cursor)
    paged_scope = _apply_cursor(sorted_items, sort_by=sort_by, sort_dir=sort_dir, cursor=cursor_tuple)

    rows = paged_scope[:page_size]
    has_more = len(paged_scope) > page_size
    next_cursor = None
    if has_more and rows:
        last = rows[-1]
        if sort_by == SortBy.last_action_date:
            next_cursor = encode_cursor(last["last_action_date"], last["id"])
        elif sort_by == SortBy.year:
            next_cursor = encode_cursor(str(last["year"]), last["id"])
        else:
            next_cursor = encode_cursor(str(last.get("relevance") or 0.0), last["id"])

    return BillsListResponse(
        bills=[BillListItem.model_validate(item) for item in rows],
        facets=_build_facets(scoped),
        meta=ListMeta(
            sortBy=sort_by,
            sortDir=sort_dir,
            pageSize=page_size,
            cursor=cursor,
            nextCursor=next_cursor,
            hasMore=has_more,
            totalCount=len(scoped),
        ),
    )


def get_bill_detail(bill_pk: str) -> BillDetailResponse | None:
    record = MOCK_BILL_DETAILS_BY_PK.get(bill_pk)
    if record is None:
        return None
    return BillDetailResponse.model_validate(record)

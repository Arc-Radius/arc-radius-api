"""Neo4j-backed UI responses (parameterized Cypher only)."""

from __future__ import annotations

from typing import Any

from src.db.cypher import ui_queries as Q
from src.db.neo4j_client import neo4j_read
from src.models.ui import (
    BillDetail,
    BillDetailResponse,
    BillFacets,
    BillListItem,
    BillTab,
    BillsListResponse,
    GraphBillRecord,
    LegislativeStatus,
    ListMeta,
    SortBy,
    SortDir,
    StateCounts,
    StateRow,
    StatesResponse,
)
from src.services.bill_cursor import decode_cursor, encode_cursor

_STATE_NAMES: dict[str, str] = dict(
    [
        ("AL", "Alabama"),
        ("AK", "Alaska"),
        ("AZ", "Arizona"),
        ("AR", "Arkansas"),
        ("CA", "California"),
        ("CO", "Colorado"),
        ("CT", "Connecticut"),
        ("DE", "Delaware"),
        ("DC", "District of Columbia"),
        ("FL", "Florida"),
        ("GA", "Georgia"),
        ("HI", "Hawaii"),
        ("ID", "Idaho"),
        ("IL", "Illinois"),
        ("IN", "Indiana"),
        ("IA", "Iowa"),
        ("KS", "Kansas"),
        ("KY", "Kentucky"),
        ("LA", "Louisiana"),
        ("ME", "Maine"),
        ("MD", "Maryland"),
        ("MA", "Massachusetts"),
        ("MI", "Michigan"),
        ("MN", "Minnesota"),
        ("MS", "Mississippi"),
        ("MO", "Missouri"),
        ("MT", "Montana"),
        ("NE", "Nebraska"),
        ("NV", "Nevada"),
        ("NH", "New Hampshire"),
        ("NJ", "New Jersey"),
        ("NM", "New Mexico"),
        ("NY", "New York"),
        ("NC", "North Carolina"),
        ("ND", "North Dakota"),
        ("OH", "Ohio"),
        ("OK", "Oklahoma"),
        ("OR", "Oregon"),
        ("PA", "Pennsylvania"),
        ("RI", "Rhode Island"),
        ("SC", "South Carolina"),
        ("SD", "South Dakota"),
        ("TN", "Tennessee"),
        ("TX", "Texas"),
        ("UT", "Utah"),
        ("VT", "Vermont"),
        ("VA", "Virginia"),
        ("WA", "Washington"),
        ("WV", "West Virginia"),
        ("WI", "Wisconsin"),
        ("WY", "Wyoming"),
    ]
)


def _stance(raw: str | None) -> LegislativeStatus:
    if raw in ("supportive", "harmful", "mixed"):
        return LegislativeStatus(raw)
    return LegislativeStatus.mixed


def _numeric_route_key(bill_pk: str) -> bool:
    return bill_pk.isdigit()


def _normalize_graph_props(props: dict[str, Any]) -> dict[str, Any]:
    out = dict(props)
    bid = out.get("bill_id")
    if bid is not None and not isinstance(bid, str):
        out["bill_id"] = str(bid)
    return out


def _bill_match_params(bill_pk: str) -> dict[str, Any]:
    return {"billPk": bill_pk, "numericOnly": _numeric_route_key(bill_pk)}


async def list_states_neo4j(*, include_counts: bool) -> StatesResponse:
    rows = await neo4j_read(Q.STATES_LIST)
    states: list[StateRow] = []
    for row in rows:
        s = row.get("state") or {}
        abbr = str(s.get("abbr", "")).upper()
        name = _STATE_NAMES.get(abbr, abbr)
        counts_raw = s.get("counts")
        counts: StateCounts | None = None
        if include_counts and isinstance(counts_raw, dict):
            counts = StateCounts(
                activeBills=int(counts_raw.get("activeBills", 0)),
                passedBills=int(counts_raw.get("passedBills", 0)),
            )
        states.append(
            StateRow(
                abbr=abbr,
                name=name,
                status=_stance(s.get("status")),
                legislature=str(s.get("legislature", "")),
                session=str(s.get("session", "")),
                sessionWindow=str(s.get("sessionWindow", "")),
                stateLink=str(s.get("stateLink", "")),
                lastUpdated=str(s.get("lastUpdated", "")),
                counts=counts,
            )
        )
    return StatesResponse(states=states)


def _facets_from_neo4j(
    raw: dict[str, Any] | None,
    category_rows: list[dict[str, Any]] | None,
) -> BillFacets:
    raw = raw or {}
    stances: dict[LegislativeStatus, int] = {k: 0 for k in LegislativeStatus}
    for pair in raw.get("stancePairs") or []:
        if isinstance(pair, (list, tuple)) and len(pair) >= 2:
            try:
                stances[_stance(str(pair[0]))] = int(pair[1])
            except (TypeError, ValueError):
                continue
    categories: dict[str, int] = {}
    for row in category_rows or []:
        cat = row.get("category")
        if cat is not None and str(cat).strip() != "":
            categories[str(cat)] = int(row.get("cnt", 0))
    years: dict[str, int] = {}
    for pair in raw.get("yearPairs") or []:
        if isinstance(pair, (list, tuple)) and len(pair) >= 2:
            years[str(pair[0])] = int(pair[1])
    return BillFacets(stances=stances, categories=categories, years=years)


async def list_bills_neo4j(
    *,
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
    cursor_tuple = decode_cursor(cursor)
    cursor_sort_value = cursor_tuple[0] if cursor_tuple else None
    cursor_bill_pk = cursor_tuple[1] if cursor_tuple else None

    params_base: dict[str, Any] = {
        "state": normalized_state,
        "stances": [s.value for s in stances] if stances else [],
        "categories": list(categories) if categories else [],
        "years": list(years) if years else [],
        "tab": tab.value if tab else None,
        "sortBy": sort_by.value,
        "sortDir": sort_dir.value,
        "pageSize": page_size,
    }

    facets_rows = await neo4j_read(
        Q.BILLS_FACETS,
        {**params_base},
    )
    facets_raw = facets_rows[0]["facets"] if facets_rows else None
    category_rows = await neo4j_read(
        Q.BILLS_FACET_CATEGORY_COUNTS,
        {**params_base},
    )
    facets = _facets_from_neo4j(facets_raw, category_rows)
    total_count = int((facets_raw or {}).get("totalCount", 0))

    list_rows = await neo4j_read(
        Q.BILLS_LIST_PAGE,
        {
            **params_base,
            "cursorSortValue": cursor_sort_value,
            "cursorBillPk": cursor_bill_pk,
            "fetchLimit": page_size + 1,
        },
    )
    raw_items: list[dict[str, Any]] = list_rows[0].get("rows") or [] if list_rows else []

    has_more = len(raw_items) > page_size
    page_items = raw_items[:page_size]

    bills: list[BillListItem] = []
    for item in page_items:
        row = {k: v for k, v in item.items() if k != "_sortValue"}
        row["stance"] = _stance(row.get("stance")).value
        row["billTab"] = row.get("billTab", "active")
        if row.get("issue_categories") is None:
            row["issue_categories"] = []
        bills.append(BillListItem.model_validate(row))

    next_cursor: str | None = None
    if has_more and page_items:
        last = page_items[-1]
        sv = str(last.get("_sortValue", ""))
        bid = str(last.get("id", ""))
        next_cursor = encode_cursor(sv, bid)

    return BillsListResponse(
        bills=bills,
        facets=facets,
        meta=ListMeta(
            sortBy=sort_by,
            sortDir=sort_dir,
            pageSize=page_size,
            cursor=cursor,
            nextCursor=next_cursor,
            hasMore=has_more,
            totalCount=total_count,
        ),
    )


async def get_bill_detail_neo4j(bill_pk: str) -> BillDetailResponse | None:
    rows = await neo4j_read(Q.BILL_DETAIL, _bill_match_params(bill_pk))
    if not rows or not rows[0].get("payload"):
        return None
    payload = rows[0]["payload"]
    bill = dict(payload["bill"])
    sponsors = [s for s in (bill.get("sponsors") or []) if (s or {}).get("name")]
    bill["sponsors"] = sponsors
    graph_raw = _normalize_graph_props(dict(payload.get("graphRecord") or {}))
    return BillDetailResponse(
        bill=BillDetail.model_validate(bill),
        graphRecord=GraphBillRecord.model_validate(graph_raw),
    )

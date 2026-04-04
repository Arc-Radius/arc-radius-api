"""
Ingest bill metadata and relational data from LegiScan bulk CSVs into Neo4j. Filter to matched bills from EDA matched_lgbtq_bills.csv
"""

from graph.api.neo4j_client import Neo4j
import ast
import csv
import re
from pathlib import Path


# paths and constants
REPO_ROOT = Path(__file__).resolve().parents[2]

# path to matched bills CSV
MATCHED_CSV = (
    REPO_ROOT / "datasources" / "final-outputs" / "matched_lgbtq_bills.csv"
)
# path to root of bulk CSV directory
BULK_CSV_ROOT = REPO_ROOT / "datasources" / "legiscan-bulk-csv"

# required files in each bulk CSV directory
REQUIRED_FILES = [
    "bills.csv", "people.csv", "sponsors.csv",
    "history.csv", "documents.csv", "rollcalls.csv",
]
BATCH_SIZE = 500

# fiels that are only present in the matched bills CSV
EDA_FIELDS = [
    "label", "label_source", "year", "state_lean", "bill_dominant_party",
    "state_r_sponsorship_ratio", "pass_rate_gap",
    "overall_pass_rate", "session_year",
    "issues", "issue_categories",
]

FALLBACK_FIELDS = [
    "state", "session_id", "bill_number",
    "status", "status_desc", "status_date",
    "title", "description",
    "last_action_date", "last_action",
    "url", "state_link",
    "document_id", "document_type", "document_url",
]

# cypher for nodes
# bill nodes
BILL_CYPHER = """
UNWIND $rows AS row
MERGE (b:Bill {bill_pk: row.bill_pk})
SET b.state          = row.state,
    b.session_id     = row.session_id,
    b.bill_id        = row.bill_id,
    b.bill_number    = row.bill_number,
    b.status         = row.status,
    b.status_desc    = row.status_desc,
    b.status_date    = row.status_date,
    b.title          = row.title,
    b.description    = row.description,
    b.committee_id   = row.committee_id,
    b.last_action_date = row.last_action_date,
    b.last_action    = row.last_action,
    b.url            = row.url,
    b.state_link     = row.state_link,
    b.label          = row.label,
    b.label_source   = row.label_source,
    b.year           = row.year,
    b.state_lean     = row.state_lean,
    b.bill_dominant_party = row.bill_dominant_party,
    b.state_r_sponsorship_ratio = row.state_r_sponsorship_ratio,
    b.pass_rate_gap  = row.pass_rate_gap,
    b.overall_pass_rate = row.overall_pass_rate,
    b.session_year   = row.session_year,
    b.issues         = row.issues
"""

# person nodes
PERSON_CYPHER = """
UNWIND $rows AS row
MERGE (p:Person {people_id: row.people_id})
SET p.name       = row.name,
    p.first_name = row.first_name,
    p.last_name  = row.last_name,
    p.party_id   = row.party_id,
    p.party      = row.party,
    p.role_id    = row.role_id,
    p.role       = row.role,
    p.district   = row.district,
    p.state      = row.state
"""

# committee nodes
COMMITTEE_CYPHER = """
UNWIND $rows AS row
MERGE (cm:Committee {committee_id: row.committee_id})
SET cm.name = row.name
"""

# document nodes
DOCUMENT_CYPHER = """
UNWIND $rows AS row
MERGE (d:Document {document_id: row.document_id})
SET d.document_type = row.document_type,
    d.document_size = row.document_size,
    d.document_mime = row.document_mime,
    d.document_desc = row.document_desc,
    d.url           = row.url,
    d.state_link    = row.state_link
"""

# rollcall nodes
ROLLCALL_CYPHER = """
UNWIND $rows AS row
MERGE (r:RollCall {roll_call_id: row.roll_call_id})
SET r.date        = row.date,
    r.chamber     = row.chamber,
    r.description = row.description,
    r.yea         = row.yea,
    r.nay         = row.nay,
    r.nv          = row.nv,
    r.absent      = row.absent,
    r.total       = row.total
"""

# action nodes
ACTION_CYPHER = """
UNWIND $rows AS row
MERGE (a:Action {action_id: row.action_id})
SET a.date     = row.date,
    a.chamber  = row.chamber,
    a.sequence = row.sequence,
    a.action   = row.action
"""

# cypher for relationships
# sponsors relationship
SPONSORS_REL = """
UNWIND $rows AS row
MATCH (p:Person {people_id: row.people_id})
MATCH (b:Bill {bill_pk: row.bill_pk})
MERGE (p)-[s:SPONSORS]->(b)
SET s.position = row.position
"""

# referred to relationship
REFERRED_REL = """
UNWIND $rows AS row
MATCH (b:Bill {bill_pk: row.bill_pk})
MATCH (cm:Committee {committee_id: row.committee_id})
MERGE (b)-[:REFERRED_TO]->(cm)
"""

# has document relationship
HAS_DOC_REL = """
UNWIND $rows AS row
MATCH (b:Bill {bill_pk: row.bill_pk})
MATCH (d:Document {document_id: row.document_id})
MERGE (b)-[:HAS_DOCUMENT]->(d)
"""

# has rollcall relationship
HAS_ROLLCALL_REL = """
UNWIND $rows AS row
MATCH (b:Bill {bill_pk: row.bill_pk})
MATCH (r:RollCall {roll_call_id: row.roll_call_id})
MERGE (b)-[:HAS_ROLLCALL]->(r)
"""

# voted relationship
VOTED_REL = """
UNWIND $rows AS row
MATCH (p:Person {people_id: row.people_id})
MATCH (r:RollCall {roll_call_id: row.roll_call_id})
MERGE (p)-[v:VOTED]->(r)
SET v.vote      = row.vote,
    v.vote_desc = row.vote_desc
"""

# has action relationship
HAS_ACTION_REL = """
UNWIND $rows AS row
MATCH (b:Bill {bill_pk: row.bill_pk})
MATCH (a:Action {action_id: row.action_id})
MERGE (b)-[:HAS_ACTION]->(a)
"""

# topic nodes
TOPIC_CYPHER = """
UNWIND $rows AS row
MERGE (t:Topic {name: row.name})
"""

# state nodes
STATE_CYPHER = """
UNWIND $rows AS row
MERGE (s:State {code: row.code})
"""

# session nodes
SESSION_CYPHER = """
UNWIND $rows AS row
MERGE (sn:Session {session_pk: row.session_pk})
SET sn.session_id = row.session_id,
    sn.state_code = row.state_code
"""

# has topic relationship
HAS_TOPIC_REL = """
UNWIND $rows AS row
MATCH (b:Bill {bill_pk: row.bill_pk})
MATCH (t:Topic {name: row.topic})
MERGE (b)-[:HAS_TOPIC]->(t)
"""

# in-state relationship
IN_STATE_REL = """
UNWIND $rows AS row
MATCH (b:Bill {bill_pk: row.bill_pk})
MATCH (s:State {code: row.code})
MERGE (b)-[:IN_STATE]->(s)
"""

# in-session relationship
IN_SESSION_REL = """
UNWIND $rows AS row
MATCH (b:Bill {bill_pk: row.bill_pk})
MATCH (sn:Session {session_pk: row.session_pk})
MERGE (b)-[:IN_SESSION]->(sn)
"""

# state-has-session relationship
HAS_SESSION_REL = """
UNWIND $rows AS row
MATCH (s:State {code: row.state_code})
MATCH (sn:Session {session_pk: row.session_pk})
MERGE (s)-[:HAS_SESSION]->(sn)
"""

# ---------------------------------------------------------------------------
# helpers for csv ingestion gathering docs etc
# remove non-letters from state name


def _letters_only(s: str) -> str:
    return re.sub(r"[^A-Za-z]", "", s or "")


def _normalize_state_code(s: str) -> str:
    code = _letters_only(s).upper()
    return code[:2] if len(code) >= 2 else ""


def _normalize_session_id(s: str) -> str:
    return str(s or "").strip()

# discover csv directories


def _discover_csv_dirs(root: Path) -> list[Path]:
    dirs = []
    for dirpath in root.rglob("*"):
        if dirpath.is_dir() and all((dirpath / f).is_file() for f in REQUIRED_FILES):
            dirs.append(dirpath)
    dirs.sort()
    return dirs

# load matched bills CSV from EDA into bill_ids and eda dict


def _parse_list_field(val: str) -> list[str]:
    """Parse a string-encoded list like "['a', 'b']" back into a Python list."""
    if not val:
        return []
    try:
        parsed = ast.literal_eval(val)
        return list(parsed) if isinstance(parsed, (list, tuple)) else []
    except (ValueError, SyntaxError):
        return []


def _load_matched(path: Path) -> tuple[set[int], dict[int, dict]]:
    """Return (bill_id_set, {bill_id: eda_fields + _fallback dict})."""
    bill_ids: set[int] = set()
    eda: dict[int, dict] = {}

    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            bid = int(row["bill_id"])
            bill_ids.add(bid)
            eda[bid] = {k: row.get(k, "") for k in EDA_FIELDS}
            eda[bid]["_fallback"] = {k: row.get(
                k, "") for k in FALLBACK_FIELDS}

    return bill_ids, eda

# gather bulk CSV data from directory, filter to matched bills, return all entities


def _gather_bulk(csv_dirs: list[Path], bill_ids: set[int]) -> dict:
    bills: dict[int, dict] = {}
    people: dict[int, dict] = {}
    committees: dict[int, dict] = {}
    sponsors: list[dict] = []
    documents: list[dict] = []
    rollcalls: list[dict] = []
    votes: list[dict] = []
    history: list[dict] = []

    people_ids_needed: set[int] = set()
    rc_ids: set[int] = set()

    for csv_dir in csv_dirs:
        state = _letters_only(csv_dir.parent.parent.name)

        # bills
        with open(csv_dir / "bills.csv", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                bid = int(row["bill_id"])
                if bid not in bill_ids:
                    continue
                row["state"] = state
                bills[bid] = dict(row)
                cid_raw = row.get("committee_id", "0") or "0"
                cid = int(cid_raw)
                if cid:
                    committees.setdefault(cid, {
                        "committee_id": cid,
                        "name": row.get("committee", ""),
                    })

        # sponsors
        with open(csv_dir / "sponsors.csv", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                bid = int(row["bill_id"])
                if bid not in bill_ids:
                    continue
                pid = int(row["people_id"])
                sponsors.append({
                    "bill_id": bid,
                    "people_id": pid,
                    "position": int(row.get("position", 0)),
                })
                people_ids_needed.add(pid)

        # documents
        with open(csv_dir / "documents.csv", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                bid = int(row["bill_id"])
                if bid not in bill_ids:
                    continue
                documents.append({
                    "bill_id": bid,
                    "document_id": str(row["document_id"]),
                    "document_type": row.get("document_type", ""),
                    "document_size": row.get("document_size", ""),
                    "document_mime": row.get("document_mime", ""),
                    "document_desc": row.get("document_desc", ""),
                    "url": row.get("url", ""),
                    "state_link": row.get("state_link", ""),
                })

        # history
        with open(csv_dir / "history.csv", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                bid = int(row["bill_id"])
                if bid not in bill_ids:
                    continue
                history.append({
                    "bill_id": bid,
                    "date": row.get("date", ""),
                    "chamber": row.get("chamber", ""),
                    "sequence": int(row.get("sequence", 0)),
                    "action": row.get("action", ""),
                })

        # rollcalls
        with open(csv_dir / "rollcalls.csv", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                bid = int(row["bill_id"])
                if bid not in bill_ids:
                    continue
                rcid = int(row["roll_call_id"])
                rollcalls.append({
                    "bill_id": bid,
                    "roll_call_id": rcid,
                    "date": row.get("date", ""),
                    "chamber": row.get("chamber", ""),
                    "description": row.get("description", ""),
                    "yea": int(row.get("yea", 0)),
                    "nay": int(row.get("nay", 0)),
                    "nv": int(row.get("nv", 0)),
                    "absent": int(row.get("absent", 0)),
                    "total": int(row.get("total", 0)),
                })
                rc_ids.add(rcid)

        # votes
        votes_path = csv_dir / "votes.csv"
        if votes_path.exists():
            with open(votes_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    rcid = int(row["roll_call_id"])
                    if rcid not in rc_ids:
                        continue
                    pid = int(row["people_id"])
                    votes.append({
                        "roll_call_id": rcid,
                        "people_id": pid,
                        "vote": int(row.get("vote", 0)),
                        "vote_desc": row.get("vote_desc", ""),
                    })
                    people_ids_needed.add(pid)

        # people (only those referenced by sponsors or votes in this dir)
        with open(csv_dir / "people.csv", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pid = int(row["people_id"])
                if pid not in people_ids_needed or pid in people:
                    continue
                people[pid] = {
                    "people_id": pid,
                    "name": row.get("name", ""),
                    "first_name": row.get("first_name", ""),
                    "last_name": row.get("last_name", ""),
                    "party_id": row.get("party_id", ""),
                    "party": row.get("party", ""),
                    "role_id": row.get("role_id", ""),
                    "role": row.get("role", ""),
                    "district": row.get("district", ""),
                    "state": state,
                }

    return {
        "bills": bills,
        "people": people,
        "committees": committees,
        "sponsors": sponsors,
        "documents": documents,
        "rollcalls": rollcalls,
        "votes": votes,
        "history": history,
    }


# helpers for ingestion using Cypher batching
# batch items into lists of size BATCH_SIZE
def _batched(items: list, size: int = BATCH_SIZE):
    for i in range(0, len(items), size):
        yield items[i: i + size]

# ingest nodes and relationships


def _ingest(db: Neo4j, cypher: str, rows: list[dict], label: str):
    total = 0
    for batch in _batched(rows):
        db.run_batch(cypher, batch)
        total += len(batch)
    print(f"  {label}: {total}")


def main():
    # load matched bills from EDA matched_lgbtq_bills.csv
    print("Loading matched bills...")
    bill_ids, eda = _load_matched(MATCHED_CSV)
    print(f"  {len(bill_ids)} bill_ids from matched CSV")

    # discover and gather bulk CSV data
    csv_dirs = _discover_csv_dirs(BULK_CSV_ROOT)
    print(f"  {len(csv_dirs)} bulk CSV directories found")

    print("Gathering bulk CSV data (filtering to matched bills)...")
    bulk = _gather_bulk(csv_dirs, bill_ids)
    # print counts of each entity type for sanity check
    print(
        f"  bills={len(bulk['bills'])}  people={len(bulk['people'])}  "
        f"committees={len(bulk['committees'])}  sponsors={len(bulk['sponsors'])}  "
        f"documents={len(bulk['documents'])}  rollcalls={len(bulk['rollcalls'])}  "
        f"votes={len(bulk['votes'])}  history={len(bulk['history'])}"
    )

    # build bill_pk lookup
    bill_pk: dict[int, str] = {}
    for bid, bdata in bulk["bills"].items():
        bill_pk[bid] = f"{bdata['state']}:{bdata['session_id']}:{bid}"

    # build bill rows
    bill_rows: list[dict] = []

    for bid, bdata in bulk["bills"].items():
        e = eda.get(bid, {})
        bill_rows.append({
            "bill_pk": bill_pk[bid],
            "state": bdata.get("state", ""),
            "session_id": bdata.get("session_id", ""),
            "bill_id": bid,
            "bill_number": bdata.get("bill_number", ""),
            "status": bdata.get("status", ""),
            "status_desc": bdata.get("status_desc", ""),
            "status_date": bdata.get("status_date", ""),
            "title": bdata.get("title", ""),
            "description": bdata.get("description", ""),
            "committee_id": bdata.get("committee_id", ""),
            "last_action_date": bdata.get("last_action_date", ""),
            "last_action": bdata.get("last_action", ""),
            "url": bdata.get("url", ""),
            "state_link": bdata.get("state_link", ""),
            **{k: e.get(k, "") for k in EDA_FIELDS},
        })

    # fallback bills not found in bulk CSVs
    fallback_ids = bill_ids - set(bulk["bills"].keys())
    fallback_doc_rows: list[dict] = []
    fallback_doc_rels: list[dict] = []

    # build fallback bill rows for bills not found in bulk CSVs
    for bid in fallback_ids:
        e = eda.get(bid, {})
        fb = e.get("_fallback", {})
        pk = f"{fb.get('state', '')}:{fb.get('session_id', '')}:{bid}"
        bill_pk[bid] = pk
        bill_rows.append({
            "bill_pk": pk,
            "state": fb.get("state", ""),
            "session_id": fb.get("session_id", ""),
            "bill_id": bid,
            "bill_number": fb.get("bill_number", ""),
            "status": fb.get("status", ""),
            "status_desc": fb.get("status_desc", ""),
            "status_date": fb.get("status_date", ""),
            "title": fb.get("title", ""),
            "description": fb.get("description", ""),
            "committee_id": "",
            "last_action_date": fb.get("last_action_date", ""),
            "last_action": fb.get("last_action", ""),
            "url": fb.get("url", ""),
            "state_link": fb.get("state_link", ""),
            **{k: e.get(k, "") for k in EDA_FIELDS},
        })
        doc_id = fb.get("document_id", "")
        if doc_id:
            fallback_doc_rows.append({
                "document_id": str(doc_id),
                "document_type": fb.get("document_type", ""),
                "document_size": "",
                "document_mime": "",
                "document_desc": "",
                "url": fb.get("document_url", ""),
                "state_link": "",
            })
            fallback_doc_rels.append({
                "bill_pk": pk,
                "document_id": str(doc_id),
            })

    if fallback_ids:
        print(f"  {len(fallback_ids)} bills from fallback (no bulk CSV coverage)")

    # build relationship rows
    person_rows = list(bulk["people"].values())
    committee_rows = list(bulk["committees"].values())

    doc_rows = bulk["documents"] + fallback_doc_rows

    rollcall_rows = bulk["rollcalls"]
    vote_rows = bulk["votes"]

    action_rows = []
    for h in bulk["history"]:
        action_rows.append({
            "action_id": f"{h['bill_id']}:{h['sequence']}",
            "date": h["date"],
            "chamber": h["chamber"],
            "sequence": h["sequence"],
            "action": h["action"],
        })

    sponsor_rels = []
    for s in bulk["sponsors"]:
        pk = bill_pk.get(s["bill_id"])
        if pk:
            sponsor_rels.append({
                "people_id": s["people_id"],
                "bill_pk": pk,
                "position": s["position"],
            })

    referred_rels = []
    for bid, bdata in bulk["bills"].items():
        cid_raw = bdata.get("committee_id", "0") or "0"
        cid = int(cid_raw)
        if cid:
            referred_rels.append({
                "bill_pk": bill_pk[bid],
                "committee_id": cid,
            })

    doc_rels = []
    for d in bulk["documents"]:
        pk = bill_pk.get(d["bill_id"])
        if pk:
            doc_rels.append({"bill_pk": pk, "document_id": d["document_id"]})
    doc_rels.extend(fallback_doc_rels)

    rc_rels = []
    for rc in bulk["rollcalls"]:
        pk = bill_pk.get(rc["bill_id"])
        if pk:
            rc_rels.append({"bill_pk": pk, "roll_call_id": rc["roll_call_id"]})

    action_rels = []
    for h in bulk["history"]:
        pk = bill_pk.get(h["bill_id"])
        if pk:
            action_rels.append({
                "bill_pk": pk,
                "action_id": f"{h['bill_id']}:{h['sequence']}",
            })

    # build topic nodes and HAS_TOPIC rels from issue_categories
    topic_names: set[str] = set()
    topic_rels: list[dict] = []

    for br in bill_rows:
        cats = _parse_list_field(br.get("issue_categories", ""))
        for cat in cats:
            topic_names.add(cat)
            topic_rels.append({"bill_pk": br["bill_pk"], "topic": cat})

    topic_rows = [{"name": n} for n in sorted(topic_names)]
    state_codes = sorted(
        {_normalize_state_code(br.get("state", "")) for br in bill_rows}
        - {""}
    )
    state_rows = [{"code": code} for code in state_codes]
    in_state_rels = []
    for br in bill_rows:
        code = _normalize_state_code(br.get("state", ""))
        if code:
            in_state_rels.append({"bill_pk": br["bill_pk"], "code": code})
    session_keys: set[tuple[str, str]] = set()
    session_rows: list[dict] = []
    in_session_rels: list[dict] = []
    has_session_rels: list[dict] = []
    for br in bill_rows:
        state_code = _normalize_state_code(br.get("state", ""))
        session_id = _normalize_session_id(br.get("session_id", ""))
        if not state_code or not session_id:
            continue
        session_pk = f"{state_code}:{session_id}"
        if (state_code, session_id) not in session_keys:
            session_keys.add((state_code, session_id))
            session_rows.append({
                "session_pk": session_pk,
                "session_id": session_id,
                "state_code": state_code,
            })
            has_session_rels.append({
                "state_code": state_code,
                "session_pk": session_pk,
            })
        in_session_rels.append({
            "bill_pk": br["bill_pk"],
            "session_pk": session_pk,
        })

    # ingest into Neo4j! Hooray!
    db = Neo4j()
    print("Ingesting nodes...")
    _ingest(db, BILL_CYPHER, bill_rows, "Bill")
    _ingest(db, STATE_CYPHER, state_rows, "State")
    _ingest(db, SESSION_CYPHER, session_rows, "Session")
    _ingest(db, PERSON_CYPHER, person_rows, "Person")
    _ingest(db, COMMITTEE_CYPHER, committee_rows, "Committee")
    _ingest(db, DOCUMENT_CYPHER, doc_rows, "Document")
    _ingest(db, ROLLCALL_CYPHER, rollcall_rows, "RollCall")
    _ingest(db, ACTION_CYPHER, action_rows, "Action")
    _ingest(db, TOPIC_CYPHER, topic_rows, "Topic")

    print("Ingesting relationships...")
    _ingest(db, SPONSORS_REL, sponsor_rels, "SPONSORS")
    _ingest(db, REFERRED_REL, referred_rels, "REFERRED_TO")
    _ingest(db, HAS_DOC_REL, doc_rels, "HAS_DOCUMENT")
    _ingest(db, HAS_ROLLCALL_REL, rc_rels, "HAS_ROLLCALL")
    _ingest(db, VOTED_REL, vote_rows, "VOTED")
    _ingest(db, HAS_ACTION_REL, action_rels, "HAS_ACTION")
    _ingest(db, HAS_TOPIC_REL, topic_rels, "HAS_TOPIC")
    _ingest(db, IN_STATE_REL, in_state_rels, "IN_STATE")
    _ingest(db, IN_SESSION_REL, in_session_rels, "IN_SESSION")
    _ingest(db, HAS_SESSION_REL, has_session_rels, "HAS_SESSION")

    db.close()
    # Total success!
    print("[OK] metadata ingestion complete")


if __name__ == "__main__":
    main()

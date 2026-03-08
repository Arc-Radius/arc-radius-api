"""
Arc Radius — LegiScan Polling Lambda
======================================
EventBridge triggers this on a schedule. It:
  1. Loads known bill_ids + change_hashes from S3
  2. Calls getDatasetList (1 API call) to find changed sessions
  3. Calls getMasterListRaw only for changed sessions
  4. Detects NEW bills and UPDATED bills (hash changed)
  5. Calls getBill for each new/updated bill
  6. Saves CSVs to S3, updates known_ids + session hashes

API cost:
  - Quiet day: 1 call (getDatasetList)
  - Active day: 1 + ~5 getMasterListRaw + N getBill

Deploy as a Lambda with:
  - Runtime: Python 3.14
  - Timeout: 15 minutes
  - Memory: 512 MB
  - Environment variables:
      LEGISCAN_API_KEY = your key
      BUCKET = arc-radius-s3-bucket
      STATES = TX,CA,FL  (comma-separated, or ALL)
"""

import json
import os
import time
from datetime import datetime, timezone

import boto3
import requests

# ─── Config from environment ──────────────────────────────
API_KEY = os.environ["LEGISCAN_API_KEY"]
BUCKET = os.environ.get("BUCKET", "arc-radius-s3-bucket")
REGION = os.environ.get("AWS_REGION", "us-east-1")
STATES_ENV = os.environ.get("STATES", "CA")

API_URL = "https://api.legiscan.com/"
KNOWN_IDS_KEY = "pipeline/metadata/known_bill_ids.json"
SESSION_HASHES_KEY = "pipeline/metadata/session_hashes.json"
OUTPUT_PREFIX = "raw/legiscan-incremental/"
MIN_YEAR = 2021

s3 = boto3.client("s3", region_name=REGION)


# ─── LegiScan API ─────────────────────────────────────────

def api_call(op, **params):
    params["key"] = API_KEY
    params["op"] = op
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK":
        raise RuntimeError(
            f"LegiScan {op}: {data.get('alert', {}).get('message', 'error')}")
    time.sleep(0.4)
    return data


# ─── S3 helpers ───────────────────────────────────────────

def load_json_from_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def save_json_to_s3(key, data):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json",
    )


def load_known_ids():
    """Load {bill_id: change_hash_or_None}."""
    data = load_json_from_s3(KNOWN_IDS_KEY)
    if data is None:
        return {}
    if isinstance(data, list):
        return {str(bid): None for bid in data}
    return {str(k): v for k, v in data.items()}


def load_session_hashes():
    """
    Load session hashes from S3.
    Format: {session_id: {dataset_hash, state, session_name, ...}}
    """
    data = load_json_from_s3(SESSION_HASHES_KEY)
    if data is None:
        return {}
    # Handle old simple format {session_id: hash_string}
    upgraded = {}
    for sid, val in data.items():
        if isinstance(val, str):
            upgraded[sid] = {"dataset_hash": val}
        else:
            upgraded[sid] = val
    return upgraded


def save_bills_csv(rows, label):
    """Save bills as CSV to S3."""
    if not rows:
        return None

    cols = list(rows[0].keys())
    lines = [",".join(cols)]
    for row in rows:
        vals = []
        for c in cols:
            v = str(row.get(c, "")).replace('"', '""')
            if "," in v or '"' in v or "\n" in v:
                v = f'"{v}"'
            vals.append(v)
        lines.append(",".join(vals))

    csv_body = "\n".join(lines)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{OUTPUT_PREFIX}{label}_bills_{timestamp}.csv"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=csv_body,
        ContentType="text/csv",
    )
    return key


# ─── State ID map ─────────────────────────────────────────

STATE_ID_MAP = {
    1: "AL", 2: "AK", 3: "AZ", 4: "AR", 5: "CA", 6: "CO", 7: "CT",
    8: "DE", 9: "FL", 10: "GA", 11: "HI", 12: "ID", 13: "IL", 14: "IN",
    15: "IA", 16: "KS", 17: "KY", 18: "LA", 19: "ME", 20: "MD",
    21: "MA", 22: "MI", 23: "MN", 24: "MS", 25: "MO", 26: "MT",
    27: "NE", 28: "NV", 29: "NH", 30: "NJ", 31: "NM", 32: "NY",
    33: "NC", 34: "ND", 35: "OH", 36: "OK", 37: "OR", 38: "PA",
    39: "RI", 40: "SC", 41: "SD", 42: "TN", 43: "TX", 44: "UT",
    45: "VT", 46: "VA", 47: "WA", 48: "WV", 49: "WI", 50: "WY",
    51: "DC",
}


def get_state_filter():
    if STATES_ENV == "ALL":
        return None
    return {s.strip().upper() for s in STATES_ENV.split(",")}


# ─── Core logic ───────────────────────────────────────────

def find_changed_sessions(stored_sessions):
    """
    Call getDatasetList (1 API call).
    Compare dataset_hash per session against stored values.
    Returns list of changed sessions.
    """
    now = datetime.now(timezone.utc).isoformat()
    state_filter = get_state_filter()

    data = api_call("getDatasetList")
    all_sessions = data.get("datasetlist", [])

    changed = []
    unchanged = 0
    skipped = 0

    for session in all_sessions:
        sid = str(session["session_id"])
        state_id = session.get("state_id", 0)
        state_abbr = STATE_ID_MAP.get(state_id, "??")
        session_name = session.get("session_name", "")
        year_start = session.get("year_start", 0)
        current_hash = session.get("dataset_hash", "")

        # Filter by year
        if year_start < MIN_YEAR:
            skipped += 1
            continue

        # Filter by state if specified
        if state_filter and state_abbr not in state_filter:
            skipped += 1
            continue

        stored = stored_sessions.get(sid, {})
        stored_hash = stored.get("dataset_hash") if isinstance(
            stored, dict) else stored

        # Update stored session info (always, so we track last_checked)
        stored_sessions[sid] = {
            "dataset_hash": current_hash,
            "state": state_abbr,
            "state_id": state_id,
            "session_name": session_name,
            "year_start": year_start,
            "year_end": session.get("year_end", 0),
            "last_checked": now,
            "last_changed": now if stored_hash != current_hash else stored.get("last_changed", now) if isinstance(stored, dict) else now,
        }

        if stored_hash == current_hash:
            unchanged += 1
            continue

        changed.append({
            "session_id": session["session_id"],
            "state": state_abbr,
            "session_name": session_name,
            "year_start": year_start,
            "dataset_hash": current_hash,
        })

    print(
        f"  Sessions — changed: {len(changed)}, unchanged: {unchanged}, skipped: {skipped}")
    for s in changed:
        print(f"    CHANGED: {s['state']} — {s['session_name']}")

    return changed


def find_new_and_updated_bills(changed_sessions, known_ids):
    """
    Call getMasterListRaw for each changed session.
    Compare bill change_hashes against known_ids.
    """
    new_ids = set()
    updated_ids = set()
    hashes_backfilled = 0
    api_calls = 0

    for session in changed_sessions:
        sid = session["session_id"]
        state = session["state"]

        try:
            master = api_call("getMasterListRaw", id=sid)
            api_calls += 1
        except Exception as e:
            print(f"  {state} session {sid}: getMasterListRaw failed: {e}")
            continue

        session_new = 0
        session_updated = 0

        for key, info in master.get("masterlist", {}).items():
            if key == "session":
                continue

            bid = str(info["bill_id"])
            current_hash = info.get("change_hash", "")

            if bid not in known_ids:
                new_ids.add(bid)
                known_ids[bid] = current_hash
                session_new += 1

            elif known_ids[bid] is None:
                known_ids[bid] = current_hash
                hashes_backfilled += 1

            elif known_ids[bid] != current_hash:
                updated_ids.add(bid)
                known_ids[bid] = current_hash
                session_updated += 1

        print(f"  {state} ({session['session_name']}): "
              f"+{session_new} new, {session_updated} updated")

    print(f"  getMasterListRaw calls: {api_calls}")
    print(f"  Hashes backfilled: {hashes_backfilled}")
    return new_ids, updated_ids, api_calls


def fetch_bills(bill_ids, label):
    """Call getBill for each bill_id, return flat rows."""
    if not bill_ids:
        return []

    print(f"  Fetching {len(bill_ids)} {label} bills...")
    rows = []
    for i, bid in enumerate(sorted(bill_ids)):
        try:
            data = api_call("getBill", id=int(bid))
            bill = data.get("bill", {})
            rows.append(flatten_bill(bill))

            if (i + 1) % 50 == 0:
                print(f"    Fetched {i+1}/{len(bill_ids)}...")
        except Exception as e:
            print(f"    ERROR bill_id={bid}: {e}")

    return rows


def flatten_bill(bill):
    """Convert getBill JSON to flat dict."""
    sponsors = bill.get("sponsors", [])
    texts = bill.get("texts", [])
    latest_text = texts[-1] if texts else {}
    history = bill.get("history", [])
    votes = bill.get("votes", [])

    committee = bill.get("committee", {})
    committee_name = committee.get(
        "name", "") if isinstance(committee, dict) else ""

    return {
        "state": bill.get("state", ""),
        "bill_id": bill.get("bill_id"),
        "session_id": bill.get("session_id"),
        "bill_number": bill.get("bill_number", ""),
        "status": bill.get("status", 0),
        "status_date": bill.get("status_date", ""),
        "title": bill.get("title", ""),
        "description": bill.get("description", ""),
        "url": bill.get("url", ""),
        "state_link": bill.get("state_link", ""),
        "change_hash": bill.get("change_hash", ""),
        "committee": committee_name,
        "sponsor_names": " | ".join(s.get("name", "") for s in sponsors),
        "sponsor_parties": " | ".join(s.get("party", "") for s in sponsors),
        "primary_sponsor": next(
            (s["name"] for s in sponsors if s.get("sponsor_order") == 1), ""
        ),
        "sponsor_count": len(sponsors),
        "last_action_date": history[-1].get("date", "") if history else "",
        "last_action": history[-1].get("action", "") if history else "",
        "document_id": latest_text.get("doc_id", 0),
        "document_type": latest_text.get("type", ""),
        "rollcall_count": len(votes),
    }


# ─── Lambda handler ───────────────────────────────────────

def lambda_handler(event, context):
    start = time.time()
    print(f"Poll started at {datetime.now(timezone.utc).isoformat()}")

    # 1. Load state from S3
    known_ids = load_known_ids()
    session_hashes = load_session_hashes()
    null_hashes = sum(1 for v in known_ids.values() if v is None)
    print(f"Known bills: {len(known_ids)} ({null_hashes} without hash)")
    print(f"Tracked sessions: {len(session_hashes)}")

    # 2. Find changed sessions (1 API call)
    print("\n[1] getDatasetList — checking for session changes...")
    changed_sessions = find_changed_sessions(session_hashes)

    if not changed_sessions:
        save_json_to_s3(SESSION_HASHES_KEY, session_hashes)
        print("\nNo sessions changed. Done.")
        return {
            "statusCode": 200,
            "new_bills": 0,
            "updated_bills": 0,
            "api_calls": 1,
            "duration_seconds": round(time.time() - start, 1),
        }

    # 3. Check changed sessions for new/updated bills
    print(
        f"\n[2] getMasterListRaw — checking {len(changed_sessions)} session(s)...")
    new_ids, updated_ids, master_calls = find_new_and_updated_bills(
        changed_sessions, known_ids
    )
    print(f"\nNew: {len(new_ids)}  Updated: {len(updated_ids)}")

    # 4. Fetch new bills
    print(f"\n[3] getBill — fetching details...")
    new_rows = fetch_bills(new_ids, "new")
    new_csv_key = save_bills_csv(new_rows, "new") if new_rows else None

    # 5. Fetch updated bills
    updated_rows = fetch_bills(updated_ids, "updated")
    updated_csv_key = save_bills_csv(
        updated_rows, "updated") if updated_rows else None

    # 6. Save all state back to S3
    save_json_to_s3(KNOWN_IDS_KEY, known_ids)
    save_json_to_s3(SESSION_HASHES_KEY, session_hashes)

    total_calls = 1 + master_calls + len(new_ids) + len(updated_ids)
    duration = round(time.time() - start, 1)

    print(f"\nDone. {total_calls} API calls in {duration}s")
    if new_csv_key:
        print(f"  New bills:     s3://{BUCKET}/{new_csv_key}")
    if updated_csv_key:
        print(f"  Updated bills: s3://{BUCKET}/{updated_csv_key}")

    return {
        "statusCode": 200,
        "new_bills": len(new_rows),
        "updated_bills": len(updated_rows),
        "api_calls": total_calls,
        "new_csv_key": new_csv_key,
        "updated_csv_key": updated_csv_key,
        "duration_seconds": duration,
    }


# ─── Local testing ────────────────────────────────────────

if __name__ == "__main__":
    if not API_KEY:
        print("Set LEGISCAN_API_KEY environment variable")
    else:
        result = lambda_handler({}, None)
        print(json.dumps(result, indent=2))

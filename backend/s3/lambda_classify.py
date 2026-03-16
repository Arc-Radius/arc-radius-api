"""
Arc Radius — Classification Lambda
====================================
Reads new/updated bill CSVs from the polling Lambda,
classifies them via SageMaker endpoint (LegalBERT + LogReg).

Lambda is a dumb pipe — sends raw bill fields to the endpoint,
which computes all derived features internally:
  - sponsor_parties → dominant_party, r/d/other_sponsors
  - total_yea/nay → percent_nay
  - state → state_r_sponsorship_ratio (baked-in lookup)

Lambda only adds:
  - Metadata for Neo4j (status_desc, year, session_year, passed/failed/vetoed)
  - Issue categorization using TOPIC_RULES (endpoint doesn't return these)

Tracking:
  - classified_bills.json: {bill_id: change_hash} for ALL classified bills

Outputs:
  - processed/classified/matches_{ts}.csv — LGBTQ+ matches only (triggers Lambda 3)

Trigger:
  - S3 event: prefix raw/legiscan-incremental/, suffix .csv

After reading:
  - Moves input CSVs to raw/legiscan-classified/ (different prefix, no re-trigger)

Deploy as a Lambda with:
  - Runtime: Python 3.12
  - Timeout: 15 minutes
  - Memory: 1024 MB
  - Environment variables:
      BUCKET = arc-radius-s3-bucket
      SAGEMAKER_ENDPOINT = arc-radius-legalbert
"""

import json
import os
import csv
import time
from datetime import datetime, timezone
from io import StringIO

import boto3

# ─── Config ────────────────────────────────────────────────
BUCKET = os.environ.get("BUCKET", "arc-radius-s3-bucket")
REGION = os.environ.get("AWS_REGION", "us-east-1")
SAGEMAKER_ENDPOINT = os.environ.get(
    "SAGEMAKER_ENDPOINT", "arc-radius-legalbert")

# S3 keys
CLASSIFIED_KEY = "pipeline/metadata/classified_bills.json"
INCREMENTAL_PREFIX = "raw/legiscan-incremental/"
ARCHIVE_PREFIX = "raw/legiscan-classified/"
CLASSIFIED_OUTPUT_PREFIX = "processed/classified/"

s3 = boto3.client("s3", region_name=REGION)
sm_runtime = boto3.client("sagemaker-runtime", region_name=REGION)

# Status code → description mapping (from LegiScan API docs)
STATUS_MAP = {
    "0": "N/A", "1": "Introduced", "2": "Engrossed", "3": "Enrolled",
    "4": "Passed", "5": "Vetoed", "6": "Failed",
}


# ─── S3 helpers ───────────────────────────────────────────

def load_json_from_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def save_json_to_s3(key, data):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )


def list_incremental_csvs():
    """Find unprocessed CSVs from polling Lambda."""
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=INCREMENTAL_PREFIX)
    keys = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            keys.append(key)
    return keys


def read_csv_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    text = obj["Body"].read().decode("utf-8")
    return list(csv.DictReader(StringIO(text)))


def move_to_archive(key):
    """Move input CSV to raw/legiscan-classified/ (different prefix, no re-trigger)."""
    filename = key.split("/")[-1]
    new_key = f"{ARCHIVE_PREFIX}{filename}"
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": key},
        Key=new_key,
    )
    s3.delete_object(Bucket=BUCKET, Key=key)


def save_matches_csv(rows):
    """Save LGBTQ+ matches to processed/classified/ (triggers Lambda 3)."""
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

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{CLASSIFIED_OUTPUT_PREFIX}matches_{timestamp}.csv"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body="\n".join(lines),
        ContentType="text/csv",
    )
    return key


# ─── Bill metadata (for output CSV / Neo4j) ──────────────

def compute_bill_metadata(bill):
    """Add derived metadata columns. NOT for SageMaker — just for the output CSV."""
    status = str(bill.get("status", "0"))
    bill["passed"] = status == "4"
    bill["failed"] = status == "6"
    bill["vetoed"] = status == "5"
    bill["status_desc"] = STATUS_MAP.get(status, "")

    date_str = bill.get("status_date", "") or bill.get("last_action_date", "")
    year = date_str[:4] if date_str and len(date_str) >= 4 else ""
    bill["year"] = year

    url = bill.get("url", "") or ""
    session_year = ""
    if url:
        parts = url.rstrip("/").split("/")
        if parts and parts[-1].isdigit() and len(parts[-1]) == 4:
            session_year = parts[-1]
    bill["session_year"] = session_year or year

    return bill


# ─── Issue categorization (matches notebook 03 TOPIC_RULES) ──
# The SageMaker endpoint returns label + confidence but NOT issues.
# Lambda categorizes issues from bill text for Neo4j display.

TOPIC_RULES = [
    ("healthcare", [
        "healthcare", "medical", "heathcare",
    ]),
    ("sports", [
        "sports", "athletics",
    ]),
    ("education", [
        "student", "educator", "school restriction", "school or curriculum",
    ]),
    ("curriculum_speech", [
        "curriculum", "outing", "don't say",
    ]),
    ("facilities", [
        "facilities", "facility", "bathroom", "single-sex",
        "public accommodation",
    ]),
    ("religious_exemption", [
        "religious", "rfra",
    ]),
    ("identity_documents", [
        "accurate id", "identification document", "gender marker",
        "definition of sex", "re-definition", "barriers to accurate",
    ]),
    ("expression", [
        "drag", "free speech", "expression ban", "expression restriction",
    ]),
    ("civil_rights", [
        "civil rights", "nondiscrimination", "equality",
    ]),
]


def categorize_issues(text):
    """Map bill text to normalized topic categories (notebook 03 TOPIC_RULES)."""
    if not text:
        return ["other"]
    text_lower = text.lower()
    categories = set()
    for category, keywords in TOPIC_RULES:
        if any(kw in text_lower for kw in keywords):
            categories.add(category)
    return sorted(categories) if categories else ["other"]


# ─── SageMaker classification ────────────────────────────

def classify_bill(bill):
    """
    Call SageMaker endpoint with raw bill fields.
    The endpoint computes all derived features internally.

    Sends:    text, state, sponsor_parties, total_yea, total_nay
    Receives: lgbtq_related, label, confidence, relevance_score
    """
    text = f"{bill.get('title', '')} {bill.get('description', '')}"

    payload = {
        "text": text[:512],
        "state": bill.get("state", ""),
        "sponsor_parties": bill.get("sponsor_parties", ""),
        "total_yea": _to_float(bill.get("total_yea", 0)),
        "total_nay": _to_float(bill.get("total_nay", 0)),
    }

    response = sm_runtime.invoke_endpoint(
        EndpointName=SAGEMAKER_ENDPOINT,
        ContentType="application/json",
        Body=json.dumps(payload),
    )
    result = json.loads(response["Body"].read().decode())

    is_relevant = result.get("lgbtq_related", False)
    label = result.get("label", "unknown")
    confidence = float(result.get("confidence", 0.0))

    # Categorize issues from text (endpoint doesn't return these)
    issue_categories = categorize_issues(text)

    return is_relevant, label, confidence, issue_categories


def _to_float(val):
    """Safely convert to float, default 0."""
    try:
        return float(val) if val != "" and val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


# ─── Lambda handler ───────────────────────────────────────

def lambda_handler(event, context):
    start = time.time()
    now = datetime.now(timezone.utc).isoformat()
    print(f"Classification started at {now}")
    print(f"Endpoint: {SAGEMAKER_ENDPOINT}")

    # 1. Load classification tracker
    classified = load_json_from_s3(CLASSIFIED_KEY) or {}
    print(f"Previously classified: {len(classified)} bills")

    # 2. Find incremental CSVs
    csv_keys = list_incremental_csvs()
    if not csv_keys:
        print("No incremental CSVs to process. Done.")
        return {"statusCode": 200, "classified": 0, "matched": 0}

    print(f"Found {len(csv_keys)} CSV(s) to process:")
    for k in csv_keys:
        print(f"  {k}")

    # 3. Read all bills
    all_bills = []
    for key in csv_keys:
        bills = read_csv_from_s3(key)
        all_bills.extend(bills)
        print(f"  {key}: {len(bills)} bills")

    # 4. Filter to unclassified (or hash-changed)
    to_classify = []
    skipped = 0
    for bill in all_bills:
        bid = str(bill.get("bill_id", ""))
        current_hash = bill.get("change_hash", "")

        if bid in classified and classified[bid] == current_hash:
            skipped += 1
            continue

        to_classify.append(bill)

    print(f"To classify: {len(to_classify)} (skipped {skipped} already done)")

    if not to_classify:
        for key in csv_keys:
            move_to_archive(key)
        print("All bills already classified. CSVs archived. Done.")
        return {"statusCode": 200, "classified": 0, "matched": 0}

    # 5. Classify
    new_matches = []
    not_relevant = 0
    errors = 0

    for i, bill in enumerate(to_classify):
        bid = str(bill.get("bill_id", ""))

        # Add metadata columns for output CSV
        bill = compute_bill_metadata(bill)

        # Call SageMaker
        try:
            is_relevant, label, confidence, issue_categories = classify_bill(bill)
        except Exception as e:
            print(f"    ERROR classifying bill {bid}: {e}")
            errors += 1
            continue

        # Update tracker (regardless of result)
        classified[bid] = bill.get("change_hash", "")

        if is_relevant:
            bill["label"] = label
            bill["label_source"] = "legalbert"
            bill["confidence"] = confidence
            bill["issue_categories"] = str(issue_categories)
            new_matches.append(bill)
        else:
            not_relevant += 1

        if (i + 1) % 100 == 0:
            print(f"  Classified {i+1}/{len(to_classify)}... "
                  f"({len(new_matches)} matches so far)")

    print(f"\nResults:")
    print(f"  Classified: {len(to_classify)}")
    print(f"  LGBTQ+ relevant: {len(new_matches)}")
    print(f"  Not relevant: {not_relevant}")
    if errors:
        print(f"  Errors: {errors}")

    # 6. Write matches to processed/classified/ (triggers Lambda 3)
    matches_csv_key = None
    if new_matches:
        matches_csv_key = save_matches_csv(new_matches)
        print(f"  Matches CSV: s3://{BUCKET}/{matches_csv_key}")

        print(f"\n  Sample matches:")
        for m in new_matches[:5]:
            print(f"    {m.get('state')} {m.get('bill_number')}: "
                  f"{m.get('label')} — {m.get('title', '')[:60]}")
        if len(new_matches) > 5:
            print(f"    ... and {len(new_matches) - 5} more")

    # 7. Save classification tracker
    save_json_to_s3(CLASSIFIED_KEY, classified)
    print(f"  classified_bills.json: {len(classified)} total")

    # 8. Move input CSVs to archive (different prefix, no re-trigger)
    for key in csv_keys:
        move_to_archive(key)
        print(f"  Archived: {key} → {ARCHIVE_PREFIX}")

    duration = round(time.time() - start, 1)
    print(f"\nDone in {duration}s")

    return {
        "statusCode": 200,
        "classified": len(to_classify),
        "matched": len(new_matches),
        "not_relevant": not_relevant,
        "errors": errors,
        "matches_csv_key": matches_csv_key,
        "duration_seconds": duration,
    }


# ─── Local testing ────────────────────────────────────────

if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2))

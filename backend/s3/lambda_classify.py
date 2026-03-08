"""
Arc Radius — Classification Lambda
====================================
Reads new/updated bill CSVs from the polling Lambda,
classifies them for LGBTQ+ relevance and stance.

Classification:
  - Currently: heuristic keyword matching (notebook 03 logic)
  - Later: swap in SageMaker LegalBERT endpoint (USE_SAGEMAKER=true)

Tracking:
  - classified_bills.json: {bill_id: change_hash} for ALL classified bills
    (both relevant and not relevant — so we don't re-classify them)

Outputs:
  - processed/classified/matches_{ts}.csv — LGBTQ+ matches only (triggers Lambda 3)
  - Neo4j is the source of truth for matched bills (no S3 CSV append)

Trigger:
  - S3 event: prefix raw/legiscan-incremental/, suffix .csv

After reading:
  - Moves input CSVs to raw/legiscan-classified/ (different prefix, no re-trigger)

Deploy as a Lambda with:
  - Runtime: Python 3.14
  - Timeout: 15 minutes
  - Memory: 1024 MB
  - Environment variables:
      BUCKET = arc-radius-s3-bucket
      SAGEMAKER_ENDPOINT = arc-radius-legalbert  (when ready)
      USE_SAGEMAKER = false  (set to true when endpoint is deployed)
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
USE_SAGEMAKER = os.environ.get("USE_SAGEMAKER", "false").lower() == "true"

# S3 keys
CLASSIFIED_KEY = "pipeline/metadata/classified_bills.json"
INCREMENTAL_PREFIX = "raw/legiscan-incremental/"
ARCHIVE_PREFIX = "raw/legiscan-classified/"           # moved here after reading (no trigger)
CLASSIFIED_OUTPUT_PREFIX = "processed/classified/"     # triggers Lambda 3

s3 = boto3.client("s3", region_name=REGION)
sm_runtime = boto3.client(
    "sagemaker-runtime", region_name=REGION) if USE_SAGEMAKER else None

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


# ─── Feature engineering (light version) ──────────────────

def compute_features(bill):
    """
    Compute features from polling data that don't need external datasets.
    Complex political features (state_lean, pass_rate_gap, etc.) are left
    empty — fill them in your notebook 03 pipeline later.
    """
    # Sponsor party counts
    parties = str(bill.get("sponsor_parties", "")).split(" | ")
    r_sponsors = sum(1 for p in parties if p.strip() == "R")
    d_sponsors = sum(1 for p in parties if p.strip() == "D")
    other_sponsors = sum(1 for p in parties if p.strip() not in ("R", "D", ""))

    # Dominant party
    if r_sponsors > d_sponsors:
        dominant = "R"
    elif d_sponsors > r_sponsors:
        dominant = "D"
    elif r_sponsors == d_sponsors and r_sponsors > 0:
        dominant = "Bipartisan"
    else:
        dominant = ""

    # Status flags
    status = str(bill.get("status", "0"))
    passed = status == "4"
    failed = status == "6"
    vetoed = status == "5"
    status_desc = STATUS_MAP.get(status, "")

    # Year from status_date or last_action_date
    date_str = bill.get("status_date", "") or bill.get("last_action_date", "")
    year = date_str[:4] if date_str and len(date_str) >= 4 else ""

    bill.update({
        "r_sponsors": r_sponsors,
        "d_sponsors": d_sponsors,
        "other_sponsors": other_sponsors,
        "bill_dominant_party": dominant,
        "passed": passed,
        "failed": failed,
        "vetoed": vetoed,
        "status_desc": status_desc,
        "year": year,
        "session_year": year,
        # These need external data — leave empty for now
        "state_lean": "",
        "r_sponsorship_ratio": "",
        "pass_rate_gap": "",
        "overall_pass_rate": "",
        "bipartisan_ratio": "",
        # Pass through existing fields
        "committee_id": bill.get("committee_id", ""),
        "total_yea": bill.get("total_yea", 0),
        "total_nay": bill.get("total_nay", 0),
        "document_count": bill.get("document_count", ""),
        "document_url": bill.get("document_url", ""),
    })

    return bill


# ─── Classification ───────────────────────────────────────

# LGBTQ+ relevance keywords (from notebook 03)
LGBTQ_KEYWORDS = [
    "transgender", "gender identity", "sexual orientation",
    "lgbtq", "same-sex", "same sex", "gay", "lesbian", "nonbinary",
    "non-binary", "conversion therapy", "drag", "pronoun",
    "gender-affirming", "gender affirming",
    "biological sex", "birth sex", "assigned sex",
    "sex assigned at birth", "gender transition",
    "hormone therapy", "puberty blocker",
    "don't say gay", "dont say gay",
    "bathroom bill", "sex-based", "sex based",
]

# Stance keywords
HARMFUL_KEYWORDS = [
    "ban", "prohibit", "restrict", "biological sex",
    "birth sex", "assigned sex", "female at birth",
    "male at birth", "protect children", "parental rights",
    "religious freedom", "conscience", "female sports",
    "women's sports", "biological male", "biological female",
]

SUPPORTIVE_KEYWORDS = [
    "protect", "nondiscrimination", "non-discrimination",
    "conversion therapy ban", "prohibit conversion therapy",
    "gender-affirming care", "gender affirming care",
    "inclusive", "equality", "anti-discrimination",
    "civil rights", "safe schools",
]

# Issue category mapping
ISSUE_MAP = {
    "gender identity": ("Gender identity protections", "civil_rights"),
    "sexual orientation": ("Nondiscrimination protections", "civil_rights"),
    "conversion therapy": ("Conversion therapy", "healthcare"),
    "gender-affirming": ("Gender-affirming care", "healthcare"),
    "gender affirming": ("Gender-affirming care", "healthcare"),
    "hormone therapy": ("Gender-affirming care", "healthcare"),
    "puberty blocker": ("Gender-affirming care", "healthcare"),
    "bathroom": ("Bathroom access", "civil_rights"),
    "drag": ("Drag performance restrictions", "free_speech"),
    "pronoun": ("Pronoun policies", "education"),
    "sports": ("Sports participation", "education"),
    "don't say gay": ("Education restrictions", "education"),
    "dont say gay": ("Education restrictions", "education"),
    "same-sex": ("Marriage/partnership rights", "civil_rights"),
    "same sex": ("Marriage/partnership rights", "civil_rights"),
}


def classify_heuristic(bill):
    """
    Heuristic LGBTQ+ relevance and stance classification.
    Based on notebook 03 logic.

    Returns (is_relevant, label, confidence, issues, issue_categories)
    """
    text = f"{bill.get('title', '')} {bill.get('description', '')}".lower()

    # Check relevance
    matched_keywords = [kw for kw in LGBTQ_KEYWORDS if kw in text]
    if not matched_keywords:
        return False, "not_relevant", 0.0, "", ""

    # Determine stance
    harmful_score = sum(1 for kw in HARMFUL_KEYWORDS if kw in text)
    supportive_score = sum(1 for kw in SUPPORTIVE_KEYWORDS if kw in text)

    if harmful_score > supportive_score:
        label = "harmful"
    elif supportive_score > harmful_score:
        label = "supportive"
    else:
        label = "neutral"

    # Map to issues
    issues = set()
    categories = set()
    for kw, (issue, cat) in ISSUE_MAP.items():
        if kw in text:
            issues.add(issue)
            categories.add(cat)

    confidence = min(0.6 + (len(matched_keywords) * 0.05), 0.9)

    return (
        True,
        label,
        confidence,
        "; ".join(sorted(issues)) if issues else "LGBTQ+ related",
        str(sorted(categories)) if categories else "['general']",
    )


def classify_sagemaker(bill):
    """
    Call SageMaker LegalBERT endpoint.
    Swap this in when your endpoint is deployed.

    Your endpoint should accept:
      {"text": "title + description", "state": "TX"}
    And return:
      {"lgbtq_related": true, "label": "supportive", "confidence": 0.92}
    """
    text = f"{bill.get('title', '')} {bill.get('description', '')}"

    try:
        response = sm_runtime.invoke_endpoint(
            EndpointName=SAGEMAKER_ENDPOINT,
            ContentType="application/json",
            Body=json.dumps(
                {"text": text[:512], "state": bill.get("state", "")}),
        )
        result = json.loads(response["Body"].read().decode())

        is_relevant = result.get("lgbtq_related", False)
        label = result.get("label", "unknown")
        confidence = float(result.get("confidence", 0.0))

        # Still use heuristic for issues/categories
        _, _, _, issues, categories = classify_heuristic(bill)

        return is_relevant, label, confidence, issues, categories

    except Exception as e:
        print(f"    SageMaker error for bill {bill.get('bill_id')}: {e}")
        print(f"    Falling back to heuristic")
        return classify_heuristic(bill)


def classify_bill(bill):
    """Route to SageMaker or heuristic based on config."""
    if USE_SAGEMAKER:
        return classify_sagemaker(bill)
    return classify_heuristic(bill)


# ─── Lambda handler ───────────────────────────────────────

def lambda_handler(event, context):
    start = time.time()
    now = datetime.now(timezone.utc).isoformat()
    print(f"Classification started at {now}")
    print(f"Using: {'SageMaker' if USE_SAGEMAKER else 'heuristic'} classifier")

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

    for i, bill in enumerate(to_classify):
        bid = str(bill.get("bill_id", ""))

        # Compute basic features
        bill = compute_features(bill)

        # Classify
        is_relevant, label, confidence, issues, categories = classify_bill(bill)

        # Update tracker (regardless of result)
        classified[bid] = bill.get("change_hash", "")

        if is_relevant:
            bill["label"] = label
            bill["label_source"] = "legalbert" if USE_SAGEMAKER else "heuristic"
            bill["issues"] = issues
            bill["issue_categories"] = categories
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

    # 6. Write matches to processed/classified/ (triggers Lambda 3)
    matches_csv_key = None
    if new_matches:
        matches_csv_key = save_matches_csv(new_matches)
        print(f"  Matches CSV: s3://{BUCKET}/{matches_csv_key}")

        # Print sample matches
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
        "matches_csv_key": matches_csv_key,
        "duration_seconds": duration,
    }


# ─── Local testing ────────────────────────────────────────

if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2))

"""
One-time setup: seed classified_bills.json in S3
=================================================
Seeds the classification tracker from your existing
matched_lgbtq_bills.csv so the classify Lambda knows
these bills have already been through classification.

Usage:
    python seed_classified_bills.py --from-s3

    # Or from local file:
    python seed_classified_bills.py matched_lgbtq_bills.csv
"""

import argparse
import json
import sys

import boto3
import pandas as pd

BUCKET = "arc-radius-s3-bucket"
REGION = "us-east-1"
CLASSIFIED_KEY = "pipeline/metadata/classified_bills.json"
MATCHED_S3_KEY = "processed/matched-bills/matched_lgbtq_bills.csv"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", nargs="?",
                        help="Local path to matched_lgbtq_bills.csv")
    parser.add_argument("--from-s3", action="store_true",
                        help="Read from S3 instead")
    parser.add_argument("--bucket", default=BUCKET)
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=REGION)

    if args.from_s3:
        print(f"Reading from s3://{args.bucket}/{MATCHED_S3_KEY}...")
        obj = s3.get_object(Bucket=args.bucket, Key=MATCHED_S3_KEY)
        try:
            df = pd.read_csv(obj["Body"], usecols=["bill_id", "change_hash"])
        except ValueError:
            obj = s3.get_object(Bucket=args.bucket, Key=MATCHED_S3_KEY)
            df = pd.read_csv(obj["Body"], usecols=["bill_id"])
            df["change_hash"] = None
    elif args.csv_path:
        print(f"Reading from {args.csv_path}...")
        # change_hash might not exist in older CSVs
        try:
            df = pd.read_csv(args.csv_path, usecols=["bill_id", "change_hash"])
        except ValueError:
            df = pd.read_csv(args.csv_path, usecols=["bill_id"])
            df["change_hash"] = None
    else:
        print("ERROR: provide a CSV path or use --from-s3")
        sys.exit(1)

    # Build {bill_id: change_hash} dict
    classified = {}
    for _, row in df.iterrows():
        bid = str(row["bill_id"])
        chash = row.get("change_hash")
        # Store hash if available, None if not
        classified[bid] = chash if pd.notna(chash) else None

    print(f"Found {len(classified)} classified bills")
    with_hash = sum(1 for v in classified.values() if v is not None)
    print(f"  With change_hash: {with_hash}")
    print(f"  Without (null): {len(classified) - with_hash}")

    s3.put_object(
        Bucket=args.bucket,
        Key=CLASSIFIED_KEY,
        Body=json.dumps(classified),
        ContentType="application/json",
    )
    print(f"Uploaded to s3://{args.bucket}/{CLASSIFIED_KEY}")
    print("Done. Classify Lambda will skip these bills.")


if __name__ == "__main__":
    main()

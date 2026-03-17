"""
One-time setup: seed known_bill_ids.json in S3
================================================
Run this ONCE before enabling the Lambda.
Extracts bill_ids from your existing CSV so the
Lambda knows which bills it already has.

Stores as {bill_id: change_hash} dict.
Since the CSV doesn't have change_hash, all values start as null.
The first Lambda poll will backfill the real hashes.

Usage:
    python seed_known_bill_ids.py all_bills_2021_2026.csv

    # Or if your CSV is already in S3:
    python seed_known_bill_ids.py --from-s3
"""

import argparse
import json
import sys

import boto3
import pandas as pd

BUCKET = "arc-radius-s3-bucket"
REGION = "us-east-1"
KNOWN_IDS_KEY = "pipeline/metadata/known_bill_ids.json"
S3_CSV_KEY = "raw/legiscan-bulk/all_bills_2021_2026.csv"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", nargs="?",
                        help="Local path to all_bills_2021_2026.csv")
    parser.add_argument("--from-s3", action="store_true",
                        help="Read CSV from S3 instead")
    parser.add_argument("--bucket", default=BUCKET)
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=REGION)

    if args.from_s3:
        print(f"Reading from s3://{args.bucket}/{S3_CSV_KEY}...")
        obj = s3.get_object(Bucket=args.bucket, Key=S3_CSV_KEY)
        df = pd.read_csv(obj["Body"], usecols=["bill_id"])
    elif args.csv_path:
        print(f"Reading from {args.csv_path}...")
        df = pd.read_csv(args.csv_path, usecols=["bill_id"])
    else:
        print("ERROR: provide a CSV path or use --from-s3")
        sys.exit(1)

    # Store as {bill_id: null} — first poll will backfill real hashes
    ids = {str(bid): None for bid in df["bill_id"].unique()}
    print(f"Found {len(ids)} unique bill_ids (change_hash set to null)")

    s3.put_object(
        Bucket=args.bucket,
        Key=KNOWN_IDS_KEY,
        Body=json.dumps(ids),
        ContentType="application/json",
    )
    print(f"Uploaded to s3://{args.bucket}/{KNOWN_IDS_KEY}")
    print("Done. First Lambda run will backfill change_hashes.")


if __name__ == "__main__":
    main()

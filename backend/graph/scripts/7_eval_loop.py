import argparse
import csv
import json
import time
from pathlib import Path

import requests

TASKS = ("bill_summary", "bill_why_matters", "bill_related")
OUTPUT_FIELDS = (
    "Bill ID",
    "Output Prompt Template",
    "Output Text",
    "Output Sources",
)


# build parser for command line arguments
def _build_parser() -> argparse.ArgumentParser:
    repo_root = Path(__file__).resolve().parents[3]
    graph_root = repo_root / "backend" / "graph"
    output_dir = graph_root / "output"

    parser = argparse.ArgumentParser(
        description="Run all generate tasks for eval bills and export CSV/JSON.",
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="API base URL.",
    )
    parser.add_argument(
        "--input-csv",
        default=str(graph_root / "eval_bills.csv"),
        help="Input CSV with an `id` column containing bill IDs.",
    )
    parser.add_argument(
        "--csv-out",
        default=str(output_dir / "eval_bills_generate_outputs.csv"),
        help="Output CSV path.",
    )
    parser.add_argument(
        "--json-out",
        default=str(output_dir / "eval_bills_generate_outputs.json"),
        help="Output JSON path.",
    )
    parser.add_argument(
        "--max-bills",
        type=int,
        default=None,
        help="Optional limit for a small smoke run.",
    )
    return parser


# load bill ids from csv
def load_bill_ids(csv_path: Path, max_bills: int | None = None) -> list[str]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")

    bill_ids: list[str] = []
    seen: set[str] = set()

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "id" not in (reader.fieldnames or []):
            raise ValueError("Input CSV must contain an `id` column.")

        for row in reader:
            bill_id = (row.get("id") or "").strip()
            if not bill_id or bill_id in seen:
                continue
            seen.add(bill_id)
            bill_ids.append(bill_id)
            if max_bills is not None and len(bill_ids) >= max_bills:
                break

    return bill_ids


# call generate endpoint
def call_generate(
    session: requests.Session,
    base_url: str,
    bill_id: str,
    task: str,
) -> dict:
    endpoint = f"{base_url.rstrip('/')}/generate/bill"
    response = session.post(
        endpoint,
        json={"task": task, "bill_pk": bill_id},
        timeout=180,
    )
    response.raise_for_status()
    return response.json()


def build_output_sources(
    sources: list[dict],
    anchor_context: str | None = None,
) -> dict:
    return {
        "anchor_context": anchor_context,
        "sources": sources,
    }


# write records to json
def write_json(records: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


# write records to csv
def write_csv(records: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(OUTPUT_FIELDS))
        writer.writeheader()
        for record in records:
            row = dict(record)
            row["Output Sources"] = json.dumps(row.get("Output Sources", []))
            writer.writerow(row)


# main function
def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    input_csv = Path(args.input_csv)
    csv_out = Path(args.csv_out)
    json_out = Path(args.json_out)

    bill_ids = load_bill_ids(input_csv, args.max_bills)
    total_calls = len(bill_ids) * len(TASKS)
    print(f"[START] Loaded {len(bill_ids)} bills from {input_csv}")
    print(f"[START] Total generate calls planned: {total_calls}")

    records: list[dict] = []
    success_count = 0
    failure_count = 0

    # loop through bills and tasks
    with requests.Session() as session:
        for bill_idx, bill_id in enumerate(bill_ids, start=1):
            time.sleep(0.5)
            print(f"[BILL {bill_idx}/{len(bill_ids)}] {bill_id}")
            # loop through tasks
            for task_idx, task in enumerate(TASKS, start=1):
                print(f"  [TASK {task_idx}/{len(TASKS)}] {task} -> sending request")
                # call generate endpoint
                try:
                    data = call_generate(session, args.base_url, bill_id, task)
                    # build record
                    record = {
                        "Bill ID": bill_id,
                        "Output Prompt Template": data.get(
                            "prompt_template",
                            "",
                        ),
                        "Output Text": data.get("answer", ""),
                        "Output Sources": build_output_sources(
                            data.get("sources", []),
                            data.get("anchor_context"),
                        ),
                    }
                    # append record
                    records.append(record)
                    success_count += 1
                    print(
                        "  [OK] "
                        f"{task} for {bill_id} "
                        f"(successes={success_count}, failures={failure_count})"
                    )
                except (requests.RequestException, ValueError) as exc:
                    # handle error
                    failure_count += 1
                    print(
                        "  [ERROR] "
                        f"{task} for {bill_id}: {exc} "
                        f"(successes={success_count}, failures={failure_count})"
                    )
                    # append record
                    records.append(
                        {
                            "Bill ID": bill_id,
                            "Output Prompt Template": "",
                            "Output Text": f"ERROR: {exc}",
                            "Output Sources": [],
                        }
                    )

    # write records to json
    write_json(records, json_out)
    print(f"[WRITE] JSON output: {json_out}")
    # write records to csv
    write_csv(records, csv_out)
    print(f"[WRITE] CSV output: {csv_out}")
    print(
        "[DONE] "
        f"rows={len(records)}, successes={success_count}, failures={failure_count}"
    )


if __name__ == "__main__":
    main()

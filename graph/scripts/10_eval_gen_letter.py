import argparse
import csv
import itertools
import json
import time
from pathlib import Path

import requests

MEDIUMS = ("email", "phone")
TONES = ("formal", "conversational")
LETTER_COMBINATIONS = list(itertools.product(MEDIUMS, TONES))

TASK_SLEEP_SECONDS = 5.0
OUTPUT_FIELDS = (
    "Bill PK",
    "Medium",
    "Stance",
    "Tone",
    "Output Prompt Template",
    "Output Text",
    "Output Sources",
)


def _build_parser() -> argparse.ArgumentParser:
    repo_root = Path(__file__).resolve().parents[2]
    graph_root = repo_root / "graph"
    output_dir = graph_root / "output"

    parser = argparse.ArgumentParser(
        description="Run all generate_letter combinations for eval bills and export CSV/JSON.",
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="API base URL.",
    )
    parser.add_argument(
        "--input-csv",
        default=str(graph_root / "eval_bills_letter.csv"),
        help="Input CSV with `bill_pk` and `stance` columns.",
    )
    parser.add_argument(
        "--csv-out",
        default=str(output_dir / "eval_bills_letter_outputs.csv"),
        help="Output CSV path.",
    )
    parser.add_argument(
        "--json-out",
        default=str(output_dir / "eval_bills_letter_outputs.json"),
        help="Output JSON path.",
    )
    parser.add_argument(
        "--max-bills",
        type=int,
        default=None,
        help="Optional limit for a small smoke run.",
    )
    return parser


def load_bills(csv_path: Path, max_bills: int | None = None) -> list[dict]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")

    bills: list[dict] = []
    seen: set[str] = set()

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        missing = {"bill_pk", "stance"} - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Input CSV is missing columns: {missing}")

        for row in reader:
            bill_pk = (row.get("bill_pk") or "").strip()
            stance = (row.get("stance") or "").strip().lower()
            if not bill_pk or bill_pk in seen:
                continue
            seen.add(bill_pk)
            bills.append({"bill_pk": bill_pk, "stance": stance})
            if max_bills is not None and len(bills) >= max_bills:
                break

    return bills


def call_generate_letter(
    session: requests.Session,
    base_url: str,
    bill_pk: str,
    medium: str,
    stance: str,
    tone: str,
) -> dict:
    endpoint = f"{base_url.rstrip('/')}/generate/bill"
    response = session.post(
        endpoint,
        json={
            "task": "generate_letter",
            "bill_pk": bill_pk,
            "letter_params": {
                "medium": medium,
                "stance": stance,
                "tone": tone,
            },
        },
        timeout=180,
    )
    response.raise_for_status()
    return response.json()


def build_output_sources(sources: list[dict]) -> dict:
    return {"sources": sources}


def write_json(records: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


def write_csv(records: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(OUTPUT_FIELDS))
        writer.writeheader()
        for record in records:
            row = dict(record)
            row["Output Sources"] = json.dumps(row.get("Output Sources", []))
            writer.writerow(row)


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    input_csv = Path(args.input_csv)
    csv_out = Path(args.csv_out)
    json_out = Path(args.json_out)

    bills = load_bills(input_csv, args.max_bills)
    total_calls = len(bills) * len(LETTER_COMBINATIONS)
    print(f"[START] Loaded {len(bills)} bills from {input_csv}")
    print(f"[START] {len(LETTER_COMBINATIONS)} combinations per bill → {total_calls} total calls")

    records: list[dict] = []
    success_count = 0
    failure_count = 0

    with requests.Session() as session:
        for bill_idx, bill in enumerate(bills, start=1):
            bill_pk = bill["bill_pk"]
            time.sleep(0.5)
            print(f"[BILL {bill_idx}/{len(bills)}] {bill_pk}")

            for combo_idx, (medium, tone) in enumerate(LETTER_COMBINATIONS, start=1):
                stance = bill["stance"]
                label = f"medium={medium} stance={stance} tone={tone}"
                
                try:
                    data = call_generate_letter(
                        session, args.base_url, bill_pk, medium, stance, tone
                    )
                    record = {
                        "Bill PK": bill_pk,
                        "Medium": medium,
                        "Stance": stance,
                        "Tone": tone,
                        "Output Prompt Template": data.get("prompt_template", ""),
                        "Output Text": data.get("answer", ""),
                        "Output Sources": build_output_sources(data.get("sources", [])),
                    }
                    records.append(record)
                    success_count += 1
                    print(f"  [OK] {label} (successes={success_count}, failures={failure_count})")

                except (requests.RequestException, ValueError) as exc:
                    failure_count += 1
                    print(f"  [ERROR] {label}: {exc} (successes={success_count}, failures={failure_count})")
                    records.append(
                        {
                            "Bill PK": bill_pk,
                            "Medium": medium,
                            "Stance": stance,
                            "Tone": tone,
                            "Output Prompt Template": "",
                            "Output Text": f"ERROR: {exc}",
                            "Output Sources": [],
                        }
                    )

                sleep_seconds = TASK_SLEEP_SECONDS * float(combo_idx < len(LETTER_COMBINATIONS))
                print(f"  [SLEEP] waiting {sleep_seconds:.1f}s before next combination")
                time.sleep(sleep_seconds)

    write_json(records, json_out)
    print(f"[WRITE] JSON output: {json_out}")
    write_csv(records, csv_out)
    print(f"[WRITE] CSV output: {csv_out}")
    print(f"[DONE] rows={len(records)}, successes={success_count}, failures={failure_count}")


if __name__ == "__main__":
    main()
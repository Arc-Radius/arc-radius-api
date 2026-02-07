#!/usr/bin/env python3

"""
Join LegiScan bulk CSV files per state or congresssession (sponsors.csv, history.csv, documents.csv, rollcalls.csv, bills.csv) into bill-centric flat files.

- Defaults to all .zip files in legiscan-bulk-csv/ when no args given
- Accepts one or more .zip files or extracted directories
- Safe zip extraction (zip-slip protected)
- Walks all subdirectories to find valid CSV sets
- Aggregates sponsors, history, documents, rollcalls per bill
- Adds a 'state' column derived from directory structure
- Concatenates all session CSVs into a single combined file

Usage:
    python concat_with_join_legiscan.py                          # process all zips (default)
    python concat_with_join_legiscan.py /path/to/2021-2022.zip   # single zip
    python concat_with_join_legiscan.py /path/to/2021-2022/      # single directory
"""

import argparse
import csv
import sys
import shutil
import zipfile
import re

import pandas as pd
from pathlib import Path


REQUIRED_FILES = ["bills.csv", "people.csv", "sponsors.csv", "history.csv", "documents.csv", "rollcalls.csv"]

BULK_CSV_ROOT = (Path(__file__).parent / ".." / "legiscan-bulk-csv").resolve()
DEFAULT_ZIPS = sorted(BULK_CSV_ROOT.glob("*.zip"))

OUTPUT_DIR = (Path(__file__).parent / ".." / "legiscan-combined-by-state-year").resolve()
COMBINED_PATH = (Path(__file__).parent / ".." / "legiscan-combined" / "all_bills_2021_2026.csv").resolve()


def letters_only(s: str) -> str:
    return re.sub(r"[^A-Za-z]", "", s or "")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Join LegiScan bulk CSVs into bill-centric flat files."
    )
    parser.add_argument(
        "filenames",
        nargs="*",
        default=[str(p) for p in DEFAULT_ZIPS],
        help="Path(s) to .zip or extracted directories (default: all zips in legiscan-bulk-csv/)",
    )
    return parser.parse_args()


def is_within_directory(base: Path, target: Path) -> bool:
    try:
        target.resolve().relative_to(base.resolve())
        return True
    except ValueError:
        return False


def safe_extract_zip(zip_path: Path, dest_dir: Path):
    with zipfile.ZipFile(zip_path, "r") as z:
        for member in z.infolist():
            member_path = Path(member.filename)

            # Skip macOS junk
            if "__MACOSX" in member_path.parts:
                continue
            if member.filename.endswith(".DS_Store"):
                continue

            out_path = (dest_dir / member_path).resolve()

            if not is_within_directory(dest_dir, out_path):
                raise RuntimeError(
                    f"Unsafe path detected in zip (zip-slip): {member.filename}"
                )

            if member.is_dir():
                out_path.mkdir(parents=True, exist_ok=True)
            else:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with z.open(member) as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)


def resolve_root_dir(input_path: Path) -> Path:
    if input_path.suffix == ".zip":
        zip_path = input_path
        dir_path = input_path.with_suffix("")
    else:
        dir_path = input_path
        zip_path = input_path.with_suffix(".zip")

    if zip_path.exists():
        if dir_path.exists():
            if zip_path.stat().st_mtime > dir_path.stat().st_mtime:
                print("Zip newer than directory. Re-extracting...")
                shutil.rmtree(dir_path)
                dir_path.mkdir(parents=True, exist_ok=True)
                safe_extract_zip(zip_path, dir_path)
        else:
            print("Extracting zip...")
            dir_path.mkdir(parents=True, exist_ok=True)
            safe_extract_zip(zip_path, dir_path)

        return dir_path

    if dir_path.exists():
        return dir_path

    raise FileNotFoundError("No valid zip or directory found.")


# ── Aggregation helpers ──────────────────────────────────────────────


def aggregate_sponsors(sponsors: pd.DataFrame, people: pd.DataFrame) -> pd.DataFrame:
    # join sponsors with people to get names/parties, then collapse per bill
    merged = sponsors.merge(people[["people_id", "name", "party"]], on="people_id", how="left")
    merged = merged.sort_values(["bill_id", "position"])

    def _agg(group: pd.DataFrame) -> pd.Series:
        primary = group.loc[group["position"] == 1, "name"]
        return pd.Series(
            {
                "sponsor_names": " | ".join(group["name"].dropna()),
                "sponsor_parties": " | ".join(group["party"].dropna()),
                "primary_sponsor": primary.iloc[0] if len(primary) > 0 else "",
                "sponsor_count": len(group),
            }
        )

    return merged.groupby("bill_id", sort=False).apply(_agg, include_groups=False).reset_index()


def aggregate_history(history: pd.DataFrame) -> pd.DataFrame:
    # get action count and the most recent action per bill
    history = history.sort_values(["bill_id", "date", "sequence"])

    def _agg(group: pd.DataFrame) -> pd.Series:
        return pd.Series(
            {
                "action_count": len(group),
                "last_history_action": group["action"].iloc[-1],
            }
        )

    return history.groupby("bill_id", sort=False).apply(_agg, include_groups=False).reset_index()


def aggregate_documents(documents: pd.DataFrame) -> pd.DataFrame:
    # count docs per bill and collect the legiscan text urls
    def _agg(group: pd.DataFrame) -> pd.Series:
        return pd.Series(
            {
                "document_count": len(group),
                "document_types": " | ".join(group["document_type"].dropna().unique()),
                "document_urls": " | ".join(group["url"].dropna()),
            }
        )

    return documents.groupby("bill_id", sort=False).apply(_agg, include_groups=False).reset_index()


def aggregate_rollcalls(rollcalls: pd.DataFrame) -> pd.DataFrame:
    # sum up yea/nay totals across all roll calls for each bill
    return (
        rollcalls.groupby("bill_id", sort=False)
        .agg(
            rollcall_count=("roll_call_id", "count"),
            total_yea=("yea", "sum"),
            total_nay=("nay", "sum"),
        )
        .reset_index()
    )


# ── Discovery & processing ───────────────────────────────────────────


def discover_csv_dirs(root: Path) -> list[Path]:
    """Walk root and return every directory that contains all required CSV files."""
    dirs = []
    for dirpath in root.rglob("*"):
        if dirpath.is_dir() and all((dirpath / f).is_file() for f in REQUIRED_FILES):
            dirs.append(dirpath)
    dirs.sort()
    return dirs


def derive_output_path(csv_dir: Path) -> Path:
    """Build output filename from directory structure."""
    session_folder = csv_dir.parent.name
    state_code = letters_only(csv_dir.parent.parent.name)
    return OUTPUT_DIR / f"{state_code}_{session_folder}.csv"


def process_csv_dir(csv_dir: Path, out_path: Path) -> int:
    """Process a single LegiScan CSV directory. Returns row count."""
    bills = pd.read_csv(csv_dir / "bills.csv")
    people = pd.read_csv(csv_dir / "people.csv")
    sponsors = pd.read_csv(csv_dir / "sponsors.csv")
    history = pd.read_csv(csv_dir / "history.csv")
    documents = pd.read_csv(csv_dir / "documents.csv")
    rollcalls = pd.read_csv(csv_dir / "rollcalls.csv")

    sponsors_agg = aggregate_sponsors(sponsors, people)
    history_agg = aggregate_history(history)
    documents_agg = aggregate_documents(documents)
    rollcalls_agg = aggregate_rollcalls(rollcalls)

    state_code = letters_only(csv_dir.parent.parent.name)

    result = bills.copy()
    result.insert(0, "state", state_code)
    result = result.merge(sponsors_agg, on="bill_id", how="left")
    result = result.merge(history_agg, on="bill_id", how="left")
    result = result.merge(documents_agg, on="bill_id", how="left")
    result = result.merge(rollcalls_agg, on="bill_id", how="left")

    count_cols = ["sponsor_count", "action_count", "document_count", "rollcall_count", "total_yea", "total_nay"]
    for col in count_cols:
        if col in result.columns:
            result[col] = result[col].fillna(0).astype(int)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(out_path, index=False)
    return len(result)


def main():
    args = parse_args()

    # Gather all csv dirs across all inputs
    csv_dirs = []
    for filename in args.filenames:
        input_path = Path(filename).expanduser()
        root_dir = resolve_root_dir(input_path)
        found = discover_csv_dirs(root_dir)
        if not found:
            print(f"No valid CSV directories found under {root_dir}", file=sys.stderr)
        csv_dirs.extend(found)

    if not csv_dirs:
        print("No valid CSV directories found.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(csv_dirs)} dataset(s) to process.\n")

    total_rows = 0

    for i, csv_dir in enumerate(csv_dirs, 1):
        out_path = derive_output_path(csv_dir)

        print(f"[{i}/{len(csv_dirs)}] {csv_dir}")
        try:
            rows = process_csv_dir(csv_dir, out_path)
            total_rows += rows
            print(f"  -> {out_path} ({rows} rows)\n")
        except Exception as e:
            print(f"  ERROR: {e}\n", file=sys.stderr)

    print(f"Done. Wrote {total_rows} total rows across {len(csv_dirs)} files.\n")

    # ── Concatenate all session CSVs into a single file ──────────────
    print("Concatenating all session CSVs into single file...")

    csv_paths = sorted(OUTPUT_DIR.glob("*.csv"))

    if not csv_paths:
        print("No session CSVs found to concatenate")
        return

    COMBINED_PATH.parent.mkdir(parents=True, exist_ok=True)

    header_written = False
    concat_rows = 0

    with open(COMBINED_PATH, "w", newline="", encoding="utf-8") as outfile:
        writer = None

        for csv_path in csv_paths:
            try:
                with open(csv_path, "r", newline="", encoding="utf-8") as infile:
                    reader = csv.reader(infile)
                    header = next(reader)

                    if not header_written:
                        writer = csv.writer(outfile)
                        writer.writerow(header)
                        header_written = True

                    for row in reader:
                        writer.writerow(row)
                        concat_rows += 1

            except Exception as e:
                print(f"Error processing {csv_path}: {e}")

    print(f"Written combined CSV to {COMBINED_PATH} ({concat_rows} rows)")


if __name__ == "__main__":
    main()

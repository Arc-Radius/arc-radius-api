"""
Join LegiScan bulk CSV files into a single bill-centric flat file.

"""

import argparse
import sys

import pandas as pd
from pathlib import Path

# Must have 2021-2022 2023-2024 2025-2026 folders bulk data unzipped, in the legiscan-bulk-csv folder
BULK_CSV_ROOT = (Path(__file__).parent / ".." / "datasources" / "legiscan-bulk-csv").resolve()
REQUIRED_FILES = ["bills.csv", "people.csv", "sponsors.csv", "history.csv", "documents.csv", "rollcalls.csv"]


def aggregate_sponsors(sponsors: pd.DataFrame, people: pd.DataFrame) -> pd.DataFrame:
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
    return (
        rollcalls.groupby("bill_id", sort=False)
        .agg(
            rollcall_count=("roll_call_id", "count"),
            total_yea=("yea", "sum"),
            total_nay=("nay", "sum"),
        )
        .reset_index()
    )


def discover_csv_dirs(root: Path) -> list[Path]:
    """Walk root and return every directory that contains all required CSV files."""
    dirs = []
    for dirpath in root.rglob("*"):
        if dirpath.is_dir() and all((dirpath / f).is_file() for f in REQUIRED_FILES):
            dirs.append(dirpath)
    dirs.sort()
    return dirs


def process_csv_dir(csv_dir: Path, out_dir: Path) -> Path:
    """Process a single LegiScan CSV directory and write the combined file. Returns output path."""
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

    # Derive state code + session folder from directory structure:
    # .../legiscan-bulk-csv/2021-2022/AK/2021-2022_32nd_Legislature/csv
    session_folder = csv_dir.parent.name
    state_code = csv_dir.parent.parent.name

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

    out_filename = f"{state_code}_{session_folder}.csv"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / out_filename
    result.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Join LegiScan CSVs into a bill-centric flat file.")
    parser.add_argument(
        "path",
        nargs="?",
        default=str(BULK_CSV_ROOT),
        help="Path to a single csv/ directory, or a root to walk (default: legiscan-bulk-csv/)",
    )
    args = parser.parse_args()

    target = Path(args.path).resolve()
    out_dir = (Path(__file__).parent / ".." / "datasources" / "legiscan-combined-by-state-year").resolve()

    # If target itself is a csv dir, process just that one
    if all((target / f).is_file() for f in REQUIRED_FILES):
        csv_dirs = [target]
    else:
        csv_dirs = discover_csv_dirs(target)

    if not csv_dirs:
        print(f"No valid CSV directories found under {target}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(csv_dirs)} dataset(s) to process.\n")

    for i, csv_dir in enumerate(csv_dirs, 1):
        print(f"[{i}/{len(csv_dirs)}] {csv_dir}")
        try:
            out_path = process_csv_dir(csv_dir, out_dir)
            print(f"  -> {out_path}\n")
        except Exception as e:
            print(f"  ERROR: {e}\n", file=sys.stderr)


if __name__ == "__main__":
    main()

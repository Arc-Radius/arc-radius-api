#!/usr/bin/env python3
"""
Concatenate all per-state-year CSVs from legiscan-combined-by-state-year/
into a single CSV at datasources/legiscan-combined/all_bills_2021_2026.csv
"""

import pandas as pd
from pathlib import Path

# paths
INPUT_PATH = Path(__file__).parent / ".." / "datasources" / "legiscan-combined-by-state-year"
SAVE_PATH = Path(__file__).parent / ".." / "datasources" / "legiscan-combined" / "all_bills_2021_2026.csv"

INPUT_PATH = INPUT_PATH.resolve()
SAVE_PATH = SAVE_PATH.resolve()

# Collect all combined CSVs
csv_paths = sorted(INPUT_PATH.glob("*.csv"))

all_bills = []

if not csv_paths:
    print(f"No CSV files found in {INPUT_PATH}")
else:
    for csv_path in csv_paths:
        try:
            df = pd.read_csv(csv_path)
            all_bills.append(df)
            print(f"  {csv_path.name}: {len(df)} rows")
        except Exception as e:
            print(f"Error reading {csv_path}: {e}")

# --- Write output ---
if all_bills:
    combined = pd.concat(all_bills, ignore_index=True)

    before = len(combined)
    combined = combined.drop_duplicates()
    after = len(combined)

    SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(SAVE_PATH, index=False)

    print(
        f"\nWritten combined CSV to {SAVE_PATH} "
        f"({before} → {after} rows, {before - after} duplicates removed)"
    )
else:
    print("No CSV files to concatenate")

#!/usr/bin/env python3

"""
Concatenate LegiScan bulk CSV/bills.csv files into one file.

- Accepts either a .zip file or extracted directory
- Safe zip extraction (zip-slip protected)
- Stream-writes (constant memory usage)
- Adds a 'state' column
- No external dependencies required

Usage:
    python concat_bills_builtin.py /path/to/2025-2026.zip
"""

import argparse
import sys
import shutil
import zipfile
import csv
import re
from pathlib import Path


def letters_only(s: str) -> str:
    return re.sub(r"[^A-Za-z]", "", s or "")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream-concatenate all CSV/bills.csv files."
    )
    parser.add_argument("filename", help="Path to .zip or extracted directory")
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite output without prompt"
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


def derive_output_path(input_path: Path) -> Path:
    if input_path.suffix == ".zip":
        base_name = input_path.with_suffix("").name
    else:
        base_name = input_path.name

    return input_path.parent / f"concatenated-{base_name}" / "all-bills.csv"


def stream_concat(root_dir: Path, output_path: Path):
    bills_paths = sorted(root_dir.rglob("CSV/bills.csv"))

    if not bills_paths:
        print("No bills.csv files found")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    header_written = False
    total_rows = 0

    with open(output_path, "w", newline="", encoding="utf-8") as outfile:
        writer = None

        for bills_path in bills_paths:
            try:
                state = bills_path.parents[2].name
                state_clean = letters_only(state)

                with open(bills_path, "r", newline="", encoding="utf-8") as infile:
                    reader = csv.reader(infile)

                    header = next(reader)

                    if not header_written:
                        writer = csv.writer(outfile)
                        writer.writerow(header + ["state"])
                        header_written = True

                    for row in reader:
                        writer.writerow(row + [state_clean])
                        total_rows += 1

            except Exception as e:
                print(f"Error processing {bills_path}: {e}")

    print(f"Done. Wrote {total_rows} rows to {output_path}")


def main():
    args = parse_args()

    input_path = Path(args.filename).expanduser()
    root_dir = resolve_root_dir(input_path)

    output_path = derive_output_path(input_path)

    if output_path.exists() and not args.overwrite:
        print(f"File exists: {output_path}")
        confirm = input("Overwrite? (y/n): ").strip().lower()
        if confirm != "y":
            print("Stopping.")
            sys.exit(0)

    stream_concat(root_dir, output_path)


if __name__ == "__main__":
    main()

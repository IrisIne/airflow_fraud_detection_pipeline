"""Build a compact three-month labeled dataset from the competition train.csv."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


MONTHS = ("2019-01", "2019-02", "2019-03")


def build(source: Path, destination: Path, per_month: int, positives_per_month: int) -> None:
    selected: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    with source.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        fieldnames = reader.fieldnames
        if not fieldnames or "target" not in fieldnames:
            raise ValueError("source CSV must contain the competition columns and target")
        for row in reader:
            month = row["transaction_time"][:7]
            target = row["target"]
            if month not in MONTHS or target not in {"0", "1"}:
                continue
            limit = positives_per_month if target == "1" else per_month - positives_per_month
            bucket = selected[(month, target)]
            if len(bucket) < limit:
                bucket.append(row)

    rows = []
    for month in MONTHS:
        rows.extend(selected[(month, "0")])
        rows.extend(selected[(month, "1")])
    rows.sort(key=lambda row: row["transaction_time"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {destination}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--per-month", type=int, default=1500)
    parser.add_argument("--positives-per-month", type=int, default=100)
    arguments = parser.parse_args()
    build(arguments.source, arguments.destination, arguments.per_month, arguments.positives_per_month)


if __name__ == "__main__":
    main()

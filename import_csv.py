import csv
import datetime
import sys
from pathlib import Path

from dotenv import load_dotenv

from db import ensure_db_and_tables, upsert_bill_months

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_int_or_none(value: str) -> int | None:
    v = value.strip()
    return int(v) if v else None


def _parse_row(row: dict) -> dict:
    print(row)
    month_name = row.get("Month", "").strip().lower()
    year = int(row["Year"].strip())
    month_num = MONTH_MAP.get(month_name)
    if not month_num:
        raise ValueError(f"Unknown month: {row.get('Month')!r}")

    month = datetime.date(year, month_num, 1)
    print(month)
    units = int(row.get("Units", "0").strip() or 0)
    cost  = int(row.get("Bill",  "0").strip() or 0)
    pres_read = _parse_int_or_none(row.get("Meter Reading", ""))

    return {
        "month": month,
        "units": units,
        "cost": cost,
        "prev_read": None,
        "pres_read": pres_read,
    }


def import_csv(path: Path) -> int:
    rows = []
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for i, row in enumerate(reader, start=2):   # row 1 = header
            try:
                rows.append(_parse_row(row))
            except (ValueError, KeyError) as e:
                print(f"  Skipping row {i}: {e}")

    if not rows:
        print("No valid rows found.")
        return 0

    return upsert_bill_months(rows)


def main():
    load_dotenv()
    ensure_db_and_tables()

    paths = [Path(p) for p in sys.argv[1:]]
    if not paths:
        print("Usage: uv run python import_csv.py <file1.csv> [file2.csv ...]")
        sys.exit(1)

    for path in paths:
        if not path.exists():
            print(f"File not found: {path}")
            continue
        print(f"Importing {path} ...")
        n = import_csv(path)
        print(f"  Inserted/updated {n} rows.")


if __name__ == "__main__":
    main()

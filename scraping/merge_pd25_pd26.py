"""Merge two pd25-style Data Warehouse exports (tests/pd25.csv and
tests/pd26.csv) into a single CSV.

Merge rule (as requested):
  - Every row of pd25.csv is kept as-is: it is the "trusted" version.
  - For each row of pd26.csv, look up its (cod Materia, matricola docente)
    key among pd25's rows. If that key already exists in pd25, drop the
    pd26 row (the pd25 version wins). If it does not exist in pd25, the
    pd26 row is appended at the end of the output.

Caveat logged at runtime: the (cod Materia, matricola docente) key is not
unique on its own -- a teacher can have several moduli of the same course.
When a key has a different number of rows in pd25 vs pd26, some pd26 rows
for that key are dropped in favour of pd25's even though they might not be
the exact same moduli; this is flagged so it can be checked by hand.
"""

from __future__ import annotations

import argparse
import csv
import logging
import pathlib
from collections import Counter
from typing import Any

from scraping._utils import configure_logging
from scraping.pd25 import ROOT_DIR, read_pd25_header, read_pd25_rows


LOGGER = logging.getLogger(pathlib.Path(__file__).stem)

DEFAULT_PD25_PATH = ROOT_DIR / "tests" / "pd25.csv"
DEFAULT_PD26_PATH = ROOT_DIR / "tests" / "pd26.csv"
DEFAULT_OUTPUT_PATH = ROOT_DIR / "tests" / "pd25_pd26_merged.csv"

SOURCE_COLUMN = "_source"
KEY_COLUMNS = ["cod Materia", "matricola docente"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pd25", type=pathlib.Path, default=DEFAULT_PD25_PATH)
    parser.add_argument("--pd26", type=pathlib.Path, default=DEFAULT_PD26_PATH)
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT_PATH)
    return parser.parse_args()


def row_key(row: dict[str, str]) -> tuple[str, ...]:
    return tuple((row.get(column) or "").strip() for column in KEY_COLUMNS)


def main() -> int:
    args = parse_args()
    configure_logging()

    columns25 = read_pd25_header(args.pd25)
    columns26 = read_pd25_header(args.pd26)
    only25 = [column for column in columns25 if column not in columns26]
    only26 = [column for column in columns26 if column not in columns25]
    if only25 or only26:
        LOGGER.warning(
            "%s and %s do NOT have exactly the same columns -- only in pd25=%s, only in pd26=%s.",
            args.pd25.name,
            args.pd26.name,
            only25,
            only26,
        )
    else:
        LOGGER.info("%s and %s have exactly the same columns.", args.pd25.name, args.pd26.name)

    rows25 = read_pd25_rows(args.pd25)
    rows26 = read_pd25_rows(args.pd26)

    pd25_key_counts = Counter(row_key(row) for _, row in rows25)
    pd26_key_counts = Counter(row_key(row) for _, row in rows26)

    for key, pd26_count in pd26_key_counts.items():
        pd25_count = pd25_key_counts.get(key, 0)
        if pd25_count and pd25_count != pd26_count:
            LOGGER.warning(
                "EDGE CASE: key cod Materia=%r matricola docente=%r has %d row(s) in pd25 but %d in pd26 "
                "-- all pd26 rows for this key are dropped in favour of pd25's, verify by hand that no "
                "modulo was lost.",
                key[0],
                key[1],
                pd25_count,
                pd26_count,
            )

    matched_pd26_count = sum(count for key, count in pd26_key_counts.items() if pd25_key_counts.get(key))
    unmatched_pd26 = [(line, row) for line, row in rows26 if not pd25_key_counts.get(row_key(row))]

    merged_rows: list[dict[str, str]] = []
    for _, row in rows25:
        merged_row = dict(row)
        merged_row[SOURCE_COLUMN] = "pd25"
        merged_rows.append(merged_row)
    for _, row in unmatched_pd26:
        merged_row = dict(row)
        merged_row[SOURCE_COLUMN] = "pd26"
        merged_rows.append(merged_row)

    LOGGER.info(
        "pd25: %d rows (all kept). pd26: %d rows -- %d dropped (key already in pd25), %d appended (new key).",
        len(rows25),
        len(rows26),
        matched_pd26_count,
        len(unmatched_pd26),
    )

    output_fieldnames = list(dict.fromkeys(columns25 + columns26)) + [SOURCE_COLUMN]
    with args.output.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=output_fieldnames, restval="")
        writer.writeheader()
        writer.writerows(merged_rows)

    LOGGER.info("Wrote %d merged rows to %s.", len(merged_rows), args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

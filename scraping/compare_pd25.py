"""Cross-reference our crawled data/courses/<teacher>/<year>/teaching-*.yml
files against the external pd25.csv dataset (per-teaching data supplied by
Claudio, richer on some fields such as "codice fascia"/copertura but with
fewer teachings overall than what we crawl).

Produces a single CSV with three sections, in this order:
  1. My teachings that have a match in pd25.csv: my fields + the matched
     pd25 columns.
  2. My teachings with no match in pd25.csv: my fields, pd25 columns blank.
  3. pd25.csv rows with no match among my teachings: pd25 columns, my
     fields blank.

Matching is tried in cascading tiers, from strongest to weakest, since a
single (year, cod Materia, matricola docente) key misses real matches
whenever pd25 uses a "corso integrato" container code (see cod_integrato)
or when a course's own code differs for other reasons but teacher+title
still agree. Only the strong tiers (see STRONG_MATCH_METHODS) count as a
match for section 1/3 purposes; a weaker "same course, different teacher"
hit is recorded as a note but does not join the rows.
"""

from __future__ import annotations

import argparse
import csv
import logging
import pathlib
import shlex
import sys
from collections import defaultdict
from typing import Any

import yaml

from scraping._utils import configure_logging
from scraping.pd25 import (
    ROOT_DIR,
    academic_year_start,
    candidate_failures,
    is_placeholder,
    iter_teaching_files,
    normalize_course_title,
    normalize_id,
    read_pd25_rows,
)


LOGGER = logging.getLogger(pathlib.Path(__file__).stem)

DEFAULT_OUTPUT_PATH = ROOT_DIR / "tests" / "pd25_comparison.csv"

SECTION_MATCHED = "1_matched"
SECTION_MINE_ONLY = "2_mine_only"
SECTION_PD25_ONLY = "3_pd25_only"

MY_COLUMNS = [
    "my_path",
    "my_year",
    "my_course_id",
    "my_course_name",
    "my_teacher_id",
    "my_teacher_name",
    "my_teacher_email",
    "my_teacher_role",
    "my_teacher_affiliation",
    "my_teacher_ssd",
    "my_credits",
    "my_ssd",
    "my_language",
    "my_teaching_mode",
    "my_campus",
    "my_schedule_from",
    "my_schedule_to",
    "my_programmes",
]
STRONG_MATCH_METHODS = {"exact_cod_materia", "exact_cod_integrato", "teacher_title_fuzzy"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Where to write the 3-section comparison CSV (default: {DEFAULT_OUTPUT_PATH}).",
    )
    return parser.parse_args()


def load_my_teachings() -> list[tuple[pathlib.Path, dict[str, Any]]]:
    teachings: list[tuple[pathlib.Path, dict[str, Any]]] = []
    for path in iter_teaching_files():
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(payload, dict):
            teachings.append((path, payload))
    return teachings


def my_row(path: pathlib.Path, payload: dict[str, Any]) -> dict[str, str]:
    teacher = payload.get("teacher") or {}
    teacher_ssd = teacher.get("ssd") or {}
    course_title = payload.get("course_title") or {}
    schedule = payload.get("schedule") or {}
    programmes = [p for p in payload.get("programmes") or [] if isinstance(p, dict)]

    programme_name_values: set[str] = set()
    for programme in programmes:
        names = programme.get("name") or {}
        if isinstance(names, dict):
            programme_name_values.update(str(value) for value in names.values() if value)
        elif names:
            programme_name_values.add(str(names))

    return {
        "my_path": str(path.relative_to(ROOT_DIR)),
        "my_year": str(payload.get("year") or ""),
        "my_course_id": str(course_title.get("id") or ""),
        "my_course_name": str(course_title.get("name") or ""),
        "my_teacher_id": str(teacher.get("id") or ""),
        "my_teacher_name": str(teacher.get("name") or ""),
        "my_teacher_email": str(teacher.get("email") or ""),
        "my_teacher_role": ", ".join(teacher.get("role") or []),
        "my_teacher_affiliation": str(teacher.get("affiliation") or ""),
        "my_teacher_ssd": str(teacher_ssd.get("name") or ""),
        "my_credits": str(payload.get("credits") or ""),
        "my_ssd": str(payload.get("ssd") or ""),
        "my_language": str(payload.get("language") or ""),
        "my_teaching_mode": str(payload.get("teaching_mode") or ""),
        "my_campus": str(payload.get("campus") or ""),
        "my_schedule_from": str(schedule.get("from") or ""),
        "my_schedule_to": str(schedule.get("to") or ""),
        "my_programmes": "; ".join(sorted(programme_name_values)),
    }


class Pd25Indices:
    """Lookup structures over pd25 rows, supporting the cascading match tiers."""

    def __init__(self, rows: list[tuple[int, dict[str, str]]]) -> None:
        self.by_materia_teacher: dict[tuple[str, str, str], list[tuple[int, dict[str, str]]]] = defaultdict(list)
        self.by_integrato_teacher: dict[tuple[str, str, str], list[tuple[int, dict[str, str]]]] = defaultdict(list)
        self.by_materia_only: dict[tuple[str, str], list[tuple[int, dict[str, str]]]] = defaultdict(list)
        self.by_integrato_only: dict[tuple[str, str], list[tuple[int, dict[str, str]]]] = defaultdict(list)
        self.by_teacher_year: dict[tuple[str, str], list[tuple[int, dict[str, str]]]] = defaultdict(list)

        for row_number, row in rows:
            year = academic_year_start(row.get("A.A."))
            teacher_id = normalize_id(row.get("matricola docente"))
            materia = normalize_id(row.get("cod Materia"))
            integrato = normalize_id(row.get("cod_integrato"))

            self.by_materia_teacher[(year, materia, teacher_id)].append((row_number, row))
            self.by_materia_only[(year, materia)].append((row_number, row))

            if integrato and integrato != materia:
                self.by_integrato_teacher[(year, integrato, teacher_id)].append((row_number, row))
                self.by_integrato_only[(year, integrato)].append((row_number, row))

            if not is_placeholder(row.get("matricola docente")):
                self.by_teacher_year[(year, teacher_id)].append((row_number, row))


def match_teaching(
    payload: dict[str, Any],
    indices: Pd25Indices,
) -> tuple[tuple[int, dict[str, str]] | None, str, str]:
    """Returns (matched_pd25_row_or_None, match_method, note)."""
    teacher = payload.get("teacher") or {}
    course_title = payload.get("course_title") or {}
    year = str(payload.get("year") or "")
    course_id = normalize_id(course_title.get("id"))
    teacher_id = normalize_id(teacher.get("id"))
    course_name_norm = normalize_course_title(course_title.get("name"))

    key = (year, course_id, teacher_id)

    candidates = indices.by_materia_teacher.get(key)
    if candidates:
        return candidates[0], "exact_cod_materia", ""

    candidates = indices.by_integrato_teacher.get(key)
    if candidates:
        return candidates[0], "exact_cod_integrato", ""

    if course_name_norm:
        for candidate in indices.by_teacher_year.get((year, teacher_id), []):
            _, row = candidate
            if normalize_course_title(row.get("Materia reale")) == course_name_norm:
                return candidate, "teacher_title_fuzzy", ""

    weak_candidates = indices.by_materia_only.get((year, course_id)) or indices.by_integrato_only.get((year, course_id))
    if weak_candidates:
        _, weak_row = weak_candidates[0]
        other_teacher = f"{weak_row.get('cognome docente', '')} {weak_row.get('nome docente', '')}".strip()
        other_matricola = weak_row.get("matricola docente")
        note = f"course found in pd25 under different/unassigned teacher: {other_teacher!r} (matricola {other_matricola!r})"
        return None, "course_only_different_teacher", note

    return None, "none", ""


def build_my_side_rows(
    teachings: list[tuple[pathlib.Path, dict[str, Any]]],
    indices: Pd25Indices,
    pd25_fieldnames: list[str],
) -> tuple[list[dict[str, str]], list[dict[str, str]], set[int]]:
    """Returns (matched_rows, mine_only_rows, used_pd25_row_numbers)."""
    matched_rows: list[dict[str, str]] = []
    mine_only_rows: list[dict[str, str]] = []
    used_pd25_row_numbers: set[int] = set()

    for path, payload in teachings:
        row_out = my_row(path, payload)
        for column in pd25_fieldnames:
            row_out[f"pd25_{column}"] = ""

        matched, method, note = match_teaching(payload, indices)
        row_out["match_method"] = method
        row_out["match_notes"] = note

        if matched is not None and method in STRONG_MATCH_METHODS:
            row_number, pd25_row = matched
            used_pd25_row_numbers.add(row_number)
            for column in pd25_fieldnames:
                row_out[f"pd25_{column}"] = pd25_row.get(column, "")
            mismatches = candidate_failures(row=pd25_row, row_number=row_number, path=path, payload=payload)
            if mismatches:
                row_out["match_notes"] = "; ".join(mismatches)
            row_out["section"] = SECTION_MATCHED
            matched_rows.append(row_out)
        else:
            row_out["section"] = SECTION_MINE_ONLY
            mine_only_rows.append(row_out)

    return matched_rows, mine_only_rows, used_pd25_row_numbers


def build_pd25_only_rows(
    rows: list[tuple[int, dict[str, str]]],
    used_pd25_row_numbers: set[int],
    my_columns: list[str],
    pd25_fieldnames: list[str],
) -> list[dict[str, str]]:
    pd25_only_rows: list[dict[str, str]] = []
    for row_number, row in rows:
        if row_number in used_pd25_row_numbers:
            continue

        row_out = {column: "" for column in my_columns}
        for column in pd25_fieldnames:
            row_out[f"pd25_{column}"] = row.get(column, "")
        row_out["section"] = SECTION_PD25_ONLY
        row_out["match_method"] = ""
        row_out["match_notes"] = ""
        pd25_only_rows.append(row_out)
    return pd25_only_rows


def write_csv(path: pathlib.Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    configure_logging()
    LOGGER.info("Command line: %s", shlex.join(sys.argv))
    args = parse_args()

    teachings = load_my_teachings()
    pd25_rows = read_pd25_rows()
    pd25_fieldnames = list(pd25_rows[0][1].keys()) if pd25_rows else []

    indices = Pd25Indices(pd25_rows)

    matched_rows, mine_only_rows, used_pd25_row_numbers = build_my_side_rows(teachings, indices, pd25_fieldnames)
    pd25_only_rows = build_pd25_only_rows(pd25_rows, used_pd25_row_numbers, MY_COLUMNS, pd25_fieldnames)

    all_rows = matched_rows + mine_only_rows + pd25_only_rows
    fieldnames = ["section"] + MY_COLUMNS + [f"pd25_{column}" for column in pd25_fieldnames] + ["match_method", "match_notes"]
    write_csv(args.output, all_rows, fieldnames)

    LOGGER.info(
        "Section 1 (matched): %s. Section 2 (mine only): %s. Section 3 (pd25 only): %s.",
        len(matched_rows),
        len(mine_only_rows),
        len(pd25_only_rows),
    )
    LOGGER.info("Wrote %s (%s rows total)", args.output, len(all_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

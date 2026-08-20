"""Apply per-teaching coverage data from tests/pd25_pd26_merged.csv onto the
matching data/courses/<teacher>/2025/teaching-<id>.yml files.

For each row of the merged CSV:
  1. matricola docente -> data/contacts.csv uid -> email -> the email's
     username is the teacher's folder name (data/courses/<username>/2025/).
  2. cod_integrato and, if different, cod Materia (for a "corso integrato"
     C.I., cod Materia is the container code shared by all its
     sub-disciplines while cod_integrato is the component course code --
     usually course-*.yml is named after cod_integrato, but a few are named
     after the container code instead, so both are tried) -> every
     course-<code>.yml AND course-<code>-*.yml file in that folder for
     either code (there can be several: the same code is sometimes reused
     across unrelated offerings, each with its own -A/-B/-C/-D variant file
     and its own teaching_id).
  3. In each such file, the teachers[] entry whose id matches the matricola.
  4. Among that teacher's modules[], the one scoring highest against the
     row's "Codice sdoppiamento reale" / "modulo" (+1 per expectation an
     entry in `details` explicitly confirms; -1 if `details` carries a
     CL.x-like tag the row doesn't expect at all; entries like "12 cfu"
     score 0, they carry no information either way). An explicit
     contradiction -- a different CL.x or a different "Module N" -- excludes
     that module outright. If more than one module ties for the top score,
     or none score high enough to stand out, the row is left unresolved
     rather than guessed.
  5. That module's teaching_id -> data/courses/<username>/2025/teaching-
     <teaching_id>.yml, which gets a `coverage:` block appended (not
     rewritten) with: cod_copertura, desc_copertura, gratuito_retribuito,
     ore_frontali_erogato, ore_contratto_erogato, ore_frontali_erogato_molt,
     cfu_erogati, cfu_erogati_molt.

Without --apply this only prints a resolution report; no file is touched.
"""

from __future__ import annotations

import argparse
import csv
import logging
import pathlib
import re
from collections import Counter
from typing import Any

import yaml

from scraping._utils import configure_logging
from scraping.pd25 import ROOT_DIR, clean, is_placeholder, normalize_id


LOGGER = logging.getLogger(pathlib.Path(__file__).stem)

CONTACTS_PATH = ROOT_DIR / "data" / "contacts.csv"
COURSES_DIR = ROOT_DIR / "data" / "courses"
DEFAULT_MERGED_PATH = ROOT_DIR / "tests" / "pd25_pd26_merged.csv"
DEFAULT_UNRESOLVED_PATH = ROOT_DIR / "tests" / "pd_coverage_unresolved.csv"
YEAR_DIR = "2025"

MODULE_NUMBER_RE = re.compile(r"^(?:module\s*)?(\d+)$")
CREDITS_RE = re.compile(r"^\d+(?:\.\d+)?\s*cfu$")
DIGITS_RE = re.compile(r"(\d+)")

COVERAGE_COLUMNS = {
    "Cod. copertura": ("cod_copertura", "str"),
    "desc copertura": ("desc_copertura", "str"),
    "Gratuito/Retribuito": ("gratuito_retribuito", "str"),
    "Tot. Ore frontali (erogato)": ("ore_frontali_erogato", "num"),
    "Ore Contratto (erogato)": ("ore_contratto_erogato", "num"),
    "Tot. Ore frontali (erogato) molt.": ("ore_frontali_erogato_molt", "num"),
    "CFU erogati": ("cfu_erogati", "num"),
    "CFU erogati molt.": ("cfu_erogati_molt", "num"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--merged", type=pathlib.Path, default=DEFAULT_MERGED_PATH)
    parser.add_argument("--apply", action="store_true", help="Actually append coverage blocks (default: dry-run).")
    parser.add_argument("--unresolved-report", type=pathlib.Path, default=DEFAULT_UNRESOLVED_PATH)
    return parser.parse_args()


def load_contacts() -> dict[str, str]:
    with CONTACTS_PATH.open(encoding="utf-8", newline="") as file:
        return {normalize_id(row["uid"]): row["email"] for row in csv.DictReader(file) if row.get("email")}


def load_merged_rows(path: pathlib.Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def normalize_detail(value: Any) -> str:
    return " ".join(str(value).strip().lower().replace("modulo", "module").split())


def score_module(details: list[Any] | None, expected_csr: str | None, expected_modulo_num: str | None) -> int | None:
    """Returns None if `details` explicitly contradicts expected_csr/expected_modulo_num
    (hard exclusion), else a score where higher = more of the expectations confirmed.
    A CL.x-like token in `details` when the row expects none (expected_csr is None) is
    a mild negative signal rather than a conflict: it means this specific module has an
    explicit channel/sdoppiamento the row's own account doesn't have, so it should lose
    to a candidate module with no such tag when both otherwise fit."""
    score = 0
    saw_csr_like_token = False

    for raw in details or []:
        token = normalize_detail(raw)
        module_match = MODULE_NUMBER_RE.match(token)
        if module_match:
            if expected_modulo_num is not None:
                if module_match.group(1) == expected_modulo_num:
                    score += 1
                else:
                    return None
            continue
        if CREDITS_RE.match(token):
            continue

        saw_csr_like_token = True
        if expected_csr is not None:
            if token == expected_csr:
                score += 1
            else:
                return None

    if expected_csr is None and saw_csr_like_token:
        score -= 1
    return score


def find_course_files(teacher_dir: pathlib.Path, codes: list[str]) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    seen: set[pathlib.Path] = set()
    for code in codes:
        candidates = [teacher_dir / f"course-{code}.yml"] + sorted(teacher_dir.glob(f"course-{code}-*.yml"))
        for candidate in candidates:
            if candidate.exists() and candidate not in seen:
                seen.add(candidate)
                files.append(candidate)
    return files


def find_teaching_id(
    teacher_dir: pathlib.Path, codes: list[str], matricola_norm: str, expected_csr: str | None, expected_modulo_num: str | None
) -> tuple[str | None, str, list[str]]:
    course_files = find_course_files(teacher_dir, codes)
    if not course_files:
        return None, "no_course_file", []

    candidates: list[tuple[int, str]] = []  # (score, teaching_id)
    for course_file in course_files:
        payload = yaml.safe_load(course_file.read_text(encoding="utf-8")) or {}
        for teacher in payload.get("teachers") or []:
            if normalize_id(teacher.get("id")) != matricola_norm:
                continue
            for module in teacher.get("modules") or []:
                score = score_module(module.get("details"), expected_csr, expected_modulo_num)
                if score is not None:
                    candidates.append((score, str(module.get("teaching_id"))))

    if not candidates:
        return None, "no_module_match", []

    best_score = max(score for score, _ in candidates)
    winners = sorted({teaching_id for score, teaching_id in candidates if score == best_score})
    if len(winners) != 1:
        return None, "ambiguous_module_match", winners
    return winners[0], "ok", winners


def parse_number(raw: str) -> Any:
    text = raw.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def build_coverage(row: dict[str, str]) -> dict[str, Any]:
    coverage: dict[str, Any] = {}
    for column, (key, kind) in COVERAGE_COLUMNS.items():
        value = clean(row.get(column))
        coverage[key] = parse_number(value) if kind == "num" else value
    return coverage


def append_coverage(teaching_path: pathlib.Path, coverage: dict[str, Any]) -> bool:
    existing = yaml.safe_load(teaching_path.read_text(encoding="utf-8")) or {}
    if "coverage" in existing:
        return False
    with teaching_path.open("a", encoding="utf-8") as file:
        file.write("\n" + yaml.safe_dump({"coverage": coverage}, allow_unicode=True, sort_keys=False))
    return True


def main() -> int:
    args = parse_args()
    configure_logging()

    contacts = load_contacts()
    rows = load_merged_rows(args.merged)

    resolved: list[tuple[dict[str, str], pathlib.Path]] = []
    unresolved: list[dict[str, str]] = []
    skip_reasons: Counter[str] = Counter()

    def record_unresolved(row: dict[str, str], reason: str, detail: str = "") -> None:
        skip_reasons[reason] += 1
        unresolved.append(
            {
                "reason": reason,
                "detail": detail,
                "cod Materia": row.get("cod Materia", ""),
                "cod_integrato": row.get("cod_integrato", ""),
                "Materia reale": row.get("Materia reale", ""),
                "matricola docente": row.get("matricola docente", ""),
                "cognome docente": row.get("cognome docente", ""),
                "nome docente": row.get("nome docente", ""),
                "Codice sdoppiamento reale": row.get("Codice sdoppiamento reale", ""),
                "modulo": row.get("modulo", ""),
                "_source": row.get("_source", ""),
            }
        )

    for row in rows:
        matricola_norm = normalize_id(row.get("matricola docente"))
        email = contacts.get(matricola_norm)
        if not email:
            record_unresolved(row, "no_contact")
            continue

        username = email.split("@", 1)[0]
        teacher_dir = COURSES_DIR / username / YEAR_DIR
        if not teacher_dir.is_dir():
            record_unresolved(row, "no_teacher_folder", str(teacher_dir.relative_to(ROOT_DIR)))
            continue

        cod_integrato = clean(row.get("cod_integrato"))
        cod_materia = clean(row.get("cod Materia"))
        codes = [code for code in dict.fromkeys([cod_integrato, cod_materia]) if code]
        csr_raw = clean(row.get("Codice sdoppiamento reale"))
        expected_csr = None if is_placeholder(csr_raw) else normalize_detail(csr_raw)
        modulo_raw = clean(row.get("modulo"))
        expected_modulo_num = None
        if not is_placeholder(modulo_raw):
            digits = DIGITS_RE.search(modulo_raw)
            expected_modulo_num = digits.group(1) if digits else None

        teaching_id, reason, candidates = find_teaching_id(teacher_dir, codes, matricola_norm, expected_csr, expected_modulo_num)
        if reason != "ok":
            detail = f"{teacher_dir.relative_to(ROOT_DIR)}" + (f" candidates={candidates}" if candidates else "")
            record_unresolved(row, reason, detail)
            continue

        teaching_path = teacher_dir / f"teaching-{teaching_id}.yml"
        if not teaching_path.is_file():
            record_unresolved(row, "no_teaching_file", str(teaching_path.relative_to(ROOT_DIR)))
            continue

        resolved.append((row, teaching_path))

    LOGGER.info("Resolved %d/%d rows. Unresolved by reason: %s", len(resolved), len(rows), dict(skip_reasons))

    unresolved_fieldnames = [
        "reason",
        "detail",
        "cod Materia",
        "cod_integrato",
        "Materia reale",
        "matricola docente",
        "cognome docente",
        "nome docente",
        "Codice sdoppiamento reale",
        "modulo",
        "_source",
    ]
    with args.unresolved_report.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=unresolved_fieldnames)
        writer.writeheader()
        writer.writerows(unresolved)
    LOGGER.info("Wrote %d unresolved row(s) to %s.", len(unresolved), args.unresolved_report)

    if not args.apply:
        LOGGER.info("Dry-run: no files written. Re-run with --apply to write coverage blocks.")
        for row, teaching_path in resolved[:10]:
            LOGGER.info("  would append to %s <- cod Materia=%r matricola=%r", teaching_path.relative_to(ROOT_DIR), row.get("cod Materia"), row.get("matricola docente"))
        return 0

    written = 0
    already_had = 0
    for row, teaching_path in resolved:
        coverage = build_coverage(row)
        if append_coverage(teaching_path, coverage):
            written += 1
        else:
            already_had += 1

    LOGGER.info("Appended coverage to %d file(s); %d already had a coverage block and were left untouched.", written, already_had)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

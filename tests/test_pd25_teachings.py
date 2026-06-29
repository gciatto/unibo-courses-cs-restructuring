import csv
import pathlib
import re
import unittest
import warnings
from collections import defaultdict
from collections.abc import Callable
from typing import Any

import yaml

from _utils import normalize_programme_title
from resources import classify_dept, classify_role


ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
CSV_PATH = ROOT_DIR / "tests" / "pd25.csv"
COURSES_DIR = ROOT_DIR / "data" / "courses"

PLACEHOLDER_VALUES = {
    "",
    "-",
    "'-",
    "-1",
    "0",
    "000000",
    "000000000000",
    "campus non definito",
    "n.d.",
    "n.d",
    "non assegnato",
}

SSD_ALIASES = {
    "agr/01": {"agr/01", "agri-01/a"},
    "agr/10": {"agr/10", "agri-04/c"},
    "fis/05": {"fis/05", "phys-05/a"},
    "fis/07": {"fis/07", "phys-06/a"},
    "inf/01": {"inf/01", "info-01/a"},
    "ing-ind/19": {"ing-ind/19", "iind-07/d"},
    "ing-ind/20": {"ing-ind/20", "iind-07/e"},
    "ing-inf/01": {"ing-inf/01", "iinf-01/a"},
    "ing-inf/03": {"ing-inf/03", "iinf-03/a"},
    "ing-inf/05": {"ing-inf/05", "iinf-05/a"},
    "ius/20": {"ius/20", "giur-17/a", "phil-02/a"},
    "mat/08": {"mat/08", "math-05/a"},
    "mat/09": {"mat/09", "math-06/a"},
    "m-sto/08": {"m-sto/08", "hist-04/c"},
}


def clean(value: Any) -> str:
    return str(value or "").replace("\xa0", " ").strip()


def normalize_text(value: Any) -> str:
    return " ".join(clean(value).split()).casefold()


def is_placeholder(value: Any) -> bool:
    return normalize_text(value).strip("'") in PLACEHOLDER_VALUES


def normalize_id(value: Any) -> str:
    text = clean(value).strip("'")
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    if text.isdigit():
        text = str(int(text))
    return text.casefold()


def normalize_course_title(value: Any) -> str:
    text = normalize_text(value)
    text = re.sub(r"\s*[-–—]\s*\d+\s*cfu\s*$", " ", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\s*\((?:\d+|l|lm|lt|lmcu|i\.?c\.?|c\.?i\.?|\d+\s*cfu)\)\s*",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\b\d+\s*cfu\b", " ", text, flags=re.IGNORECASE)
    return " ".join(text.split())


def normalize_ssd(value: Any) -> str:
    return normalize_text(value)


def ssd_matches(expected: Any, actual: Any) -> bool:
    if is_placeholder(expected):
        return True

    normalized_expected = normalize_ssd(expected)
    normalized_actual = normalize_ssd(actual)
    return normalized_actual in SSD_ALIASES.get(normalized_expected, {normalized_expected})


def programme_names(programme: dict[str, Any]) -> set[str]:
    names = programme.get("name") or {}
    if isinstance(names, dict):
        return {normalize_programme_title(str(value)) for value in names.values() if value}
    if names:
        return {normalize_programme_title(str(names))}
    return set()


def programme_duration_check(tipo_corso: Any) -> Callable[[Any], bool] | None:
    tipo = normalize_text(tipo_corso)
    if tipo == "l":
        return lambda duration: duration == 3
    if tipo == "lm":
        return lambda duration: duration == 2
    if tipo == "lmcu":
        return lambda duration: isinstance(duration, int) and duration >= 4
    return None


def warn_unsupported_tipo(row_number: int, row: dict[str, str]) -> None:
    if programme_duration_check(row.get("Tipo Corso")) is None:
        warnings.warn(
            f"row {row_number}: skipping unsupported Tipo Corso={row.get('Tipo Corso')!r}",
            RuntimeWarning,
            stacklevel=2,
        )


def academic_year_start(value: Any) -> str:
    text = clean(value)
    return text.split("/", 1)[0] if "/" in text else text


def read_pd25_rows() -> list[tuple[int, dict[str, str]]]:
    with CSV_PATH.open(encoding="utf-8-sig", newline="") as file:
        return list(enumerate(csv.DictReader(file), start=2))


def iter_teaching_files() -> list[pathlib.Path]:
    return sorted(COURSES_DIR.glob("*/*/teaching-*.yml"))


def teaching_key(payload: dict[str, Any]) -> tuple[str, str, str]:
    teacher = payload.get("teacher") or {}
    course_title = payload.get("course_title") or {}
    return (
        str(payload.get("year") or ""),
        normalize_id(course_title.get("id")),
        normalize_id(teacher.get("id")),
    )


def row_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        academic_year_start(row.get("A.A.")),
        normalize_id(row.get("cod Materia")),
        normalize_id(row.get("matricola docente")),
    )


def load_teachings_by_key() -> dict[tuple[str, str, str], list[tuple[pathlib.Path, dict[str, Any]]]]:
    teachings_by_key: dict[tuple[str, str, str], list[tuple[pathlib.Path, dict[str, Any]]]] = defaultdict(list)
    for path in iter_teaching_files():
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(payload, dict):
            teachings_by_key[teaching_key(payload)].append((path, payload))
    return teachings_by_key


def normalized_programme_department(row: dict[str, str]) -> str | None:
    value = row.get("Dipartimento di riferimento")
    if is_placeholder(value):
        return None
    return classify_dept(clean(value))


def normalized_teacher_affiliation(row: dict[str, str]) -> str | None:
    value = row.get("acronimo DIP")
    if is_placeholder(value):
        return None
    return classify_dept(clean(value))


def candidate_failures(
    *,
    row: dict[str, str],
    row_number: int,
    path: pathlib.Path,
    payload: dict[str, Any],
) -> list[str]:
    failures: list[str] = []
    teacher = payload.get("teacher") or {}
    teacher_ssd = teacher.get("ssd") or {}
    course_title = payload.get("course_title") or {}
    programmes = [programme for programme in payload.get("programmes") or [] if isinstance(programme, dict)]

    expected_department = normalized_programme_department(row)
    if expected_department is None and not is_placeholder(row.get("Dipartimento di riferimento")):
        failures.append(f"unrecognized Dipartimento di riferimento={row.get('Dipartimento di riferimento')!r}")
    elif expected_department and not any(normalize_text(programme.get("department")) == expected_department for programme in programmes):
        failures.append(f"no programme department={expected_department!r}")

    duration_check = programme_duration_check(row.get("Tipo Corso"))
    if duration_check is not None and not any(duration_check(programme.get("duration")) for programme in programmes):
        failures.append(f"no programme duration matching Tipo Corso={row.get('Tipo Corso')!r}")

    expected_programme = normalize_programme_title(clean(row.get("Corso di Studio")))
    if expected_programme and not any(expected_programme in programme_names(programme) for programme in programmes):
        failures.append(f"no programme named {row.get('Corso di Studio')!r}")

    if not is_placeholder(row.get("sede didattica")) and normalize_text(row.get("sede didattica")) != normalize_text(payload.get("campus")):
        failures.append(f"campus {payload.get('campus')!r} != sede didattica {row.get('sede didattica')!r}")

    if normalize_id(row.get("cod Materia")) != normalize_id(course_title.get("id")):
        failures.append(f"course_title.id {course_title.get('id')!r} != cod Materia {row.get('cod Materia')!r}")

    if normalize_course_title(row.get("Materia reale")) != normalize_course_title(course_title.get("name")):
        failures.append(f"course_title.name {course_title.get('name')!r} != Materia reale {row.get('Materia reale')!r}")

    if not ssd_matches(row.get("SSD materia"), payload.get("ssd")):
        failures.append(f"ssd {payload.get('ssd')!r} != SSD materia {row.get('SSD materia')!r}")

    if normalize_id(row.get("matricola docente")) != normalize_id(teacher.get("id")):
        failures.append(f"teacher.id {teacher.get('id')!r} != matricola docente {row.get('matricola docente')!r}")

    teacher_name = normalize_text(teacher.get("name"))
    for column in ("cognome docente", "nome docente"):
        expected_name_part = normalize_text(row.get(column))
        if expected_name_part and not is_placeholder(expected_name_part) and expected_name_part not in teacher_name:
            failures.append(f"teacher.name {teacher.get('name')!r} does not contain {column}={row.get(column)!r}")

    if not ssd_matches(row.get("SSD docente"), teacher_ssd.get("name")):
        failures.append(f"teacher.ssd.name {teacher_ssd.get('name')!r} != SSD docente {row.get('SSD docente')!r}")

    if not is_placeholder(row.get("Ruolo docente")):
        expected_roles = classify_role(clean(row.get("Ruolo docente")))
        if not expected_roles:
            failures.append(f"unrecognized Ruolo docente={row.get('Ruolo docente')!r}")
        else:
            actual_roles = {normalize_text(role) for role in teacher.get("role") or []}
            if not {normalize_text(role) for role in expected_roles} & actual_roles:
                failures.append(f"teacher.role {teacher.get('role')!r} does not match Ruolo docente={row.get('Ruolo docente')!r}")

    expected_affiliation = normalized_teacher_affiliation(row)
    if expected_affiliation is None and not is_placeholder(row.get("acronimo DIP")):
        failures.append(f"unrecognized acronimo DIP={row.get('acronimo DIP')!r}")
    elif expected_affiliation and normalize_text(expected_affiliation) != normalize_text(teacher.get("affiliation")):
        failures.append(f"teacher.affiliation {teacher.get('affiliation')!r} != acronimo DIP {row.get('acronimo DIP')!r}")

    if failures:
        relative_path = path.relative_to(ROOT_DIR)
        return [f"{relative_path}: {failure}" for failure in failures]
    return []


def format_row_context(row_number: int, row: dict[str, str]) -> str:
    return (
        f"row={row_number} "
        f"year={academic_year_start(row.get('A.A.'))!r} "
        f"cod Materia={row.get('cod Materia')!r} "
        f"matricola docente={row.get('matricola docente')!r} "
        f"Materia reale={row.get('Materia reale')!r}"
    )


class TestPd25Teachings(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rows = read_pd25_rows()
        cls.teachings_by_key = load_teachings_by_key()

    def test_rows_are_represented_by_yaml_teachings(self):
        for row_number, row in self.rows:
            with self.subTest(
                row=row_number,
                teaching=row.get("cod Materia"),
                teacher=row.get("cognome docente"),
                course=row.get("Materia reale"),
            ):
                warn_unsupported_tipo(row_number, row)
                key = row_key(row)
                candidates = self.teachings_by_key.get(key, [])
                context = format_row_context(row_number, row)
                self.assertTrue(candidates, f"No YAML teaching found for {context} using key={key!r}")

                failures_by_candidate = [
                    candidate_failures(row=row, row_number=row_number, path=path, payload=payload)
                    for path, payload in candidates
                ]
                if any(not failures for failures in failures_by_candidate):
                    continue

                details = "\n".join(failure for failures in failures_by_candidate for failure in failures)
                self.fail(f"No YAML teaching candidate satisfies {context}.\n{details}")


if __name__ == "__main__":
    unittest.main()

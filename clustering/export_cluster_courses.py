from __future__ import annotations

import argparse
import csv
import pathlib
from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence

import yaml


FIELDNAMES = [
    "cluster_id",
    "cluster_name",
    "course_id",
    "course_name",
    "year",
    "teachers",
    "teachers_affiliations",
    "teachers_ssds",
    "teachers_roles",
    "campus",
    "credits",
    "ssd",
    "language",
    "programmes_codes",
    "programmes_names",
    "programmes_types",
    "programmes_affiliations",
    "course_scope",
    "course_url",
]

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _load_yaml_mapping(path: pathlib.Path, description: str) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as error:
        raise ValueError(f"{description} does not exist: {path}") from error
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f"Could not read {description} {path}: {error}") from error
    if not isinstance(payload, dict):
        raise ValueError(f"{description} must contain a YAML mapping: {path}")
    return payload


def _strings(values: Any) -> list[str]:
    if values is None:
        return []
    items = values if isinstance(values, list) else [values]
    return [str(item).strip() for item in items if item is not None and str(item).strip()]


def _sorted_unique(values: Iterable[Any]) -> list[str]:
    by_normalized_value: dict[str, str] = {}
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            by_normalized_value.setdefault(text.casefold(), text)
    return sorted(by_normalized_value.values(), key=lambda value: (value.casefold(), value))


def _join(values: Iterable[Any]) -> str:
    return ", ".join(_sorted_unique(values))


def _format_number(value: Any) -> str:
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return str(value).strip()
    if not number.is_finite():
        return str(value).strip()
    normalized = format(number.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized.replace(".", ",")


def _join_credits(values: Any) -> str:
    items = values if isinstance(values, list) else ([] if values is None else [values])
    unique: dict[Decimal | str, str] = {}
    for value in items:
        formatted = _format_number(value)
        if not formatted:
            continue
        try:
            key: Decimal | str = Decimal(str(value).strip())
        except (InvalidOperation, ValueError):
            key = formatted.casefold()
        unique.setdefault(key, formatted)

    def sort_key(item: tuple[Decimal | str, str]) -> tuple[int, Any]:
        key, formatted = item
        return (0, key) if isinstance(key, Decimal) else (1, formatted.casefold())

    return ", ".join(formatted for _, formatted in sorted(unique.items(), key=sort_key))


def _resolve_course_path(raw_path: Any, input_path: pathlib.Path) -> pathlib.Path:
    if not isinstance(raw_path, (str, pathlib.Path)) or not str(raw_path).strip():
        raise ValueError("Course entry is missing a non-empty 'path'")
    path = pathlib.Path(str(raw_path))
    candidates = [path] if path.is_absolute() else [
        pathlib.Path.cwd() / path,
        REPOSITORY_ROOT / path,
        input_path.parent / path,
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise ValueError(f"Course file does not exist: {path}")


def _programme_fallback(
    code: str,
    year: Any,
    cache: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    key = (str(year), code)
    if key in cache:
        return cache[key]
    pattern = f"programme-{code}.yml"
    candidates = sorted((REPOSITORY_ROOT / "data" / "programmes" / str(year)).glob(f"*/{pattern}"))
    cache[key] = _load_yaml_mapping(candidates[0], "Programme YAML") if candidates else {}
    return cache[key]


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {}


def _complete_programme(
    programme: Mapping[str, Any],
    course_year: Any,
    cache: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    result = dict(programme)
    code = str(result.get("code") or result.get("id") or "").strip()
    raw_name = result.get("name")
    english_name_missing = (
        not isinstance(raw_name, dict)
        or not str(raw_name.get("en") or "").strip()
    )
    if code and (
        english_name_missing
        or _is_missing(result.get("duration"))
        or _is_missing(result.get("department"))
    ):
        fallback = _programme_fallback(code, result.get("year") or course_year, cache)
        fallback_name = fallback.get("name")
        result = {**fallback, **{key: value for key, value in result.items() if not _is_missing(value)}}
        if isinstance(fallback_name, dict) and isinstance(raw_name, dict):
            result["name"] = {
                **fallback_name,
                **{key: value for key, value in raw_name.items() if not _is_missing(value)},
            }
    if code:
        result["code"] = code
    return result


def _programme_name(programme: Mapping[str, Any]) -> str:
    name = programme.get("name")
    if isinstance(name, dict):
        return str(name.get("en") or "").strip()
    return str(name or "").strip()


def _programme_type(duration: Any) -> str:
    try:
        numeric_duration = Decimal(str(duration).strip())
    except (InvalidOperation, ValueError):
        return ""
    if numeric_duration == 3:
        return "LT"
    if numeric_duration == 2:
        return "LM"
    if numeric_duration > 3:
        return "LMCU"
    return ""


def course_scope(teachers: Sequence[Mapping[str, Any]], programmes: Sequence[Mapping[str, Any]]) -> str:
    teacher_flags = [
        str(teacher.get("affiliation") or "").strip().casefold() == "disi"
        for teacher in teachers
    ]
    programme_flags = [
        str(programme.get("department") or "").strip().casefold() == "disi"
        for programme in programmes
    ]
    any_teacher = any(teacher_flags)
    any_programme = any(programme_flags)
    all_teachers = bool(teacher_flags) and all(teacher_flags)
    all_programmes = bool(programme_flags) and all(programme_flags)
    if all_teachers and all_programmes:
        return "internal"
    if any_teacher and any_programme:
        return "weak_internal"
    if any_teacher:
        return "service"
    if any_programme:
        return "borrow"
    return "external"


def _preferred_course_url(teachers: Sequence[Mapping[str, Any]]) -> str:
    counts_by_language: dict[str, Counter[str]] = {"en": Counter(), "it": Counter()}
    for teacher in teachers:
        modules = teacher.get("modules")
        if not isinstance(modules, list):
            continue
        for module in modules:
            if not isinstance(module, dict):
                continue
            urls = module.get("syllabus_urls")
            if not isinstance(urls, dict):
                continue
            for language in ("en", "it"):
                url = urls.get(language)
                if isinstance(url, str) and url.strip():
                    counts_by_language[language][url.strip()] += 1
    for language in ("en", "it"):
        counts = counts_by_language[language]
        if counts:
            return min(counts, key=lambda url: (-counts[url], url.casefold(), url))
    return ""


def _teachers_ssds(teachers: Sequence[Mapping[str, Any]]) -> list[str]:
    result: list[str] = []
    for teacher in teachers:
        ssd = teacher.get("ssd")
        if isinstance(ssd, dict):
            result.extend(_strings(ssd.get("name")))
        else:
            result.extend(_strings(ssd))
    return result


def _row_for_course(
    cluster_id: int,
    cluster_name: str,
    cluster_course: Mapping[str, Any],
    course: Mapping[str, Any],
    programme_cache: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    raw_teachers = course.get("teachers")
    teachers = [item for item in raw_teachers if isinstance(item, dict)] if isinstance(raw_teachers, list) else []
    course_year = course.get("year") or cluster_course.get("year") or ""
    raw_programmes = course.get("programmes")
    programmes = [
        _complete_programme(item, course_year, programme_cache)
        for item in raw_programmes
        if isinstance(item, dict)
    ] if isinstance(raw_programmes, list) else []
    course_title = course.get("course_title")
    course_title = course_title if isinstance(course_title, dict) else {}
    course_id = str(cluster_course.get("id") or course_title.get("id") or "").strip()
    course_name = str(course_title.get("name") or cluster_course.get("name") or "").strip()
    roles = [
        role
        for teacher in teachers
        for role in _strings(teacher.get("role"))
    ]
    return {
        "cluster_id": cluster_id,
        "cluster_name": cluster_name,
        "course_id": course_id,
        "course_name": course_name,
        "year": course_year,
        "teachers": _join(_strings(cluster_course.get("teachers"))),
        "teachers_affiliations": _join(teacher.get("affiliation") for teacher in teachers),
        "teachers_ssds": _join(_teachers_ssds(teachers)),
        "teachers_roles": _join(roles),
        "campus": _join(_strings(course.get("campi") or cluster_course.get("campi"))),
        "credits": _join_credits(course.get("credits")),
        "ssd": _join(_strings(course.get("ssds"))),
        "language": _join(_strings(course.get("languages"))),
        "programmes_codes": _join(programme.get("code") for programme in programmes),
        "programmes_names": _join(_programme_name(programme) for programme in programmes),
        "programmes_types": _join(_programme_type(programme.get("duration")) for programme in programmes),
        "programmes_affiliations": _join(programme.get("department") for programme in programmes),
        "course_scope": course_scope(teachers, programmes),
        "course_url": _preferred_course_url(teachers),
    }


def build_rows(input_path: pathlib.Path) -> list[dict[str, Any]]:
    payload = _load_yaml_mapping(input_path, "Cluster YAML")
    programme_cache: dict[tuple[str, str], dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    for raw_cluster_name, raw_cluster in payload.items():
        cluster_name = str(raw_cluster_name)
        if not isinstance(raw_cluster, dict):
            raise ValueError(f"Cluster {cluster_name!r} must be a mapping")
        if "index" not in raw_cluster:
            raise ValueError(f"Cluster {cluster_name!r} is missing 'index'")
        try:
            cluster_id = int(raw_cluster["index"])
        except (TypeError, ValueError) as error:
            raise ValueError(f"Cluster {cluster_name!r} has an invalid 'index'") from error
        courses = raw_cluster.get("courses")
        if not isinstance(courses, list):
            raise ValueError(f"Cluster {cluster_name!r} is missing a 'courses' list")
        for index, cluster_course in enumerate(courses):
            if not isinstance(cluster_course, dict):
                raise ValueError(f"Course {index} in cluster {cluster_name!r} must be a mapping")
            course_path = _resolve_course_path(cluster_course.get("path"), input_path)
            course = _load_yaml_mapping(course_path, "Course YAML")
            rows.append(_row_for_course(cluster_id, cluster_name, cluster_course, course, programme_cache))
    return sorted(rows, key=lambda row: (row["cluster_id"], str(row["course_id"]).casefold(), str(row["course_id"])))


def export_cluster_courses(input_path: pathlib.Path, output_path: pathlib.Path | None = None) -> pathlib.Path:
    input_path = input_path.resolve()
    destination = output_path or input_path.with_suffix(".csv")
    rows = build_rows(input_path)
    try:
        with destination.open("w", newline="", encoding="utf-8-sig") as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=FIELDNAMES,
                delimiter=";",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
            )
            writer.writeheader()
            writer.writerows(rows)
    except OSError as error:
        raise ValueError(f"Could not write CSV {destination}: {error}") from error
    return destination


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export clustered courses to an Excel-ready CSV file.")
    parser.add_argument("input", type=pathlib.Path, help="Path to cluster_courses.yml")
    parser.add_argument("-o", "--output", type=pathlib.Path, help="Destination CSV (defaults next to the input)")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        destination = export_cluster_courses(args.input, args.output)
    except ValueError as error:
        parser.error(str(error))
    print(destination)


if __name__ == "__main__":
    main()

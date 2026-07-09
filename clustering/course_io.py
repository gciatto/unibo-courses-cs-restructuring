from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass
from typing import Any

import yaml

from clustering.cache import file_sha256
from clustering.sections import extract_similarity_sections


COURSE_FILENAME_RE = re.compile(r"^course-(?P<course_id>.+)\.ya?ml$")


@dataclass(frozen=True)
class CourseRecord:
    course_id: str
    path: pathlib.Path
    sha256: str
    title: str
    credits: int
    ssd: str
    DISI_teacher: bool | None
    DISI_cds: bool | None
    campus: str
    sections: dict[str, str | None]
    section_languages: dict[str, str | None]
    raw: dict[str, Any]

def _extract_first_credits(payload: dict[str, Any]) -> int:
    credits = payload.get("credits")
    if isinstance(credits, list) and credits:
        try:
            return int(credits[0])
        except (TypeError, ValueError):
            return 0
    return 0


def _extract_first_ssd(payload: dict[str, Any]) -> str:
    ssds = payload.get("ssds")
    if isinstance(ssds, list) and ssds:
        return str(ssds[0])
    return ""


def _extract_first_campus(payload: dict[str, Any]) -> str:
    campi = payload.get("campi")
    if isinstance(campi, list) and campi:
        return str(campi[0])
    return ""


def _teacher_roles(teacher: dict[str, Any]) -> set[str]:
    role = teacher.get("role")
    roles = role if isinstance(role, list) else ([role] if role else [])
    return {str(item).strip().lower() for item in roles if item}


def _has_disi_teacher(payload: dict[str, Any]) -> bool:
    teachers = payload.get("teachers")
    if not isinstance(teachers, list):
        return False

    for teacher in teachers:
        if not isinstance(teacher, dict):
            continue
        if "teaching tutor" in _teacher_roles(teacher):
            continue
        affiliation = teacher.get("affiliation")
        if isinstance(affiliation, str) and affiliation.strip().lower() == "disi":
            return True
    return False


def _has_disi_cds(payload: dict[str, Any]) -> bool:
    programmes = payload.get("programmes")
    if not isinstance(programmes, list):
        return False

    for programme in programmes:
        if not isinstance(programme, dict):
            continue
        department = programme.get("department")
        if isinstance(department, str) and department.strip().lower() == "disi":
            return True
    return False

def parse_course_id_from_filename(path_or_name: str | pathlib.Path) -> str:
    name = pathlib.Path(path_or_name).name
    match = COURSE_FILENAME_RE.fullmatch(name)
    if match is None:
        raise ValueError(f"Expected filename like course-00819-B.yml, got {name!r}")
    return match.group("course_id")


def discover_course_paths(courses_dir: pathlib.Path, year: int) -> list[pathlib.Path]:
    year_dir = courses_dir / str(year)
    if not year_dir.exists():
        raise FileNotFoundError(f"Course year directory does not exist: {year_dir}")
    return sorted(year_dir.glob("course-*.yml"))


def load_course(path: pathlib.Path) -> CourseRecord:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Course YAML is not a mapping: {path}")

    # The filename is the canonical identifier for clustering instances.
    course_id = parse_course_id_from_filename(path)

    sections, section_languages = extract_similarity_sections(payload)
    title = sections.get("course title.name") or ""

    return CourseRecord(
        course_id=course_id,
        path=path,
        sha256=file_sha256(path),
        title=title,
        credits=_extract_first_credits(payload),
        ssd=_extract_first_ssd(payload),
        DISI_teacher=_has_disi_teacher(payload),
        DISI_cds=_has_disi_cds(payload),
        campus=_extract_first_campus(payload),
        sections=sections,
        section_languages=section_languages,
        raw=payload,
    )


def load_courses(courses_dir: pathlib.Path, year: int, limit: int | None = None) -> list[CourseRecord]:
    paths = discover_course_paths(courses_dir, year)
    if limit is not None:
        if limit < 0:
            raise ValueError("--limit must be >= 0")
        paths = paths[:limit]
    return [load_course(path) for path in paths]

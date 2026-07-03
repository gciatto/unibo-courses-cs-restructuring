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
    sections: dict[str, str | None]
    section_languages: dict[str, str | None]
    raw: dict[str, Any]


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

import argparse
import logging
import os
import pathlib
import re
import shlex
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable

import yaml
from pydantic import BaseModel, Field

from _utils import configure_logging
from download_teachings import (
    CourseMetadata,
    CourseSchedule,
    DEFAULT_OUTPUT,
    SyllabusPage,
    Teacher,
    TeacherSsd,
)


DEFAULT_INPUT_DIR = DEFAULT_OUTPUT
DEFAULT_MERGED_DIRNAME = ".files"
PATTERN_TEACHING_FILENAME = re.compile(r"^teaching-(?P<teaching_id>[^/]+)\.yml$")

LOGGER = logging.getLogger(pathlib.Path(__file__).stem)


class TeachingModule(BaseModel):
    teaching_id: str = Field(default="")
    url: str = Field(default="")
    syllabus_urls: dict[str, str] = Field(default_factory=dict)
    details: list[str] = Field(default_factory=list)


class TeacherWithModule(Teacher):
    module: TeachingModule = Field(default_factory=TeachingModule)


class MergedCourseTitle(BaseModel):
    id: str = Field(default="")
    name: str = Field(default="")


class MergedSyllabusPage(BaseModel):
    title: str = Field(default="")
    contents: dict[str, str] = Field(default_factory=dict)


class MergedCourseMetadata(BaseModel):
    year: int
    credits: int | None = None
    ssd: str = Field(default="")
    language: str = Field(default="")
    teaching_mode: str = Field(default="")
    schedule: CourseSchedule | None = None
    teachers: list[TeacherWithModule] = Field(default_factory=list)
    course_title: MergedCourseTitle
    integrated_course: str = Field(default="")
    campus: str = Field(default="")
    programme: str = Field(default="")
    syllabus: dict[str, MergedSyllabusPage] = Field(default_factory=dict)


TeacherWithModule.model_rebuild(_types_namespace={"TeacherSsd": TeacherSsd})


@dataclass(frozen=True)
class TeachingRecord:
    path: pathlib.Path
    teacher_dir: pathlib.Path
    year_dir: pathlib.Path
    metadata: CourseMetadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge teaching YAML files by course_title.id.")
    parser.add_argument(
        "--courses-dir",
        "-i",
        type=pathlib.Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Courses directory containing TEACHER/YEAR/teaching-*.yml (default: {DEFAULT_INPUT_DIR}).",
    )
    return parser.parse_args()


def normalize_teaching_payload(raw_data: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(raw_data)

    teacher_data = normalized.get("teacher")
    if isinstance(teacher_data, dict):
        normalized["teacher"] = {
            "teacher_id": teacher_data.get("id", ""),
            "teacher_name": teacher_data.get("name", ""),
            "teacher_email": teacher_data.get("email", ""),
            "teacher_website": teacher_data.get("website", ""),
            "teacher_role": teacher_data.get("role", ""),
            "teacher_affiliation": teacher_data.get("affiliation", ""),
            "teacher_ssd": teacher_data.get("ssd"),
        }

    schedule_data = normalized.get("schedule")
    if isinstance(schedule_data, dict):
        normalized["schedule"] = {
            "schedule_from": schedule_data.get("from"),
            "schedule_to": schedule_data.get("to"),
        }

    return normalized


def load_teaching_metadata(path: pathlib.Path) -> CourseMetadata:
    raw_data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return CourseMetadata.model_validate(normalize_teaching_payload(raw_data))


def iter_teaching_records(courses_dir: pathlib.Path) -> list[TeachingRecord]:
    records: list[TeachingRecord] = []

    for path in sorted(courses_dir.rglob("teaching-*.yml")):
        relative_path = path.relative_to(courses_dir)
        if len(relative_path.parts) != 3:
            LOGGER.warning("Skipping unexpected teaching path layout: %s", path)
            continue

        teacher_dir = path.parent.parent
        year_dir = path.parent

        if PATTERN_TEACHING_FILENAME.fullmatch(path.name) is None:
            LOGGER.warning("Skipping file with unexpected teaching name: %s", path)
            continue

        metadata = load_teaching_metadata(path)
        if not metadata.course_title.id:
            LOGGER.warning("Skipping %s because course_title.id is empty", path)
            continue

        if year_dir.name != str(metadata.year):
            LOGGER.warning(
                "Year mismatch for %s: directory=%s yaml=%s",
                path,
                year_dir.name,
                metadata.year,
            )

        records.append(
            TeachingRecord(
                path=path,
                teacher_dir=teacher_dir,
                year_dir=year_dir,
                metadata=metadata,
            ),
        )

    return records


def has_value(value: Any) -> bool:
    return value not in (None, "", [], {})


def to_plain_data(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(by_alias=True, exclude_none=True)
    if isinstance(value, dict):
        return {key: to_plain_data(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_plain_data(item) for item in value]
    if isinstance(value, tuple):
        return [to_plain_data(item) for item in value]
    return value


def normalize_for_comparison(value: Any) -> str:
    return yaml.safe_dump(to_plain_data(value), sort_keys=True, allow_unicode=True).strip()


def merge_value(
    records: list[TeachingRecord],
    label: str,
    getter: Callable[[CourseMetadata], Any],
) -> Any:
    selected_value: Any = None
    selected_path: pathlib.Path | None = None
    selected_normalized: str | None = None

    for record in records:
        value = getter(record.metadata)
        if not has_value(value):
            continue

        normalized = normalize_for_comparison(value)
        if selected_path is None:
            selected_value = value
            selected_path = record.path
            selected_normalized = normalized
            continue

        if normalized != selected_normalized:
            LOGGER.warning(
                "Conflicting %s for course %s in year %s; keeping value from %s and ignoring %s",
                label,
                record.metadata.course_title.id,
                record.metadata.year,
                selected_path,
                record.path,
            )

    return selected_value


def build_teacher_entry(record: TeachingRecord) -> TeacherWithModule:
    teacher_payload = record.metadata.teacher.model_dump(by_alias=False, exclude_none=True)
    teaching_id = PATTERN_TEACHING_FILENAME.fullmatch(record.path.name).group("teaching_id")  # type: ignore[union-attr]
    teacher_payload["module"] = {
        "teaching_id": teaching_id,
        "url": record.metadata.url,
        "syllabus_urls": {
            lang: page.url
            for lang, page in record.metadata.syllabus.items()
            if page.url
        },
        "details": list(record.metadata.course_title.details),
    }
    return TeacherWithModule.model_validate(teacher_payload)


def to_merged_syllabus(
    syllabus: dict[str, SyllabusPage] | None,
) -> dict[str, MergedSyllabusPage]:
    if not syllabus:
        return {}
    return {
        lang: MergedSyllabusPage(title=page.title, contents=page.contents)
        for lang, page in syllabus.items()
    }


def merge_records(records: list[TeachingRecord]) -> MergedCourseMetadata:
    if not records:
        raise ValueError("Cannot merge an empty set of teaching records.")

    first_metadata = records[0].metadata
    teachers = [build_teacher_entry(record) for record in records]
    teachers.sort(key=lambda teacher: (teacher.teacher_name, teacher.teacher_email, teacher.module.teaching_id))

    raw_syllabus: dict[str, SyllabusPage] | None = merge_value(
        records, "syllabus", lambda metadata: metadata.syllabus
    )

    return MergedCourseMetadata(
        year=first_metadata.year,
        credits=merge_value(records, "credits", lambda metadata: metadata.credits),
        ssd=merge_value(records, "ssd", lambda metadata: metadata.ssd) or "",
        language=merge_value(records, "language", lambda metadata: metadata.language) or "",
        teaching_mode=merge_value(records, "teaching_mode", lambda metadata: metadata.teaching_mode) or "",
        schedule=merge_value(records, "schedule", lambda metadata: metadata.schedule),
        teachers=teachers,
        course_title=MergedCourseTitle(
            id=first_metadata.course_title.id,
            name=merge_value(records, "course_title.name", lambda metadata: metadata.course_title.name)
            or first_metadata.course_title.name,
        ),
        integrated_course=merge_value(records, "integrated_course", lambda metadata: metadata.integrated_course) or "",
        campus=merge_value(records, "campus", lambda metadata: metadata.campus) or "",
        programme=merge_value(records, "programme", lambda metadata: metadata.programme) or "",
        syllabus=to_merged_syllabus(raw_syllabus),
    )


def ensure_symlink(link_path: pathlib.Path, target_path: pathlib.Path) -> None:
    relative_target = pathlib.Path(os.path.relpath(target_path, start=link_path.parent))

    if link_path.is_symlink():
        if pathlib.Path(os.readlink(link_path)) == relative_target:
            return
        link_path.unlink()
    elif link_path.exists():
        if link_path.is_dir():
            raise IsADirectoryError(f"Cannot replace directory with symlink: {link_path}")
        link_path.unlink()

    link_path.symlink_to(relative_target)


def merge_courses_tree(courses_dir: pathlib.Path) -> tuple[int, int]:
    records = iter_teaching_records(courses_dir)
    grouped_records: dict[tuple[int, str], list[TeachingRecord]] = defaultdict(list)

    for record in records:
        grouped_records[(record.metadata.year, record.metadata.course_title.id)].append(record)

    merged_count = 0
    symlink_count = 0

    for (year, course_id), course_records in sorted(grouped_records.items()):
        merged_dir = courses_dir / DEFAULT_MERGED_DIRNAME / str(year)
        merged_dir.mkdir(parents=True, exist_ok=True)

        merged_path = merged_dir / f"course-{course_id}.yml"
        merged_metadata = merge_records(course_records)
        merged_path.write_text(
            yaml.safe_dump(
                merged_metadata.model_dump(by_alias=True, exclude_none=True),
                sort_keys=False,
                allow_unicode=True,
            ),
            encoding="utf-8",
        )
        merged_count += 1

        for record in course_records:
            symlink_path = record.year_dir / f"course-{course_id}.yml"
            ensure_symlink(symlink_path, merged_path)
            symlink_count += 1

        LOGGER.info("Wrote %s from %s teaching file(s)", merged_path, len(course_records))

    return merged_count, symlink_count


def main() -> int:
    configure_logging()
    LOGGER.info("Command line: %s", shlex.join(sys.argv))
    args = parse_args()

    if not args.courses_dir.exists():
        LOGGER.error("courses directory does not exist: %s", args.courses_dir)
        return 2

    merged_count, symlink_count = merge_courses_tree(args.courses_dir)
    LOGGER.info("Done. merged=%s symlinks=%s", merged_count, symlink_count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

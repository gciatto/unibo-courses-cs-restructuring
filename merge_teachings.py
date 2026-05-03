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
    credits: int | None = None
    schedule: CourseSchedule | None = None
    campus: str = Field(default="")
    programme: str = Field(default="")
    ssd: str = Field(default="")
    language: str = Field(default="")
    teaching_mode: str = Field(default="")


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
    credits: list[int | None] = Field(default_factory=list)
    ssds: list[str] = Field(default_factory=list)
    languages: list[str] = Field(default_factory=list)
    teaching_modes: list[str] = Field(default_factory=list)
    schedules: list[CourseSchedule | None] = Field(default_factory=list)
    teachers: list[TeacherWithModule] = Field(default_factory=list)
    course_title: MergedCourseTitle
    integrated_course: str = Field(default="")
    campi: list[str] = Field(default_factory=list)
    programmes: list[str] = Field(default_factory=list)
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
        "credits": record.metadata.credits,
        "schedule": record.metadata.schedule,
        "campus": record.metadata.campus,
        "programme": record.metadata.programme,
        "ssd": record.metadata.ssd,
        "language": record.metadata.language,
        "teaching_mode": record.metadata.teaching_mode,
    }
    return TeacherWithModule.model_validate(teacher_payload)


def merge_syllabus(records: list[TeachingRecord]) -> dict[str, MergedSyllabusPage]:
    merged: dict[str, MergedSyllabusPage] = {}
    selected_paths: dict[str, pathlib.Path] = {}

    for record in records:
        syllabus = record.metadata.syllabus
        if not has_value(syllabus):
            continue
        for lang, page in syllabus.items():
            candidate = MergedSyllabusPage(title=page.title, contents=page.contents)
            if lang not in merged:
                merged[lang] = candidate
                selected_paths[lang] = record.path
            else:
                existing_norm = normalize_for_comparison(merged[lang])
                candidate_norm = normalize_for_comparison(candidate)
                if existing_norm != candidate_norm:
                    course_id = record.metadata.course_title.id
                    year = record.metadata.year
                    LOGGER.warning(
                        "Conflicting syllabus[%s] for course %s in year %s; keeping value from %s and ignoring %s",
                        lang,
                        course_id,
                        year,
                        selected_paths[lang],
                        record.path,
                    )

    return merged


def get_syllabus_signature(record: TeachingRecord) -> str:
    """Get a normalized signature of syllabi to group records with identical syllabi."""
    syllabus_dict = {}
    for lang, page in record.metadata.syllabus.items():
        syllabus_dict[lang] = {
            "title": page.title,
            "contents": page.contents,
        }
    return normalize_for_comparison(syllabus_dict)


def generate_course_suffixes(
    course_groups: dict[tuple[int, str, str], list[TeachingRecord]],
) -> dict[tuple[int, str, str], str]:
    """Generate suffixes for course files with multiple syllabi using deterministic letter assignment."""
    suffixes: dict[tuple[int, str, str], str] = {}

    # Group by (year, course_id) to find conflicts
    by_course_id: dict[tuple[int, str], list[tuple[int, str, str]]] = defaultdict(list)
    for key in course_groups.keys():
        year, course_id, syllabus_sig = key
        by_course_id[(year, course_id)].append(key)

    for (year, course_id), keys in by_course_id.items():
        if len(keys) == 1:
            # No conflict, no suffix needed
            suffixes[keys[0]] = ""
        else:
            # Multiple syllabi: assign letters A, B, C, etc. sorted deterministically by syllabus signature
            LOGGER.warning(
                "Course %s in year %s has %d different syllabi; using letter suffixes",
                course_id,
                year,
                len(keys),
            )
            # Sort by syllabus signature to ensure deterministic assignment
            sorted_keys = sorted(keys, key=lambda k: k[2])  # Sort by syllabus_sig (index 2)
            for i, key in enumerate(sorted_keys):
                suffixes[key] = f"-{chr(ord('A') + i)}"

    return suffixes


def merge_records(records: list[TeachingRecord]) -> MergedCourseMetadata:
    if not records:
        raise ValueError("Cannot merge an empty set of teaching records.")

    first_metadata = records[0].metadata
    teachers = [build_teacher_entry(record) for record in records]
    teachers.sort(key=lambda teacher: (teacher.teacher_name, teacher.teacher_email, teacher.module.teaching_id))

    # Collect all unique values for list fields
    credits_set: dict[int | None, bool] = {}
    ssds_set: dict[str, bool] = {}
    languages_set: dict[str, bool] = {}
    teaching_modes_set: dict[str, bool] = {}
    schedules_set: dict[str, CourseSchedule | None] = {}
    campi_set: dict[str, bool] = {}
    programmes_set: dict[str, bool] = {}

    for record in records:
        if record.metadata.credits is not None:
            credits_set[record.metadata.credits] = True
        if record.metadata.ssd:
            ssds_set[record.metadata.ssd] = True
        if record.metadata.language:
            languages_set[record.metadata.language] = True
        if record.metadata.teaching_mode:
            teaching_modes_set[record.metadata.teaching_mode] = True
        if record.metadata.schedule is not None:
            # Use normalized representation as key to deduplicate
            sched_key = normalize_for_comparison(record.metadata.schedule)
            schedules_set[sched_key] = record.metadata.schedule
        if record.metadata.campus:
            campi_set[record.metadata.campus] = True
        if record.metadata.programme:
            programmes_set[record.metadata.programme] = True

    return MergedCourseMetadata(
        year=first_metadata.year,
        credits=[c for c in credits_set.keys()],
        ssds=[s for s in ssds_set.keys()],
        languages=[l for l in languages_set.keys()],
        teaching_modes=[tm for tm in teaching_modes_set.keys()],
        schedules=[s for s in schedules_set.values()],
        teachers=teachers,
        course_title=MergedCourseTitle(
            id=first_metadata.course_title.id,
            name=merge_value(records, "course_title.name", lambda metadata: metadata.course_title.name)
            or first_metadata.course_title.name,
        ),
        integrated_course=merge_value(records, "integrated_course", lambda metadata: metadata.integrated_course) or "",
        campi=[c for c in campi_set.keys()],
        programmes=[p for p in programmes_set.keys()],
        syllabus=merge_syllabus(records),
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
    grouped_records: dict[tuple[int, str, str], list[TeachingRecord]] = defaultdict(list)

    # Group by (year, course_id, syllabus_signature)
    for record in records:
        syllabus_sig = get_syllabus_signature(record)
        grouped_records[(record.metadata.year, record.metadata.course_title.id, syllabus_sig)].append(record)

    # Generate suffixes for courses with conflicting syllabi
    suffixes = generate_course_suffixes(grouped_records)

    merged_count = 0
    symlink_count = 0

    for (year, course_id, syllabus_sig), course_records in sorted(grouped_records.items()):
        merged_dir = courses_dir / DEFAULT_MERGED_DIRNAME / str(year)
        merged_dir.mkdir(parents=True, exist_ok=True)

        suffix = suffixes.get((year, course_id, syllabus_sig), "")
        merged_path = merged_dir / f"course-{course_id}{suffix}.yml"
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
            symlink_path = record.year_dir / f"course-{course_id}{suffix}.yml"
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

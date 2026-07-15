import argparse
from difflib import SequenceMatcher
import logging
import os
import pathlib
import re
import shlex
import sys
from collections import defaultdict
from dataclasses import dataclass
from functools import cache
from typing import Any, Callable

import numpy as np

import yaml
from pydantic import BaseModel, Field

from clustering.sections import normalize_label, normalize_text
from scraping._utils import configure_logging
from scraping.download_teachings import (
    CourseMetadata,
    CourseSchedule,
    DEFAULT_OUTPUT,
    Teacher,
    TeacherSsd,
)


DEFAULT_INPUT_DIR = DEFAULT_OUTPUT
DEFAULT_MERGED_DIRNAME = ".files"
PATTERN_TEACHING_FILENAME = re.compile(r"^teaching-(?P<teaching_id>[^/]+)\.yml$")
DEFAULT_LEARNING_OUTCOMES_SIMILARITY_BACKEND = "embedding"
DEFAULT_LEARNING_OUTCOMES_SIMILARITY_THRESHOLD = 95.0
DEFAULT_LEARNING_OUTCOMES_EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"

LEARNING_OUTCOMES_LABELS = {
    normalize_label("Learning outcomes"),
    normalize_label("Conoscenze e abilita da conseguire"),
    normalize_label("Conoscenze e abilità da conseguire"),
}

LOGGER = logging.getLogger(pathlib.Path(__file__).stem)


class TeachingModule(BaseModel):
    teaching_id: str = Field(default="")
    url: str = Field(default="")
    syllabus_urls: dict[str, str] = Field(default_factory=dict)
    details: list[str] = Field(default_factory=list)
    credits: int | None = None
    schedule: CourseSchedule | None = None
    campus: str = Field(default="")
    ssd: str = Field(default="")
    language: str = Field(default="")
    teaching_mode: str = Field(default="")


class TeacherWithModules(Teacher):
    modules: list[TeachingModule] = Field(default_factory=list)


class MergedCourseTitle(BaseModel):
    id: str = Field(default="")
    name: str = Field(default="")


class MergedSyllabusPage(BaseModel):
    title: str = Field(default="")
    contents: dict[str, Any] = Field(default_factory=dict)


class MergedCourseMetadata(BaseModel):
    year: int
    credits: list[int | None] = Field(default_factory=list)
    ssds: list[str] = Field(default_factory=list)
    languages: list[str] = Field(default_factory=list)
    teaching_modes: list[str] = Field(default_factory=list)
    schedules: list[CourseSchedule | None] = Field(default_factory=list)
    teachers: list[TeacherWithModules] = Field(default_factory=list)
    course_title: MergedCourseTitle
    integrated_course: str = Field(default="")
    campi: list[str] = Field(default_factory=list)
    programmes: list[dict[str, Any]] = Field(default_factory=list)
    syllabus: dict[str, MergedSyllabusPage] = Field(default_factory=dict)


TeacherWithModules.model_rebuild(_types_namespace={"TeacherSsd": TeacherSsd})


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
    parser.add_argument(
        "--learning-outcomes-similarity-backend",
        choices=["text", "token", "embedding"],
        default=DEFAULT_LEARNING_OUTCOMES_SIMILARITY_BACKEND,
        help="Backend used to compare learning outcomes when exact syllabus matches are not available.",
    )
    parser.add_argument(
        "--learning-outcomes-similarity-threshold",
        type=float,
        default=DEFAULT_LEARNING_OUTCOMES_SIMILARITY_THRESHOLD,
        help="Minimum learning-outcomes similarity percentage required for merging.",
    )
    parser.add_argument(
        "--learning-outcomes-embedding-model",
        default=DEFAULT_LEARNING_OUTCOMES_EMBEDDING_MODEL,
        help="Sentence-Transformers model used for learning-outcomes similarity when the embedding backend is selected.",
    )
    return parser.parse_args()


def teaching_id_from_path(path: pathlib.Path) -> str:
    match = PATTERN_TEACHING_FILENAME.fullmatch(path.name)
    if match is None:
        raise ValueError(f"Unexpected teaching filename: {path}")
    return match.group("teaching_id")


def is_learning_outcomes_label(label: str) -> bool:
    return normalize_label(label) in LEARNING_OUTCOMES_LABELS


def strip_learning_outcomes(contents: dict[str, Any]) -> dict[str, Any]:
    return {
        label: value
        for label, value in contents.items()
        if not is_learning_outcomes_label(str(label))
    }


def extract_learning_outcomes(contents: dict[str, Any]) -> str:
    for label, value in contents.items():
        if is_learning_outcomes_label(str(label)):
            text = normalize_text(value)
            if text:
                return text
    return ""


def syllabus_core_signature(syllabus: dict[str, Any]) -> str:
    payload: dict[str, Any] = {}
    for lang in sorted(syllabus):
        page = syllabus_page_to_dict(syllabus[lang])
        contents = page.get("contents", {})
        payload[lang] = {
            "title": normalize_text(page.get("title")),
            "contents": strip_learning_outcomes(contents) if isinstance(contents, dict) else {},
        }
    return normalize_for_comparison(payload)


def page_signature_without_learning_outcomes(page: Any) -> str:
    page_data = syllabus_page_to_dict(page)
    contents = page_data.get("contents", {})
    payload = {
        "title": normalize_text(page_data.get("title")),
        "contents": strip_learning_outcomes(contents) if isinstance(contents, dict) else {},
    }
    return normalize_for_comparison(payload)


def text_similarity_percent(text_a: str, text_b: str) -> float:
    return SequenceMatcher(None, text_a, text_b).ratio() * 100.0


def token_similarity_percent(text_a: str, text_b: str) -> float:
    tokens_a = set(re.findall(r"\w+", text_a.lower()))
    tokens_b = set(re.findall(r"\w+", text_b.lower()))
    if not tokens_a and not tokens_b:
        return 100.0
    union = tokens_a | tokens_b
    if not union:
        return 0.0
    return (len(tokens_a & tokens_b) / len(union)) * 100.0


@cache
def load_embedding_model(model_name: str):
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(model_name)


def embedding_similarity_percent(text_a: str, text_b: str, model_name: str) -> float:
    model = load_embedding_model(model_name)
    vectors = model.encode([text_a, text_b], normalize_embeddings=True)
    similarity = float(np.dot(vectors[0], vectors[1]))
    return max(0.0, similarity) * 100.0


def learning_outcomes_similarity_percent(
    text_a: str,
    text_b: str,
    backend: str,
    embedding_model: str,
) -> float:
    normalized_a = normalize_text(text_a)
    normalized_b = normalize_text(text_b)
    if not normalized_a and not normalized_b:
        return 100.0
    if backend == "text":
        return text_similarity_percent(normalized_a, normalized_b)
    if backend == "token":
        return token_similarity_percent(normalized_a, normalized_b)
    if backend == "embedding":
        return embedding_similarity_percent(normalized_a, normalized_b, embedding_model)
    raise ValueError(f"Unsupported learning outcomes similarity backend: {backend}")


def course_programme_code_set(metadata: CourseMetadata) -> frozenset[str]:
    return frozenset(
        str(programme.get("code") or "")
        for programme in metadata.programmes
        if str(programme.get("code") or "").strip()
    )


def merge_suffix(index: int) -> str:
    suffix = ""
    value = index
    while True:
        value, remainder = divmod(value, 26)
        suffix = chr(ord("A") + remainder) + suffix
        if value == 0:
            break
        value -= 1
    return f"-{suffix}"


def syllabus_page_to_dict(page: Any) -> dict[str, Any]:
    if isinstance(page, BaseModel):
        return page.model_dump(by_alias=True, exclude_none=True)
    if isinstance(page, dict):
        return page
    return {}


def normalize_teaching_payload(raw_data: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(raw_data)

    teacher_data = normalized.get("teacher")
    if isinstance(teacher_data, dict):
        raw_role = teacher_data.get("role", [])
        if isinstance(raw_role, str):
            normalized_role = [raw_role] if raw_role else []
        elif isinstance(raw_role, (list, tuple, set)):
            normalized_role = [str(item) for item in raw_role if str(item)]
        else:
            normalized_role = []

        normalized["teacher"] = {
            "teacher_id": teacher_data.get("id", ""),
            "teacher_name": teacher_data.get("name", ""),
            "teacher_email": teacher_data.get("email", ""),
            "teacher_website": teacher_data.get("website", ""),
            "teacher_role": normalized_role,
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


def build_teacher_entry(record: TeachingRecord) -> tuple[str, dict[str, Any], TeachingModule]:
    teacher_payload = record.metadata.teacher.model_dump(by_alias=False, exclude_none=True)
    teaching_id = teaching_id_from_path(record.path)
    module_payload = {
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
        "ssd": record.metadata.ssd,
        "language": record.metadata.language,
        "teaching_mode": record.metadata.teaching_mode,
    }
    module = TeachingModule.model_validate(module_payload)
    teacher_email = teacher_payload.get("teacher_email", "") or ""
    return teacher_email, teacher_payload, module


def merge_programmes_by_code(programmes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_code: dict[str, dict[str, Any]] = {}
    without_code: list[dict[str, Any]] = []
    seen_without_code: set[str] = set()

    for programme in programmes:
        if not isinstance(programme, dict):
            continue
        code = str(programme.get("code") or "").strip()
        if code:
            by_code.setdefault(code, programme)
            continue

        signature = normalize_for_comparison(programme)
        if signature in seen_without_code:
            continue
        seen_without_code.add(signature)
        without_code.append(programme)

    merged = [by_code[key] for key in sorted(by_code)]
    merged.extend(without_code)
    return merged


def merge_syllabus(records: list[TeachingRecord]) -> dict[str, MergedSyllabusPage]:
    merged: dict[str, MergedSyllabusPage] = {}
    selected_paths: dict[str, pathlib.Path] = {}
    learning_outcomes_labels: dict[str, str] = {}
    learning_outcomes_values: dict[str, list[str]] = defaultdict(list)
    learning_outcomes_seen: dict[str, set[str]] = defaultdict(set)

    for record in records:
        syllabus = record.metadata.syllabus
        if not has_value(syllabus):
            continue
        for lang, page in syllabus.items():
            page_data = syllabus_page_to_dict(page)
            page_contents = page_data.get("contents", {}) if isinstance(page_data, dict) else {}
            candidate = MergedSyllabusPage(
                title=normalize_text(page_data.get("title")),
                contents=strip_learning_outcomes(page_contents) if isinstance(page_contents, dict) else {},
            )
            if lang not in merged:
                merged[lang] = candidate
                selected_paths[lang] = record.path
                for label, value in (page_contents or {}).items():
                    if is_learning_outcomes_label(str(label)):
                        learning_outcomes_labels[lang] = str(label)
                        text = normalize_text(value)
                        if text and text not in learning_outcomes_seen[lang]:
                            learning_outcomes_seen[lang].add(text)
                            learning_outcomes_values[lang].append(text)
                        break
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
                if lang not in learning_outcomes_labels:
                    for label in page_contents:
                        if is_learning_outcomes_label(str(label)):
                            learning_outcomes_labels[lang] = str(label)
                            break
                lo_text = extract_learning_outcomes(page_contents)
                if lo_text and lo_text not in learning_outcomes_seen[lang]:
                    learning_outcomes_seen[lang].add(lo_text)
                    learning_outcomes_values[lang].append(lo_text)

    for lang, page in merged.items():
        label = learning_outcomes_labels.get(lang)
        values = learning_outcomes_values.get(lang, [])
        if label and values:
            contents = dict(page.contents)
            contents[label] = values[0] if len(values) == 1 else values
            merged[lang] = MergedSyllabusPage(title=page.title, contents=contents)

    return merged


def records_share_teaching_id(record_a: TeachingRecord, record_b: TeachingRecord) -> bool:
    return record_a.metadata.year == record_b.metadata.year and teaching_id_from_path(record_a.path) == teaching_id_from_path(record_b.path)


def records_share_programme_codes(record_a: TeachingRecord, record_b: TeachingRecord) -> bool:
    return course_programme_code_set(record_a.metadata) == course_programme_code_set(record_b.metadata)


def records_share_syllabus_without_learning_outcomes(record_a: TeachingRecord, record_b: TeachingRecord) -> bool:
    return syllabus_core_signature(record_a.metadata.syllabus) == syllabus_core_signature(record_b.metadata.syllabus)


def records_share_learning_outcomes(
    record_a: TeachingRecord,
    record_b: TeachingRecord,
    backend: str,
    threshold: float,
    embedding_model: str,
) -> bool:
    shared_languages = sorted(set(record_a.metadata.syllabus) & set(record_b.metadata.syllabus))
    comparisons = 0
    for lang in shared_languages:
        page_a = syllabus_page_to_dict(record_a.metadata.syllabus[lang])
        page_b = syllabus_page_to_dict(record_b.metadata.syllabus[lang])
        if not page_a or not page_b:
            continue
        lo_a = extract_learning_outcomes(page_a.get("contents", {}) if isinstance(page_a, dict) else {})
        lo_b = extract_learning_outcomes(page_b.get("contents", {}) if isinstance(page_b, dict) else {})
        if not lo_a and not lo_b:
            continue
        comparisons += 1
        similarity = learning_outcomes_similarity_percent(lo_a, lo_b, backend, embedding_model)
        if similarity < threshold:
            return False
    return comparisons > 0


def records_should_merge(
    record_a: TeachingRecord,
    record_b: TeachingRecord,
    learning_outcomes_similarity_backend: str,
    learning_outcomes_similarity_threshold: float,
    learning_outcomes_embedding_model: str,
) -> bool:
    if record_a.metadata.course_title.id != record_b.metadata.course_title.id:
        return False
    if records_share_teaching_id(record_a, record_b):
        return True
    if records_share_programme_codes(record_a, record_b):
        return True
    if records_share_syllabus_without_learning_outcomes(record_a, record_b):
        return True
    return records_share_learning_outcomes(
        record_a,
        record_b,
        learning_outcomes_similarity_backend,
        learning_outcomes_similarity_threshold,
        learning_outcomes_embedding_model,
    )


def group_records(
    records: list[TeachingRecord],
    learning_outcomes_similarity_backend: str,
    learning_outcomes_similarity_threshold: float,
    learning_outcomes_embedding_model: str,
) -> list[list[TeachingRecord]]:
    if not records:
        return []

    parents = list(range(len(records)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(index_a: int, index_b: int) -> None:
        root_a = find(index_a)
        root_b = find(index_b)
        if root_a != root_b:
            parents[root_b] = root_a

    for index_a in range(len(records)):
        for index_b in range(index_a + 1, len(records)):
            if records_should_merge(
                records[index_a],
                records[index_b],
                learning_outcomes_similarity_backend,
                learning_outcomes_similarity_threshold,
                learning_outcomes_embedding_model,
            ):
                union(index_a, index_b)

    grouped: dict[int, list[TeachingRecord]] = defaultdict(list)
    for index, record in enumerate(records):
        grouped[find(index)].append(record)
    return [grouped[root] for root in sorted(grouped)]


def merge_records(records: list[TeachingRecord]) -> MergedCourseMetadata:
    if not records:
        raise ValueError("Cannot merge an empty set of teaching records.")

    first_metadata = records[0].metadata
    teachers_by_email: dict[str, dict[str, Any]] = {}
    modules_by_email: dict[str, list[TeachingModule]] = defaultdict(list)

    # Collect all unique values for list fields
    credits_set: dict[int | None, bool] = {}
    ssds_set: dict[str, bool] = {}
    languages_set: dict[str, bool] = {}
    teaching_modes_set: dict[str, bool] = {}
    schedules_set: dict[str, CourseSchedule | None] = {}
    campi_set: dict[str, bool] = {}
    merged_programmes_source: list[dict[str, Any]] = []

    for record in records:
        teacher_email, teacher_payload, module = build_teacher_entry(record)
        key_email = teacher_email.strip().lower()
        teacher_key = key_email if key_email else f"__record__:{record.path}"
        if teacher_key not in teachers_by_email:
            teachers_by_email[teacher_key] = teacher_payload
        modules_by_email[teacher_key].append(module)

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
        merged_programmes_source.extend(record.metadata.programmes)

    teachers: list[TeacherWithModules] = []
    for teacher_key, teacher_payload in teachers_by_email.items():
        modules = sorted(modules_by_email[teacher_key], key=lambda item: item.teaching_id)
        payload = dict(teacher_payload)
        payload["modules"] = [module.model_dump(by_alias=False, exclude_none=True) for module in modules]
        teachers.append(TeacherWithModules.model_validate(payload))
    teachers.sort(key=lambda teacher: (teacher.teacher_name, teacher.teacher_email))

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
        programmes=merge_programmes_by_code(merged_programmes_source),
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


def component_signature(records: list[TeachingRecord]) -> str:
    return normalize_for_comparison([record.path.as_posix() for record in records])


def merge_courses_tree(
    courses_dir: pathlib.Path,
    learning_outcomes_similarity_backend: str = DEFAULT_LEARNING_OUTCOMES_SIMILARITY_BACKEND,
    learning_outcomes_similarity_threshold: float = DEFAULT_LEARNING_OUTCOMES_SIMILARITY_THRESHOLD,
    learning_outcomes_embedding_model: str = DEFAULT_LEARNING_OUTCOMES_EMBEDDING_MODEL,
) -> tuple[int, int]:
    records = iter_teaching_records(courses_dir)
    grouped_records: dict[tuple[int, str], list[TeachingRecord]] = defaultdict(list)

    # Group by (year, course_id) first; different course ids are never merged.
    for record in records:
        grouped_records[(record.metadata.year, record.metadata.course_title.id)].append(record)

    merged_count = 0
    symlink_count = 0

    for (year, course_id), course_records in sorted(grouped_records.items()):
        merged_dir = courses_dir / DEFAULT_MERGED_DIRNAME / str(year)
        merged_dir.mkdir(parents=True, exist_ok=True)

        components = group_records(
            course_records,
            learning_outcomes_similarity_backend,
            learning_outcomes_similarity_threshold,
            learning_outcomes_embedding_model,
        )
        if len(components) > 1:
            LOGGER.warning(
                "Course %s in year %s has %d merge components; using suffixes",
                course_id,
                year,
                len(components),
            )

        indexed_components = sorted(
            enumerate(components),
            key=lambda item: component_signature(item[1]),
        )

        for component_index, (_, component_records) in enumerate(indexed_components):
            suffix = "" if component_index == 0 else merge_suffix(component_index - 1)
            merged_path = merged_dir / f"course-{course_id}{suffix}.yml"
            merged_metadata = merge_records(component_records)
            merged_path.write_text(
                yaml.safe_dump(
                    merged_metadata.model_dump(by_alias=True, exclude_none=True),
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )
            merged_count += 1

            for record in component_records:
                symlink_path = record.year_dir / f"course-{course_id}{suffix}.yml"
                ensure_symlink(symlink_path, merged_path)
                symlink_count += 1

            LOGGER.info("Wrote %s from %s teaching file(s)", merged_path, len(component_records))

    return merged_count, symlink_count


def main() -> int:
    configure_logging()
    LOGGER.info("Command line: %s", shlex.join(sys.argv))
    args = parse_args()

    if not args.courses_dir.exists():
        LOGGER.error("courses directory does not exist: %s", args.courses_dir)
        return 2

    merged_count, symlink_count = merge_courses_tree(
        args.courses_dir,
        learning_outcomes_similarity_backend=args.learning_outcomes_similarity_backend,
        learning_outcomes_similarity_threshold=args.learning_outcomes_similarity_threshold,
        learning_outcomes_embedding_model=args.learning_outcomes_embedding_model,
    )
    LOGGER.info("Done. merged=%s symlinks=%s", merged_count, symlink_count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import hashlib
import json
import pathlib
import re
from typing import Any, Iterable

import yaml

from clustering.sections import SECTION_COURSE_CONTENTS, SECTION_LEARNING_OUTCOMES, extract_similarity_sections
from restructuring.models import ClusterInput, CourseInput, FinalClusterResponse, ModelConfig


REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parent.parent


def load_yaml_mapping(path: pathlib.Path, description: str) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as error:
        raise ValueError(f"{description} does not exist: {path}") from error
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f"Could not read {description} {path}: {error}") from error
    if not isinstance(payload, dict):
        raise ValueError(f"{description} must contain a YAML mapping: {path}")
    return payload


def _resolve_course_path(raw_path: Any, input_path: pathlib.Path) -> pathlib.Path:
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError("Course entry is missing a non-empty 'path'")
    path = pathlib.Path(raw_path)
    candidates = [path] if path.is_absolute() else [
        pathlib.Path.cwd() / path,
        REPOSITORY_ROOT / path,
        input_path.parent / path,
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise ValueError(f"Course file does not exist: {path}")


def load_clusters(input_path: pathlib.Path) -> list[ClusterInput]:
    input_path = input_path.resolve()
    payload = load_yaml_mapping(input_path, "Cluster YAML")
    clusters: list[ClusterInput] = []
    seen_cluster_ids: set[int] = set()
    for raw_name, raw_cluster in payload.items():
        name = str(raw_name)
        if not isinstance(raw_cluster, dict):
            raise ValueError(f"Cluster {name!r} must be a mapping")
        try:
            cluster_id = int(raw_cluster["index"])
        except KeyError as error:
            raise ValueError(f"Cluster {name!r} is missing 'index'") from error
        except (TypeError, ValueError) as error:
            raise ValueError(f"Cluster {name!r} has an invalid 'index'") from error
        if cluster_id in seen_cluster_ids:
            raise ValueError(f"Duplicate cluster ID: {cluster_id}")
        seen_cluster_ids.add(cluster_id)
        raw_courses = raw_cluster.get("courses")
        if not isinstance(raw_courses, list):
            raise ValueError(f"Cluster {name!r} is missing a 'courses' list")
        courses: list[CourseInput] = []
        for index, raw_course in enumerate(raw_courses):
            if not isinstance(raw_course, dict):
                raise ValueError(f"Course {index} in cluster {name!r} must be a mapping")
            course_path = _resolve_course_path(raw_course.get("path"), input_path)
            course_payload = load_yaml_mapping(course_path, "Course YAML")
            course_title = course_payload.get("course_title")
            course_title = course_title if isinstance(course_title, dict) else {}
            course_id = str(raw_course.get("id") or course_title.get("id") or "").strip()
            if not course_id:
                raise ValueError(f"Course in {course_path} is missing its ID")
            title = str(course_title.get("name") or raw_course.get("name") or "").strip()
            sections, languages = extract_similarity_sections(course_payload)
            courses.append(
                CourseInput(
                    course_id=course_id,
                    title=title,
                    path=str(course_path),
                    course_contents=sections.get(SECTION_COURSE_CONTENTS) or "",
                    course_contents_language=languages.get(SECTION_COURSE_CONTENTS),
                    learning_outcomes=sections.get(SECTION_LEARNING_OUTCOMES) or "",
                    learning_outcomes_language=languages.get(SECTION_LEARNING_OUTCOMES),
                )
            )
        courses.sort(key=lambda course: (course.course_id.casefold(), course.course_id))
        clusters.append(ClusterInput(cluster_id=cluster_id, name=name, courses=tuple(courses)))
    return sorted(clusters, key=lambda cluster: cluster.cluster_id)


def select_clusters(
    clusters: Iterable[ClusterInput],
    cluster_ids: Iterable[int] = (),
    name_regexes: Iterable[str] = (),
) -> list[ClusterInput]:
    available = list(clusters)
    requested_ids = set(cluster_ids)
    available_ids = {cluster.cluster_id for cluster in available}
    unknown_ids = sorted(requested_ids - available_ids)
    if unknown_ids:
        raise ValueError(f"Unknown cluster IDs: {unknown_ids}")
    compiled: list[re.Pattern[str]] = []
    for expression in name_regexes:
        try:
            compiled.append(re.compile(expression, re.IGNORECASE))
        except re.error as error:
            raise ValueError(f"Invalid cluster name regex {expression!r}: {error}") from error
    if not requested_ids and not compiled:
        return available
    selected = [
        cluster
        for cluster in available
        if cluster.cluster_id in requested_ids or any(pattern.search(cluster.name) for pattern in compiled)
    ]
    if not selected:
        raise ValueError("Cluster selectors matched no clusters")
    return selected


def conversation_cache_key(cluster: ClusterInput, config: ModelConfig) -> tuple[str, dict[str, Any]]:
    metadata = {
        "endpoint": config.endpoint,
        "model_parameters": config.cache_parameters(),
        "cluster": {"id": cluster.cluster_id, "name": cluster.name},
        "course_ids": sorted(course.course_id for course in cluster.courses),
    }
    serialized = json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest(), metadata


def load_cache(path: pathlib.Path, metadata: dict[str, Any]) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        payload = load_yaml_mapping(path, "Conversation cache")
    except ValueError:
        return []
    if payload.get("cache_key") != path.stem or payload.get("metadata") != metadata:
        return []
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return []
    normalized: list[dict[str, str]] = []
    for message in messages:
        if not isinstance(message, dict):
            return []
        role = message.get("role")
        content = message.get("content")
        if role not in {"system", "user", "assistant"} or not isinstance(content, str):
            return []
        normalized.append({"role": role, "content": content})
    return normalized


def _atomic_write_text(path: pathlib.Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(contents, encoding="utf-8")
    temporary.replace(path)


def write_cache(path: pathlib.Path, cache_key: str, metadata: dict[str, Any], messages: list[dict[str, str]]) -> None:
    payload = {"cache_key": cache_key, "metadata": metadata, "messages": messages}
    _atomic_write_text(path, yaml.safe_dump(payload, sort_keys=False, allow_unicode=True))


def validate_final_response(cluster: ClusterInput, response: FinalClusterResponse) -> None:
    expected_ids = {course.course_id for course in cluster.courses}
    actual_ids = {course.course_id for course in response.course_topics}
    if actual_ids != expected_ids:
        missing = sorted(expected_ids - actual_ids)
        extra = sorted(actual_ids - expected_ids)
        raise ValueError(f"Final response has wrong course IDs; missing={missing}, extra={extra}")
    plantuml = response.plantuml.strip()
    if not plantuml.startswith("@startuml") or not plantuml.endswith("@enduml"):
        raise ValueError("PlantUML must start with @startuml and end with @enduml")
    if "note " not in plantuml.casefold():
        raise ValueError("PlantUML must contain note boxes with topic descriptions")
    for course in cluster.courses:
        declaration = re.search(
            rf'class\s+"[^"]*{re.escape(course.course_id)}[^"]*"\s+as\s+([A-Za-z_][A-Za-z0-9_]*)[^\n]*#',
            plantuml,
            re.IGNORECASE,
        )
        if declaration is None:
            raise ValueError(f"PlantUML is missing a styled old-course class for {course.course_id}")
        alias = declaration.group(1)
        dashed_link = any(alias in line and ".." in line for line in plantuml.splitlines())
        if not dashed_link:
            raise ValueError(f"PlantUML old course {course.course_id} has no dashed subsumption link")


def write_cluster_artifacts(output_dir: pathlib.Path, cluster: ClusterInput, response: FinalClusterResponse) -> None:
    topics = {topic.key: topic.description.strip() for topic in sorted(response.topics, key=lambda item: item.key)}
    cluster_payload = {
        "cluster": {"id": cluster.cluster_id, "name": cluster.name},
        "topics": topics,
    }
    cluster_path = output_dir / f"topics-of-cluster-{cluster.cluster_id}.yml"
    _atomic_write_text(cluster_path, yaml.safe_dump(cluster_payload, sort_keys=False, allow_unicode=True))
    memberships = {item.course_id: item.topic_keys for item in response.course_topics}
    courses = {course.course_id: course for course in cluster.courses}
    for course_id in sorted(courses, key=lambda value: (value.casefold(), value)):
        course = courses[course_id]
        course_topics = {
            key: topics[key]
            for key in sorted(set(memberships[course_id]))
        }
        course_payload = {
            "cluster": {"id": cluster.cluster_id, "name": cluster.name},
            "course": {"id": course.course_id, "name": course.title},
            "topics": course_topics,
        }
        path = output_dir / f"topics-of-course-{course.course_id}.yml"
        _atomic_write_text(path, yaml.safe_dump(course_payload, sort_keys=False, allow_unicode=True))
    plantuml_path = output_dir / f"restructure-proposal-for-cluster-{cluster.cluster_id}.puml"
    _atomic_write_text(plantuml_path, response.plantuml.strip() + "\n")

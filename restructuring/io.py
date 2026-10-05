from __future__ import annotations

import hashlib
import html
import json
import pathlib
import re
from typing import Any, Iterable

import yaml

from clustering.sections import normalize_label, normalize_text
from restructuring.models import (
    ClusterInput,
    CourseInput,
    GlobalInput,
    ModelConfig,
    RestructuringProposal,
    TOPIC_KEY_PATTERN,
)


REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parent.parent
PROMPT_VERSION = 6
DEFAULT_SYLLABUS_SECTION_KEYS = ("title", "outcomes", "contents")

SYLLABUS_SECTION_ALIASES: dict[str, tuple[str, ...]] = {
    "outcomes": (
        "Learning outcomes",
        "Conoscenze e abilità da conseguire",
        "Conoscenze e abilita da conseguire",
    ),
    "contents": (
        "Course contents",
        "Contenuti",
    ),
    "bib": (
        "Readings/Bibliography",
        "Testi/Bibliografia",
    ),
    "teaching_methods": (
        "Teaching methods",
        "Modalità didattiche",
    ),
    "assessment": (
        "Assessment methods",
        "Modalità di verifica e valutazione dell'apprendimento",
    ),
    "teaching_tools": (
        "Teaching tools",
        "Strumenti didattici",
    ),
    "office_hours": (
        "Office hours",
        "Orario di ricevimento",
    ),
}


def normalize_syllabus_section_keys(section_keys: Iterable[str] | None = None) -> tuple[str, ...]:
    keys = DEFAULT_SYLLABUS_SECTION_KEYS if section_keys is None else tuple(section_keys)
    normalized: list[str] = []
    seen: set[str] = set()
    valid_keys = {"title"} | set(SYLLABUS_SECTION_ALIASES)
    for key in keys:
        if key not in valid_keys:
            raise ValueError(f"Unknown syllabus section keyword: {key}")
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return tuple(normalized)


def _contents_for_language(payload: dict[str, Any], language: str) -> dict[str, Any]:
    syllabus = payload.get("syllabus")
    if not isinstance(syllabus, dict):
        return {}
    page = syllabus.get(language)
    if not isinstance(page, dict):
        return {}
    contents = page.get("contents")
    return contents if isinstance(contents, dict) else {}


def _section_text(contents: dict[str, Any], aliases: tuple[str, ...]) -> tuple[str, str] | None:
    normalized_aliases = {normalize_label(alias) for alias in aliases}
    for label, value in contents.items():
        if normalize_label(str(label)) in normalized_aliases:
            text = normalize_text(value)
            if text:
                return str(label).strip(), text
    return None


def _section_language(payload: dict[str, Any], aliases: tuple[str, ...]) -> str | None:
    if _section_text(_contents_for_language(payload, "en"), aliases) is not None:
        return "en"
    if _section_text(_contents_for_language(payload, "it"), aliases) is not None:
        return "it"
    return None


def extract_course_syllabus_sections(
    payload: dict[str, Any],
    section_keys: Iterable[str] | None = None,
) -> tuple[tuple[str, str], ...]:
    selected_keys = normalize_syllabus_section_keys(section_keys)
    sections: list[tuple[str, str]] = []
    for section_key in selected_keys:
        if section_key == "title":
            continue
        aliases = SYLLABUS_SECTION_ALIASES[section_key]
        for language in ("en", "it"):
            found = _section_text(_contents_for_language(payload, language), aliases)
            if found is not None:
                sections.append(found)
                break
    return tuple(sections)


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


def load_clusters(
    input_path: pathlib.Path,
    syllabus_section_keys: Iterable[str] | None = None,
) -> list[ClusterInput]:
    input_path = input_path.resolve()
    selected_section_keys = normalize_syllabus_section_keys(syllabus_section_keys)
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
            syllabus_sections = extract_course_syllabus_sections(course_payload, selected_section_keys)
            contents_text = _section_text(_contents_for_language(course_payload, "en"), SYLLABUS_SECTION_ALIASES["contents"])
            if contents_text is None:
                contents_text = _section_text(_contents_for_language(course_payload, "it"), SYLLABUS_SECTION_ALIASES["contents"])
            outcomes_text = _section_text(_contents_for_language(course_payload, "en"), SYLLABUS_SECTION_ALIASES["outcomes"])
            if outcomes_text is None:
                outcomes_text = _section_text(_contents_for_language(course_payload, "it"), SYLLABUS_SECTION_ALIASES["outcomes"])
            courses.append(
                CourseInput(
                    course_id=course_id,
                    title=title,
                    path=str(course_path),
                    course_contents=contents_text[1] if contents_text is not None else "",
                    course_contents_language=_section_language(course_payload, SYLLABUS_SECTION_ALIASES["contents"]),
                    learning_outcomes=outcomes_text[1] if outcomes_text is not None else "",
                    learning_outcomes_language=_section_language(course_payload, SYLLABUS_SECTION_ALIASES["outcomes"]),
                    syllabus_sections=syllabus_sections,
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


def load_global_corpus(clusters: Iterable[ClusterInput]) -> GlobalInput:
    courses: dict[str, CourseInput] = {}
    sources: dict[str, list[tuple[int, str]]] = {}
    for cluster in clusters:
        for course in cluster.courses:
            previous = courses.setdefault(course.course_id, course)
            if (
                previous.course_id,
                previous.title,
                previous.course_contents,
                previous.course_contents_language,
                previous.learning_outcomes,
                previous.learning_outcomes_language,
                previous.syllabus_sections,
            ) != (
                course.course_id,
                course.title,
                course.course_contents,
                course.course_contents_language,
                course.learning_outcomes,
                course.learning_outcomes_language,
                course.syllabus_sections,
            ):
                raise ValueError(
                    f"Conflicting course data for duplicate course ID {course.course_id!r}"
                )
            sources.setdefault(course.course_id, []).append((cluster.cluster_id, cluster.name))
    return GlobalInput(
        courses=tuple(sorted(courses.values(), key=lambda item: (item.course_id.casefold(), item.course_id))),
        source_clusters={key: tuple(sorted(set(value))) for key, value in sources.items()},
    )


def conversation_cache_key(
    cluster: ClusterInput | GlobalInput,
    config: ModelConfig,
    *,
    syllabus_section_keys: Iterable[str] | None = None,
    topic_conversation_mode: str = "stateless",
) -> tuple[str, dict[str, Any]]:
    selected_section_keys = normalize_syllabus_section_keys(syllabus_section_keys)
    metadata = {
        "prompt_version": PROMPT_VERSION,
        "syllabus_sections": list(selected_section_keys),
        "topic_conversation_mode": topic_conversation_mode,
        "endpoint": config.endpoint,
        "model_parameters": config.cache_parameters(),
        **(
            {"cluster": {"id": cluster.cluster_id, "name": cluster.name}}
            if isinstance(cluster, ClusterInput)
            else {
                "analysis": {"mode": "all-courses"},
                "course_fingerprints": {
                    course.course_id: hashlib.sha256(
                        json.dumps(
                            {
                                "title": course.title,
                                "syllabus_sections": course.syllabus_sections,
                                "source_clusters": cluster.source_clusters[course.course_id],
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ).encode("utf-8")
                    ).hexdigest()
                    for course in cluster.courses
                },
            }
        ),
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


def write_cache(
    path: pathlib.Path,
    cache_key: str,
    metadata: dict[str, Any],
    messages: list[dict[str, str]],
) -> None:
    payload = {"cache_key": cache_key, "metadata": metadata, "messages": messages}
    _atomic_write_text(path, yaml.safe_dump(payload, sort_keys=False, allow_unicode=True))


def validate_restructuring_proposal(
    cluster: ClusterInput | GlobalInput,
    source_topics: dict[str, str],
    source_memberships: dict[str, list[str]],
    proposal: RestructuringProposal,
) -> None:
    proposed_topic_keys = {item.key for item in proposal.proposed_topics}
    proposed_course_keys = {item.key for item in proposal.proposed_courses}
    source_course_ids = {course.course_id for course in cluster.courses}

    unknown_source_topics = sorted(
        {
            key
            for topic in proposal.proposed_topics
            for key in topic.source_topic_keys
            if key not in source_topics
        }
    )
    if unknown_source_topics:
        raise ValueError(f"unknown source topic keys: {unknown_source_topics}")

    unknown_proposed_topics = sorted(
        {
            key
            for course in proposal.proposed_courses
            for key in course.topic_keys
            if key not in proposed_topic_keys
        }
    )
    if unknown_proposed_topics:
        raise ValueError(f"unknown proposed topic keys: {unknown_proposed_topics}")

    used_proposed_topics = {
        key for course in proposal.proposed_courses for key in course.topic_keys
    }
    unused_proposed_topics = sorted(proposed_topic_keys - used_proposed_topics)
    if unused_proposed_topics:
        raise ValueError(f"unused proposed topic keys: {unused_proposed_topics}")

    mapped_ids = {item.course_id for item in proposal.source_course_mappings}
    if mapped_ids != source_course_ids:
        raise ValueError(
            "source course mappings must cover exactly the source courses; "
            f"missing={sorted(source_course_ids - mapped_ids)}, "
            f"unknown={sorted(mapped_ids - source_course_ids)}"
        )

    referenced_courses = {
        key
        for item in proposal.source_course_mappings
        for key in item.proposed_course_keys
    } | {
        key
        for edge in proposal.prerequisites
        for key in (edge.prerequisite_course_key, edge.dependent_course_key)
    }
    unknown_courses = sorted(referenced_courses - proposed_course_keys)
    if unknown_courses:
        raise ValueError(f"unknown proposed course keys: {unknown_courses}")

    proposed_topics = {item.key: item for item in proposal.proposed_topics}
    proposed_courses = {item.key: item for item in proposal.proposed_courses}
    for mapping in proposal.source_course_mappings:
        covered_source_topics = {
            source_topic_key
            for proposed_course_key in mapping.proposed_course_keys
            for proposed_topic_key in proposed_courses[proposed_course_key].topic_keys
            for source_topic_key in proposed_topics[proposed_topic_key].source_topic_keys
        }
        missing = sorted(
            set(source_memberships[mapping.course_id]) - covered_source_topics
        )
        if missing:
            raise ValueError(
                f"source course {mapping.course_id} loses topic coverage: {missing}"
            )

    successors: dict[str, set[str]] = {key: set() for key in proposed_course_keys}
    indegree = {key: 0 for key in proposed_course_keys}
    for edge in proposal.prerequisites:
        successors[edge.prerequisite_course_key].add(edge.dependent_course_key)
        indegree[edge.dependent_course_key] += 1
    pending = [key for key, degree in indegree.items() if degree == 0]
    visited = 0
    while pending:
        current = pending.pop()
        visited += 1
        for dependent in successors[current]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                pending.append(dependent)
    if visited != len(proposed_course_keys):
        raise ValueError("course prerequisites must be acyclic")


def write_cluster_topics(
    output_dir: pathlib.Path,
    cluster: ClusterInput,
    topics: dict[str, str],
) -> pathlib.Path:
    sorted_topics = {
        key: topics[key].strip()
        for key in sorted(topics)
    }
    cluster_payload = {
        "cluster": {"id": cluster.cluster_id, "name": cluster.name},
        "topics": sorted_topics,
    }
    cluster_path = output_dir / f"topics-of-cluster-{cluster.cluster_id}.yml"
    _atomic_write_text(
        cluster_path,
        yaml.safe_dump(cluster_payload, sort_keys=False, allow_unicode=True),
    )
    return cluster_path


def write_course_topics(
    output_dir: pathlib.Path,
    cluster: ClusterInput | GlobalInput,
    course: CourseInput,
    topics: dict[str, str],
    topic_keys: Iterable[str],
) -> pathlib.Path:
    course_topics = {
        key: topics[key]
        for key in sorted(set(topic_keys))
    }
    course_payload = {
        **(
            {"cluster": {"id": cluster.cluster_id, "name": cluster.name}}
            if isinstance(cluster, ClusterInput)
            else {
                "analysis": {"mode": "all-courses"},
                "source_clusters": [
                    {"id": cluster_id, "name": name}
                    for cluster_id, name in cluster.source_clusters[course.course_id]
                ],
            }
        ),
        "course": {"id": course.course_id, "name": course.title},
        "topics": course_topics,
    }
    path = output_dir / f"topics-of-course-{course.course_id}.yml"
    _atomic_write_text(
        path,
        yaml.safe_dump(course_payload, sort_keys=False, allow_unicode=True),
    )
    return path


def write_global_topics(
    output_dir: pathlib.Path,
    global_input: GlobalInput,
    topics: dict[str, str],
) -> pathlib.Path:
    path = output_dir / "topics-global.yml"
    _atomic_write_text(path, yaml.safe_dump({
        "analysis": {"mode": "all-courses"},
        "course_ids": [course.course_id for course in global_input.courses],
        "topics": {key: topics[key].strip() for key in sorted(topics)},
    }, sort_keys=False, allow_unicode=True))
    return path


def load_topic_artifacts(
    directory: pathlib.Path,
    cluster: ClusterInput,
) -> tuple[dict[str, str], dict[str, list[str]]] | None:
    cluster_path = directory / f"topics-of-cluster-{cluster.cluster_id}.yml"
    if not cluster_path.exists():
        return None
    cluster_payload = load_yaml_mapping(cluster_path, "Cluster topic YAML")
    expected_cluster = {"id": cluster.cluster_id, "name": cluster.name}
    if cluster_payload.get("cluster") != expected_cluster:
        raise ValueError(f"Cluster metadata mismatch in {cluster_path}")
    topics = cluster_payload.get("topics")
    if not isinstance(topics, dict) or any(
        not isinstance(key, str)
        or re.fullmatch(TOPIC_KEY_PATTERN, key) is None
        or not isinstance(value, str)
        for key, value in topics.items()
    ):
        raise ValueError(f"Invalid topic dictionary in {cluster_path}")

    memberships: dict[str, list[str]] = {}
    for course in cluster.courses:
        course_path = directory / f"topics-of-course-{course.course_id}.yml"
        payload = load_yaml_mapping(course_path, "Course topic YAML")
        if payload.get("cluster") != expected_cluster or payload.get("course") != {
            "id": course.course_id,
            "name": course.title,
        }:
            raise ValueError(f"Course metadata mismatch in {course_path}")
        course_topics = payload.get("topics")
        if not isinstance(course_topics, dict) or any(
            key not in topics or description != topics[key]
            for key, description in course_topics.items()
        ):
            raise ValueError(f"Invalid topic assignment in {course_path}")
        memberships[course.course_id] = list(course_topics)
    return dict(topics), memberships


def _mermaid_label(value: str) -> str:
    return html.escape(value.strip(), quote=True).replace("\n", " ")


def render_mermaid(cluster: ClusterInput | GlobalInput, proposal: RestructuringProposal) -> str:
    course_aliases = {
        item.key: f"P{index}"
        for index, item in enumerate(proposal.proposed_courses)
    }
    source_aliases = {
        course.course_id: f"S{index}"
        for index, course in enumerate(cluster.courses)
    }
    lines = [
        (
            f"%% Restructuring proposal for cluster {cluster.cluster_id}: {_mermaid_label(cluster.name)}"
            if isinstance(cluster, ClusterInput) else "%% Restructuring proposal for all courses"
        ),
        "flowchart LR",
        '  subgraph proposed["Proposed curriculum"]',
    ]
    for item in proposal.proposed_courses:
        topics = "<br/>".join(_mermaid_label(key) for key in item.topic_keys)
        label = f"{_mermaid_label(item.title)}<br/>{topics}"
        lines.append(f'    {course_aliases[item.key]}["{label}"]:::proposed')
    lines.extend(['  end', '  subgraph current["Current courses"]'])
    for course in cluster.courses:
        label = _mermaid_label(f"{course.course_id} — {course.title}")
        lines.append(f'    {source_aliases[course.course_id]}["{label}"]:::source')
    lines.append("  end")
    for edge in proposal.prerequisites:
        lines.append(
            f"  {course_aliases[edge.prerequisite_course_key]} --> "
            f"{course_aliases[edge.dependent_course_key]}"
        )
    for mapping in proposal.source_course_mappings:
        for proposed_key in mapping.proposed_course_keys:
            lines.append(
                f"  {source_aliases[mapping.course_id]} -.-> "
                f"{course_aliases[proposed_key]}"
            )
    lines.extend(
        [
            "  classDef proposed fill:#e8f1fb,stroke:#24527a,color:#111",
            "  classDef source fill:#f3f3f3,stroke:#999,color:#555",
        ]
    )
    return "\n".join(lines) + "\n"


def write_restructuring_proposal(
    output_dir: pathlib.Path,
    cluster: ClusterInput | GlobalInput,
    source_topics: dict[str, str],
    source_memberships: dict[str, list[str]],
    proposal: RestructuringProposal,
) -> tuple[pathlib.Path, pathlib.Path]:
    stem = f"restructure-proposal-for-cluster-{cluster.cluster_id}" if isinstance(cluster, ClusterInput) else "restructure-proposal-global"
    yaml_path = output_dir / f"{stem}.yml"
    mermaid_path = output_dir / f"{stem}.mmd"
    payload = {
        **({"cluster": {"id": cluster.cluster_id, "name": cluster.name}} if isinstance(cluster, ClusterInput) else {"analysis": {"mode": "all-courses"}}),
        "source_topics": {key: source_topics[key] for key in sorted(source_topics)},
        "source_course_topic_assignments": {
            course_id: sorted(source_memberships[course_id])
            for course_id in sorted(source_memberships)
        },
        **proposal.model_dump(),
    }
    _atomic_write_text(
        yaml_path,
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
    )
    _atomic_write_text(mermaid_path, render_mermaid(cluster, proposal))
    return yaml_path, mermaid_path

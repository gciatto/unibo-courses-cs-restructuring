from __future__ import annotations

import hashlib
import html
import json
import pathlib
import re
import statistics
from typing import Any, Iterable

import yaml

from clustering.sections import normalize_label, normalize_text
from restructuring.models import (
    ClusterInput,
    CourseInput,
    GlobalInput,
    ModelConfig,
    ProposedTopic,
    RestructuringProposal,
    SourceCourseMapping,
    TOPIC_KEY_PATTERN,
)


REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parent.parent
PROMPT_VERSION = 7
DEFAULT_MAX_ECTS = 6
DEFAULT_PREFERRED_ECTS = 3
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


def _course_credits(payload: dict[str, Any]) -> float | None:
    # ponytail: merged courses listing several credit values (e.g. [12, 9])
    # take the largest; split such courses upstream if that skews weights.
    values = [
        value for value in payload.get("credits") or []
        if isinstance(value, (int, float)) and value > 0
    ]
    return float(max(values)) if values else None


def estimate_topic_weights(
    courses: Iterable[CourseInput],
    memberships: dict[str, list[str]],
) -> dict[str, dict[str, Any]]:
    """Each course spreads its credits evenly over its topics; a topic's
    estimate is the median of its shares across the courses teaching it."""
    shares: dict[str, dict[str, float]] = {}
    for course in courses:
        keys = memberships.get(course.course_id) or []
        if course.credits is None or not keys:
            continue
        for key in keys:
            shares.setdefault(key, {})[course.course_id] = round(course.credits / len(keys), 3)
    return {
        key: {
            "ects": round(statistics.median(shares[key].values()), 3),
            "per_course": shares[key],
        }
        for key in sorted(shares)
    }


def _weights_payload(
    courses: Iterable[CourseInput],
    memberships: dict[str, list[str]],
) -> dict[str, Any]:
    courses = [course for course in courses if course.course_id in memberships]
    weights = estimate_topic_weights(courses, memberships)
    return {
        "topic_weights": weights,
        # Sum of a course's topic estimates against its real credits: how far
        # the median estimates are from reproducing the source catalogue.
        "course_credit_check": {
            course.course_id: {
                "credits": course.credits,
                "estimated_ects": round(sum(
                    weights[key]["ects"] for key in memberships[course.course_id]
                    if key in weights
                ), 3),
            }
            for course in courses
            if course.credits is not None
        },
    }


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
                    credits=_course_credits(course_payload),
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
    *,
    require_all_topics_used: bool = True,
    max_ects: int = DEFAULT_MAX_ECTS,
) -> None:
    proposed_topic_keys = {item.key for item in proposal.proposed_topics}
    proposed_course_keys = {item.key for item in proposal.proposed_courses}

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
    if unused_proposed_topics and require_all_topics_used:
        raise ValueError(f"unused proposed topic keys: {unused_proposed_topics}")

    # ponytail: each proposed topic/module is assumed worth >= 1 ECTS; size
    # modules explicitly if this bound proves too loose.
    fat = sorted(
        f"{course.key} ({len(course.topic_keys)} topics, {course.ects} ECTS)"
        for course in proposal.proposed_courses
        if len(course.topic_keys) > course.ects
    )
    if fat:
        raise ValueError(
            "courses have more topics than ECTS; split them (e.g. fundamentals "
            f"vs advanced, or by abstraction level): {fat}"
        )

    oversized = sorted(
        f"{course.key} ({course.ects} ECTS)"
        for course in proposal.proposed_courses
        if course.ects > max_ects
    )
    if oversized:
        raise ValueError(
            f"courses exceed the maximum of {max_ects} ECTS; split them (e.g. "
            f"fundamentals vs advanced, or by abstraction level): {oversized}"
        )

    referenced_courses = {
        key
        for edge in proposal.prerequisites
        for key in (edge.prerequisite_course_key, edge.dependent_course_key)
    }
    unknown_courses = sorted(referenced_courses - proposed_course_keys)
    if unknown_courses:
        raise ValueError(f"unknown proposed course keys: {unknown_courses}")

    # Every proposed topic is used by some course (checked above), so covering
    # each assigned source topic by provenance lets every source course be
    # mapped locally without loss; see derive_source_course_mappings.
    provenance = {
        key for topic in proposal.proposed_topics for key in topic.source_topic_keys
    }
    uncovered = sorted(
        {
            key
            for course in cluster.courses
            for key in source_memberships[course.course_id]
        }
        - provenance
    )
    if uncovered:
        raise ValueError(
            "assigned source topics missing from proposed topic provenance: "
            f"{uncovered}"
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


def _proposed_course_provenance(proposal: RestructuringProposal) -> dict[str, set[str]]:
    topics = {
        item.key: set(item.source_topic_keys) for item in proposal.proposed_topics
    }
    return {
        item.key: set().union(*(topics[key] for key in item.topic_keys))
        for item in proposal.proposed_courses
    }


def derive_source_course_mappings(
    cluster: ClusterInput | GlobalInput,
    source_memberships: dict[str, list[str]],
    proposal: RestructuringProposal,
) -> list[SourceCourseMapping]:
    """Map each source course onto proposed courses by greedy topic set cover.

    Each step picks the proposed course covering most still-uncovered source
    topics, preferring the least unrelated content, then the smallest key.
    """
    # ponytail: greedy set cover is within ln(n) of optimal; exact ILP if
    # fragmentation numbers ever look suspicious.
    provenance = _proposed_course_provenance(proposal)
    mappings: list[SourceCourseMapping] = []
    for course in cluster.courses:
        own = set(source_memberships[course.course_id])
        remaining = set(own)
        chosen: list[str] = []
        while remaining:
            best = min(
                provenance,
                key=lambda key: (
                    -len(provenance[key] & remaining),
                    len(provenance[key] - own),
                    key,
                ),
            )
            if not provenance[best] & remaining:
                raise ValueError(
                    f"source course {course.course_id} loses topic coverage: "
                    f"{sorted(remaining)}"
                )
            chosen.append(best)
            remaining -= provenance[best]
        mappings.append(
            SourceCourseMapping(course_id=course.course_id, proposed_course_keys=chosen)
        )
    return mappings


def write_cluster_topics(
    output_dir: pathlib.Path,
    cluster: ClusterInput,
    topics: dict[str, str],
    memberships: dict[str, list[str]],
) -> pathlib.Path:
    sorted_topics = {
        key: topics[key].strip()
        for key in sorted(topics)
    }
    cluster_payload = {
        "cluster": {"id": cluster.cluster_id, "name": cluster.name},
        "topics": sorted_topics,
        **_weights_payload(cluster.courses, memberships),
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
    memberships: dict[str, list[str]],
) -> pathlib.Path:
    path = output_dir / "topics-global.yml"
    _atomic_write_text(path, yaml.safe_dump({
        "analysis": {"mode": "all-courses"},
        "course_ids": [course.course_id for course in global_input.courses],
        "topics": {key: topics[key].strip() for key in sorted(topics)},
        **_weights_payload(global_input.courses, memberships),
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


def load_global_topic_artifacts(
    directory: pathlib.Path,
    global_input: GlobalInput,
) -> tuple[dict[str, str], dict[str, list[str]]] | None:
    topics_path = directory / "topics-global.yml"
    if not topics_path.exists():
        return None
    payload = load_yaml_mapping(topics_path, "Global topic YAML")
    if payload.get("analysis") != {"mode": "all-courses"} or payload.get("course_ids") != [
        course.course_id for course in global_input.courses
    ]:
        raise ValueError(f"Global topic metadata mismatch in {topics_path}")
    topics = payload.get("topics")
    if not isinstance(topics, dict) or any(
        not isinstance(key, str)
        or re.fullmatch(TOPIC_KEY_PATTERN, key) is None
        or not isinstance(value, str)
        for key, value in topics.items()
    ):
        raise ValueError(f"Invalid topic dictionary in {topics_path}")
    memberships: dict[str, list[str]] = {}
    for course in global_input.courses:
        course_path = directory / f"topics-of-course-{course.course_id}.yml"
        payload = load_yaml_mapping(course_path, "Course topic YAML")
        expected_sources = [
            {"id": cluster_id, "name": name}
            for cluster_id, name in global_input.source_clusters[course.course_id]
        ]
        if (
            payload.get("analysis") != {"mode": "all-courses"}
            or payload.get("source_clusters") != expected_sources
            or payload.get("course") != {"id": course.course_id, "name": course.title}
        ):
            raise ValueError(f"Course metadata mismatch in {course_path}")
        course_topics = payload.get("topics")
        if not isinstance(course_topics, dict) or any(
            key not in topics or description != topics[key]
            for key, description in course_topics.items()
        ):
            raise ValueError(f"Invalid topic assignment in {course_path}")
        memberships[course.course_id] = list(course_topics)
    return dict(topics), memberships


# Label colour of proposed topics by source-course scope (see
# clustering.export_cluster_courses.course_scope). Order is priority: a topic
# takes the first scope among the source courses teaching its source topics.
TOPIC_ORIGIN_COLOURS = {
    "service": "#0072b2",
    "external": "#d55e00",
    "borrow": "#aa4499",
    "weak_internal": "#009e73",
    "internal": "#000000",
}
TOPIC_ORIGIN_LEGEND = {
    "service": "service: some DISI teacher, no DISI degree programme",
    "external": "external: no DISI teacher, no DISI degree programme",
    "borrow": "borrow: no DISI teacher, some DISI degree programme",
    "weak_internal": "weak_internal: some (not all) DISI teachers and DISI degree programmes",
    "internal": "internal: only DISI teachers and DISI degree programmes",
}


def topic_origins(
    proposal: RestructuringProposal,
    source_memberships: dict[str, list[str]],
    course_scopes: dict[str, str],
) -> dict[str, str]:
    """Proposed topic key -> highest-priority TOPIC_ORIGIN_COLOURS scope among
    the source courses teaching any of its source topics."""
    scopes_by_topic: dict[str, set[str]] = {}
    for course_id, keys in source_memberships.items():
        for key in keys:
            scopes_by_topic.setdefault(key, set()).add(course_scopes.get(course_id, ""))
    origins = {}
    for topic in proposal.proposed_topics:
        scopes = set().union(*(scopes_by_topic.get(key, set()) for key in topic.source_topic_keys))
        origin = next((scope for scope in TOPIC_ORIGIN_COLOURS if scope in scopes), None)
        if origin:
            origins[topic.key] = origin
    return origins


def _mermaid_label(value: str) -> str:
    return html.escape(value.strip(), quote=True).replace("\n", " ")


def render_mermaid(
    cluster: ClusterInput | GlobalInput,
    proposal: RestructuringProposal,
    mappings: Iterable[SourceCourseMapping] | None = None,
    show_topics: bool = False,
) -> str:
    """Proposed courses with prerequisites (optionally listing their topic
    keys), or, given mappings, the correspondence from current courses
    (grouped by identical targets)."""
    course_aliases = {
        item.key: f"P{index}"
        for index, item in enumerate(proposal.proposed_courses)
    }
    header = (
        f"%% Restructuring proposal for cluster {cluster.cluster_id}: {_mermaid_label(cluster.name)}"
        if isinstance(cluster, ClusterInput) else "%% Restructuring proposal for all courses"
    )
    lines = [header, "flowchart TB" if mappings is None else "flowchart LR"]
    for item in proposal.proposed_courses:
        label = f"{_mermaid_label(item.title)}<br/>{item.ects} ECTS"
        if show_topics:
            label += "<br/>" + "<br/>".join(_mermaid_label(key) for key in item.topic_keys)
        lines.append(f'  {course_aliases[item.key]}["{label}"]:::proposed')
    if show_topics:
        legend = "<br/>".join([
            "Topic colour: first scope, in this order, among the",
            "current courses teaching it (by teachers' and programmes' department)",
            *TOPIC_ORIGIN_LEGEND.values(),
        ])
        lines.append(f'  legend["{legend}"]:::source')
    if mappings is None:
        for edge in proposal.prerequisites:
            lines.append(
                f"  {course_aliases[edge.prerequisite_course_key]} --> "
                f"{course_aliases[edge.dependent_course_key]}"
            )
    else:
        titles = {course.course_id: course.title for course in cluster.courses}
        groups: dict[tuple[str, ...], dict[str, list[str]]] = {}
        for mapping in mappings:
            by_title = groups.setdefault(tuple(sorted(mapping.proposed_course_keys)), {})
            by_title.setdefault(titles[mapping.course_id], []).append(mapping.course_id)
        for index, (targets, by_title) in enumerate(groups.items()):
            label = "<br/>".join(
                _mermaid_label(f"{title} ({', '.join(ids)})")
                for title, ids in by_title.items()
            )
            lines.append(f'  S{index}["{label}"]:::source')
            lines.extend(f"  S{index} -.-> {course_aliases[key]}" for key in targets)
    lines.extend(
        [
            "  classDef proposed fill:#e8f1fb,stroke:#24527a",
            "  classDef source fill:#f3f3f3,stroke:#999",
        ]
    )
    return "\n".join(lines) + "\n"


def course_group_roots(proposal: RestructuringProposal) -> list[tuple[list[str], list[str]]]:
    """Connected components of the prerequisite graph as (roots, members):
    roots ranked by descendants then key, members in proposal order."""
    import networkx as nx

    graph = nx.DiGraph()
    graph.add_nodes_from(course.key for course in proposal.proposed_courses)
    graph.add_edges_from(
        (edge.prerequisite_course_key, edge.dependent_course_key) for edge in proposal.prerequisites
    )
    order = {course.key: index for index, course in enumerate(proposal.proposed_courses)}
    groups = []
    for component in nx.weakly_connected_components(graph):
        roots = sorted(
            (key for key in component if graph.in_degree(key) == 0),
            key=lambda key: (-len(nx.descendants(graph, key)), key),
        )
        groups.append((roots, sorted(component, key=order.__getitem__)))
    return sorted(groups, key=lambda group: order[group[1][0]])


def course_group_name(root: str, members: list[str]) -> str:
    return root if len(members) == 1 else f"{root}-hierarchy"


def default_course_groups(proposal: RestructuringProposal) -> dict[str, list[str]]:
    return {
        course_group_name(roots[0], members): members
        for roots, members in course_group_roots(proposal)
    }


def _csv_field(value: str) -> str:
    return '"' + " ".join(value.split()).replace('"', '""') + '"'


def render_sankey(
    cluster: ClusterInput | GlobalInput,
    proposal: RestructuringProposal,
    mappings: Iterable[SourceCourseMapping],
    source_memberships: dict[str, list[str]],
    only_targets: set[str] | None = None,
) -> str:
    """Credits flowing from each current course into the new courses it maps
    to: a course spreads its credits (1 per topic when unknown) evenly over
    its topics, and each topic flows to the first mapped course covering it.
    `only_targets` keeps the flows into those courses, computed as above."""
    provenance = _proposed_course_provenance(proposal)
    courses = {course.course_id: course for course in cluster.courses}
    targets: dict[str, str] = {}
    for item in proposal.proposed_courses:
        name = f"{item.title} ({item.ects} ECTS)"
        targets[item.key] = name if name not in targets.values() else f"{name} [{item.key}]"
    rows = []
    used: set[str] = set()
    sources = set()
    for mapping in mappings:
        course = courses[mapping.course_id]
        remaining = set(source_memberships[mapping.course_id])
        share = (course.credits or len(remaining)) / len(remaining) if remaining else 0
        for key in mapping.proposed_course_keys:
            covered = provenance[key] & remaining
            remaining -= covered
            if only_targets is not None and key not in only_targets:
                continue
            sources.add(mapping.course_id)
            used.add(key)
            rows.append(
                f"{_csv_field(f'{course.title} ({course.course_id})')},"
                f"{_csv_field(targets[key])},{round(share * len(covered), 3)}"
            )
    # Heights grow with the busier side so labels do not overlap.
    height = max(400, 24 * max(len(sources), len(used)))
    return "\n".join([
        "---",
        "config:",
        "  sankey:",
        "    width: 1400",
        f"    height: {height}",
        "    linkColor: source",
        "    showValues: true",
        '    prefix: " · "',
        '    suffix: " ECTS"',
        "---",
        "sankey-beta",
        "",
        *rows,
    ]) + "\n"


def write_restructuring_proposal(
    output_dir: pathlib.Path,
    cluster: ClusterInput | GlobalInput,
    source_topics: dict[str, str],
    source_memberships: dict[str, list[str]],
    proposal: RestructuringProposal,
    decomposed_courses: dict[str, list[str]] | None = None,
    course_groups: dict[str, list[str]] | None = None,
) -> tuple[pathlib.Path, pathlib.Path]:
    stem = f"restructure-proposal-for-cluster-{cluster.cluster_id}" if isinstance(cluster, ClusterInput) else "restructure-proposal-global"
    yaml_path = output_dir / f"{stem}.yml"
    mermaid_path = output_dir / f"{stem}.mmd"
    mappings = derive_source_course_mappings(cluster, source_memberships, proposal)
    provenance = _proposed_course_provenance(proposal)
    weights = estimate_topic_weights(cluster.courses, source_memberships)
    mapped_payload = []
    for mapping in mappings:
        own = set(source_memberships[mapping.course_id])
        covered = set().union(*(provenance[key] for key in mapping.proposed_course_keys))
        mapped_payload.append({
            **mapping.model_dump(),
            # Share of source topics in the mapped courses the old course did not teach.
            "overshoot": round(len(covered - own) / len(covered), 3) if covered else 0.0,
        })
    payload = {
        **({"cluster": {"id": cluster.cluster_id, "name": cluster.name}} if isinstance(cluster, ClusterInput) else {"analysis": {"mode": "all-courses"}}),
        "source_topics": {key: source_topics[key] for key in sorted(source_topics)},
        "source_course_topic_assignments": {
            course_id: sorted(source_memberships[course_id])
            for course_id in sorted(source_memberships)
        },
        **proposal.model_dump(),
        "source_course_mappings": mapped_payload,
        "proposed_course_estimated_ects": {
            item.key: round(sum(
                weights[key]["ects"] for key in provenance[item.key] if key in weights
            ), 3)
            for item in proposal.proposed_courses
        },
        "proposed_course_reuse": {
            item.key: sum(item.key in mapping.proposed_course_keys for mapping in mappings)
            for item in proposal.proposed_courses
        },
        # Oversized course key -> keys of the smaller courses replacing it.
        "decomposed_courses": decomposed_courses or {},
    }
    if isinstance(cluster, GlobalInput):
        # Connected prerequisite components; per-group/ holds their diagrams.
        course_groups = course_groups or default_course_groups(proposal)
        payload["proposed_course_groups"] = course_groups
    _atomic_write_text(
        yaml_path,
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
    )
    write_proposal_mermaid(output_dir, stem, cluster, proposal, mappings, source_memberships)
    if course_groups:
        write_group_mermaid(output_dir, cluster, proposal, mappings, source_memberships, course_groups)
    return yaml_path, mermaid_path


def write_proposal_mermaid(
    output_dir: pathlib.Path,
    stem: str,
    cluster: ClusterInput | GlobalInput,
    proposal: RestructuringProposal,
    mappings: list[SourceCourseMapping],
    source_memberships: dict[str, list[str]],
) -> list[pathlib.Path]:
    paths = [output_dir / f"{stem}{suffix}.mmd" for suffix in ("", "-topics", "-mapping", "-sankey")]
    _atomic_write_text(paths[0], render_mermaid(cluster, proposal))
    _atomic_write_text(paths[1], render_mermaid(cluster, proposal, show_topics=True))
    _atomic_write_text(paths[2], render_mermaid(cluster, proposal, mappings))
    _atomic_write_text(paths[3], render_sankey(cluster, proposal, mappings, source_memberships))
    return paths


def write_group_mermaid(
    output_dir: pathlib.Path,
    cluster: ClusterInput | GlobalInput,
    proposal: RestructuringProposal,
    mappings: list[SourceCourseMapping],
    source_memberships: dict[str, list[str]],
    course_groups: dict[str, list[str]],
) -> list[pathlib.Path]:
    """Per-group cuts in output_dir/per-group: -topics, -mapping and -sankey
    (the latter two only when some current course maps into the group), plus
    the plain prerequisite view for groups of more than one course."""
    group_dir = output_dir / "per-group"
    group_dir.mkdir(exist_ok=True)
    paths = []
    for name, members in course_groups.items():
        keep = set(members)
        courses = [course for course in proposal.proposed_courses if course.key in keep]
        topic_keys = {key for course in courses for key in course.topic_keys}
        group = RestructuringProposal(
            proposed_topics=[topic for topic in proposal.proposed_topics if topic.key in topic_keys],
            proposed_courses=courses,
            prerequisites=[edge for edge in proposal.prerequisites if edge.dependent_course_key in keep],
        )
        contributing = [mapping for mapping in mappings if keep & set(mapping.proposed_course_keys)]
        views = {"-topics": render_mermaid(cluster, group, show_topics=True)}
        # No current course maps here (greedy cover chose other courses): an
        # empty sankey does not even parse, so skip both correspondence views.
        if contributing:
            views["-mapping"] = render_mermaid(cluster, group, [
                SourceCourseMapping(
                    course_id=mapping.course_id,
                    proposed_course_keys=[key for key in mapping.proposed_course_keys if key in keep],
                )
                for mapping in contributing
            ])
            # Flows computed on the full mapping, then cut to this group.
            views["-sankey"] = render_sankey(cluster, proposal, contributing, source_memberships, keep)
        if len(members) > 1:
            views[""] = render_mermaid(cluster, group)
        for suffix, text in views.items():
            path = group_dir / f"{name}{suffix}.mmd"
            _atomic_write_text(path, text)
            paths.append(path)
    return paths


def write_global_modules(
    output_dir: pathlib.Path,
    source_topics: dict[str, str],
    modules: Iterable[ProposedTopic],
    weights: dict[str, dict[str, Any]],
) -> pathlib.Path:
    """Persist the module layer so a failed course-assembly call keeps it."""
    path = output_dir / "modules-global.yml"
    _atomic_write_text(path, yaml.safe_dump({
        "analysis": {"mode": "all-courses"},
        "modules": [
            {
                "key": module.key,
                "description": module.description,
                "estimated_ects": round(sum(
                    weights[key]["ects"] for key in module.source_topic_keys if key in weights
                ), 3),
                "source_topics": {key: source_topics[key] for key in module.source_topic_keys},
            }
            for module in modules
        ],
    }, sort_keys=False, allow_unicode=True))
    return path

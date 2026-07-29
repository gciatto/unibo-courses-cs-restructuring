from __future__ import annotations

import json
import logging
import os
import pathlib
import random
import time
from datetime import datetime
from typing import Any, Callable, TypeVar

from pydantic import BaseModel, ValidationError

from restructuring.io import (
    REPOSITORY_ROOT,
    conversation_cache_key,
    load_cache,
    load_clusters,
    select_clusters,
    validate_final_response,
    write_cache,
    write_cluster_artifacts,
)
from restructuring.models import (
    ClusterInput,
    CourseInput,
    CourseTopicsResponse,
    FinalClusterResponse,
    ModelConfig,
    RetryConfig,
)

LOGGER = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are a curriculum analyst and software-architecture modeller.

Work on exactly one cluster of existing university courses per conversation. Build a
canonical ontology of topics actually supported by the supplied syllabus evidence,
then propose a rational hierarchy of new courses.

Grounding rules:
- Treat course IDs and titles as labels only. Never infer a topic from a course title,
  cluster title, common expectations, or outside knowledge.
- A source topic may be created or assigned to a course only when it is supported by
  that course's supplied Course contents or Learning outcomes.
- Topic keys must be short, lowercase snake_case identifiers. Descriptions must state
  what the supplied syllabi actually cover.
- As new evidence arrives, refine the complete ontology: update descriptions, merge
  duplicates, split overly broad topics, and return the entire revised ontology.
- covered_topic_keys must select only topics evidenced by the current syllabus.

Final restructuring rules:
- Reconcile every old course against one final source-topic ontology.
- The proposed new curriculum may group or abstract source topics into sensible target
  topics, but must not claim that an old course covers unsupported source material.
- Return render-ready PlantUML from @startuml through @enduml.
- New classes represent proposed courses; their fields contain only target topic keys.
- Put target-topic descriptions in PlantUML note boxes, not in class fields.
- Use A <|-- B to mean B depends on or is propaedeutically after A; multiple inheritance
  is allowed.
- Show every old course as a differently coloured or transparent class labelled with
  its ID and title, and connect it to every subsuming new class with dashed arrows.
"""


T = TypeVar("T", bound=BaseModel)


def topics_mapping(response: CourseTopicsResponse) -> dict[str, str]:
    return {
        topic.key: topic.description.strip()
        for topic in sorted(response.topics, key=lambda topic: topic.key)
    }


def course_prompt(course: CourseInput, current_topics: dict[str, str]) -> str:
    evidence = {
        "course_contents": {
            "language": course.course_contents_language,
            "text": course.course_contents,
        },
        "learning_outcomes": {
            "language": course.learning_outcomes_language,
            "text": course.learning_outcomes,
        },
    }
    return f"""Analyze the next old course in this cluster.

<metadata_not_evidence>
course_id: {course.course_id}
course_title: {course.title}
</metadata_not_evidence>

<current_topics>
{json.dumps(current_topics, ensure_ascii=False, sort_keys=True, indent=2)}
</current_topics>

<syllabus_evidence>
{json.dumps(evidence, ensure_ascii=False, sort_keys=True, indent=2)}
</syllabus_evidence>

Return the complete refined topic ontology after considering only this syllabus, then
select in covered_topic_keys exactly the refined topics covered by this course."""


def final_prompt(
    cluster: ClusterInput,
    current_topics: dict[str, str],
    provisional_memberships: dict[str, list[str]],
) -> str:
    old_courses = [
        {"id": course.course_id, "title": course.title}
        for course in cluster.courses
    ]
    return f"""All old courses in this cluster have now been examined.

<cluster_metadata>
cluster_id: {cluster.cluster_id}
cluster_name: {cluster.name}
</cluster_metadata>

<current_source_topics>
{json.dumps(current_topics, ensure_ascii=False, sort_keys=True, indent=2)}
</current_source_topics>

<provisional_course_memberships>
{json.dumps(provisional_memberships, ensure_ascii=False, sort_keys=True, indent=2)}
</provisional_course_memberships>

<old_course_labels>
{json.dumps(old_courses, ensure_ascii=False, sort_keys=True, indent=2)}
</old_course_labels>

Produce the final reconciled source-topic ontology and a course_topics entry for every
old course, using only final topic keys. Then produce the complete PlantUML restructuring
proposal according to the system rules. Every old course ID and title must appear in the
diagram, and every old course must have at least one dashed link to a proposed new class."""


def _is_retryable(error: Exception) -> bool:
    if isinstance(error, (ValidationError, ResponseValidationError)):
        return True
    status_code = getattr(error, "status_code", None)
    if status_code in {408, 409, 429} or isinstance(status_code, int) and status_code >= 500:
        return True
    return error.__class__.__name__ in {
        "RateLimitError",
        "APITimeoutError",
        "APIConnectionError",
        "InternalServerError",
    }


class ResponseValidationError(ValueError):
    pass


def call_with_backoff(
    operation: Callable[[], T],
    retry: RetryConfig,
    *,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> T:
    for attempt in range(retry.max_retries + 1):
        try:
            return operation()
        except Exception as error:
            if attempt >= retry.max_retries or not _is_retryable(error):
                raise
            ceiling = min(retry.max_backoff, retry.initial_backoff * (2**attempt))
            delay = random_uniform(ceiling / 2, ceiling) if ceiling > 0 else 0
            LOGGER.warning(
                "Retryable LLM error %s; retrying in %.2f seconds (%d/%d)",
                error.__class__.__name__,
                delay,
                attempt + 1,
                retry.max_retries,
            )
            if delay > 0:
                sleep(delay)
    raise RuntimeError("unreachable")


def call_structured(
    client: Any,
    messages: list[dict[str, str]],
    response_model: type[T],
    config: ModelConfig,
    retry: RetryConfig,
    *,
    validator: Callable[[T], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> T:
    def operation() -> T:
        arguments: dict[str, Any] = {
            "model": config.model,
            "messages": messages,
            "response_format": response_model,
            "max_completion_tokens": config.max_completion_tokens,
        }
        if config.temperature is not None:
            arguments["temperature"] = config.temperature
        completion = client.chat.completions.parse(**arguments)
        message = completion.choices[0].message
        refusal = getattr(message, "refusal", None)
        if refusal:
            raise RuntimeError(f"Model refused the request: {refusal}")
        parsed = getattr(message, "parsed", None)
        if parsed is None:
            content = getattr(message, "content", None)
            if not isinstance(content, str):
                raise ResponseValidationError("Model returned neither parsed output nor text")
            try:
                parsed = response_model.model_validate_json(content)
            except ValidationError as error:
                raise ResponseValidationError(str(error)) from error
        if not isinstance(parsed, response_model):
            parsed = response_model.model_validate(parsed)
        if validator is not None:
            try:
                validator(parsed)
            except ValueError as error:
                raise ResponseValidationError(str(error)) from error
        return parsed

    return call_with_backoff(operation, retry, sleep=sleep, random_uniform=random_uniform)


def _cached_response(
    cached: list[dict[str, str]],
    cursor: int,
    user_message: dict[str, str],
    response_model: type[T],
) -> tuple[T | None, int]:
    if cursor + 1 >= len(cached) or cached[cursor] != user_message:
        return None, cursor
    assistant = cached[cursor + 1]
    if assistant.get("role") != "assistant":
        return None, cursor
    try:
        parsed = response_model.model_validate_json(assistant["content"])
    except (KeyError, ValidationError):
        return None, cursor
    return parsed, cursor + 2


def process_cluster(
    cluster: ClusterInput,
    client: Any,
    config: ModelConfig,
    retry: RetryConfig,
    cache_dir: pathlib.Path,
    *,
    refresh_cache: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> FinalClusterResponse:
    cache_key, metadata = conversation_cache_key(cluster, config)
    cache_path = cache_dir / f"{cache_key}.yml"
    system_message = {"role": "system", "content": SYSTEM_PROMPT}
    cached = [] if refresh_cache else load_cache(cache_path, metadata)
    if not cached or cached[0] != system_message:
        cached = []
    messages = [system_message]
    cursor = 1
    current_topics: dict[str, str] = {}
    provisional: dict[str, list[str]] = {}
    for course in cluster.courses:
        user_message = {"role": "user", "content": course_prompt(course, current_topics)}
        parsed, next_cursor = _cached_response(cached, cursor, user_message, CourseTopicsResponse)
        if parsed is None:
            LOGGER.info("Cluster %s: analyzing course %s", cluster.cluster_id, course.course_id)
            parsed = call_structured(
                client,
                messages + [user_message],
                CourseTopicsResponse,
                config,
                retry,
                sleep=sleep,
                random_uniform=random_uniform,
            )
            messages.extend([
                user_message,
                {"role": "assistant", "content": parsed.model_dump_json()},
            ])
            cached = list(messages)
            cursor = len(messages)
            write_cache(cache_path, cache_key, metadata, messages)
        else:
            LOGGER.info("Cluster %s: reusing cached course %s", cluster.cluster_id, course.course_id)
            messages.extend([user_message, cached[cursor + 1]])
            cursor = next_cursor
        current_topics = topics_mapping(parsed)
        provisional[course.course_id] = sorted(parsed.covered_topic_keys)

    user_message = {
        "role": "user",
        "content": final_prompt(cluster, current_topics, provisional),
    }
    final, _ = _cached_response(cached, cursor, user_message, FinalClusterResponse)
    if final is not None:
        try:
            validate_final_response(cluster, final)
        except ValueError:
            final = None
    if final is None:
        LOGGER.info("Cluster %s: generating final restructuring proposal", cluster.cluster_id)
        final = call_structured(
            client,
            messages + [user_message],
            FinalClusterResponse,
            config,
            retry,
            validator=lambda response: validate_final_response(cluster, response),
            sleep=sleep,
            random_uniform=random_uniform,
        )
        messages.extend([
            user_message,
            {"role": "assistant", "content": final.model_dump_json()},
        ])
        write_cache(cache_path, cache_key, metadata, messages)
    else:
        LOGGER.info("Cluster %s: reusing cached final proposal", cluster.cluster_id)
    return final


def create_openai_client(config: ModelConfig, request_timeout: float) -> Any:
    try:
        from openai import OpenAI
    except ImportError as error:
        raise RuntimeError("The 'openai' package is required; install requirements.txt") from error
    return OpenAI(
        api_key=os.environ.get("OPENAI_API_KEY"),
        base_url=config.endpoint,
        timeout=request_timeout,
        max_retries=0,
    )


def create_attempt_directory(output_root: pathlib.Path, now: datetime | None = None) -> pathlib.Path:
    timestamp = (now or datetime.now()).strftime("%Y-%m-%d-%H-%M")
    output_dir = output_root / f"attempt-{timestamp}"
    try:
        output_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError as error:
        raise ValueError(f"Attempt directory already exists: {output_dir}") from error
    return output_dir


def run_restructuring(
    input_path: pathlib.Path,
    config: ModelConfig,
    retry: RetryConfig,
    *,
    cluster_ids: tuple[int, ...] = (),
    cluster_name_regexes: tuple[str, ...] = (),
    refresh_cache: bool = False,
    request_timeout: float = 120.0,
    cache_dir: pathlib.Path | None = None,
    output_root: pathlib.Path | None = None,
    client: Any | None = None,
    now: datetime | None = None,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> pathlib.Path:
    clusters = select_clusters(load_clusters(input_path), cluster_ids, cluster_name_regexes)
    output_dir = create_attempt_directory(
        output_root or REPOSITORY_ROOT / "data" / "restructuring",
        now,
    )
    resolved_client = client or create_openai_client(config, request_timeout)
    resolved_cache_dir = cache_dir or REPOSITORY_ROOT / "data" / ".cache"
    for cluster in clusters:
        LOGGER.info("Processing cluster %s: %s", cluster.cluster_id, cluster.name)
        response = process_cluster(
            cluster,
            resolved_client,
            config,
            retry,
            resolved_cache_dir,
            refresh_cache=refresh_cache,
            sleep=sleep,
            random_uniform=random_uniform,
        )
        write_cluster_artifacts(output_dir, cluster, response)
    return output_dir

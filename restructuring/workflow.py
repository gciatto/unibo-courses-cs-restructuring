from __future__ import annotations

import json
import logging
import os
import pathlib
import random
import string
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, TypeVar

from pydantic import BaseModel, ValidationError

from restructuring.io import (
    DEFAULT_SYLLABUS_SECTION_KEYS,
    REPOSITORY_ROOT,
    conversation_cache_key,
    load_cache,
    load_clusters,
    normalize_syllabus_section_keys,
    select_clusters,
    validate_plantuml,
    write_cache,
    write_cluster_topics,
    write_course_topics,
    write_plantuml,
)
from restructuring.models import (
    ClusterInput,
    CourseInput,
    CourseTopicsResponse,
    ModelConfig,
    PlantUMLResponse,
    RetryConfig,
)

LOGGER = logging.getLogger(__name__)

PROMPTS_DIR = pathlib.Path(__file__).resolve().parent / "prompts"


def _load_prompt(name: str) -> str:
    return (PROMPTS_DIR / name).read_text(encoding="utf-8").strip()


SYSTEM_PROMPT = _load_prompt("system.txt")
COURSE_PROMPT = string.Template(_load_prompt("course.txt"))
PLANTUML_PROMPT = string.Template(_load_prompt("plantuml.txt"))
PLANTUML_REPAIR_PROMPT = string.Template(_load_prompt("plantuml_repair.txt"))
PROMPT_SYLLABUS_SECTION_KEYS = DEFAULT_SYLLABUS_SECTION_KEYS
TOPIC_CONVERSATION_MODES = ("stateless", "full")

T = TypeVar("T", bound=BaseModel)


class ResponseValidationError(ValueError):
    pass


class PlantUMLRenderError(RuntimeError):
    pass


class PlantUMLSyntaxError(ValueError):
    pass


@dataclass
class ClusterTopicState:
    topics: dict[str, str]
    memberships: dict[str, list[str]]


@dataclass
class ClusterConversation:
    state: ClusterTopicState
    messages: list[dict[str, str]]
    cached_messages: list[dict[str, str]]
    cursor: int
    cache_path: pathlib.Path
    cache_key: str
    cache_metadata: dict[str, Any]


def course_syllabus_markdown(course: CourseInput) -> str:
    lines = [f"# {course.title}" if course.title else f"# {course.course_id}"]
    for heading, text in course.syllabus_sections:
        lines.extend(["", f"## {heading}", "", text])
    return "\n".join(lines).strip()


def course_prompt(course: CourseInput, current_topics: dict[str, str]) -> str:
    return COURSE_PROMPT.substitute(
        course_id=course.course_id,
        course_title=course.title,
        current_topics=json.dumps(
            current_topics, ensure_ascii=False, sort_keys=True
        ),
        syllabus_markdown=course_syllabus_markdown(course),
    )


def plantuml_prompt(
    cluster: ClusterInput,
    topics: dict[str, str],
    memberships: dict[str, list[str]],
) -> str:
    old_courses = [
        {"id": course.course_id, "title": course.title}
        for course in cluster.courses
    ]
    return PLANTUML_PROMPT.substitute(
        cluster_id=cluster.cluster_id,
        cluster_name=cluster.name,
        current_topics=json.dumps(
            topics, ensure_ascii=False, sort_keys=True
        ),
        course_memberships=json.dumps(
            memberships, ensure_ascii=False, sort_keys=True
        ),
        old_courses=json.dumps(
            old_courses, ensure_ascii=False, sort_keys=True
        ),
    )


def plantuml_repair_prompt(plantuml: str, error: Exception) -> str:
    return PLANTUML_REPAIR_PROMPT.substitute(
        validation_error=f"{error.__class__.__name__}: {error}",
        previous_plantuml=plantuml,
    )


def _is_retryable(error: Exception) -> bool:
    if isinstance(error, (ValidationError, ResponseValidationError, PlantUMLRenderError)):
        return True
    status_code = getattr(error, "status_code", None)
    if status_code in {408, 409, 429} or (
        isinstance(status_code, int) and status_code >= 500
    ):
        return True
    return error.__class__.__name__ in {
        "RateLimitError",
        "APITimeoutError",
        "APIConnectionError",
        "InternalServerError",
    }


def call_with_backoff(
    operation: Callable[[], T],
    retry: RetryConfig,
    *,
    operation_name: str = "operation",
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> T:
    for attempt in range(retry.max_retries + 1):
        try:
            return operation()
        except Exception as error:
            if attempt >= retry.max_retries or not _is_retryable(error):
                LOGGER.error(
                    "%s failed permanently after %d attempt(s): %s: %s",
                    operation_name,
                    attempt + 1,
                    error.__class__.__name__,
                    error,
                )
                raise
            ceiling = min(retry.max_backoff, retry.initial_backoff * (2**attempt))
            delay = random_uniform(ceiling / 2, ceiling) if ceiling > 0 else 0
            LOGGER.warning(
                "%s attempt %d/%d failed with retryable %s: %s; "
                "next attempt starts in %.2f seconds",
                operation_name,
                attempt + 1,
                retry.max_retries + 1,
                error.__class__.__name__,
                error,
                delay,
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
    operation_name: str,
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
                raise ResponseValidationError(
                    "Model returned neither parsed output nor text"
                )
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

    LOGGER.info(
        "%s: submitting %d message(s) to model=%s endpoint=%s "
        "response_model=%s",
        operation_name,
        len(messages),
        config.model,
        config.endpoint,
        response_model.__name__,
    )
    return call_with_backoff(
        operation,
        retry,
        operation_name=operation_name,
        sleep=sleep,
        random_uniform=random_uniform,
    )


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


def apply_topic_response(
    cluster: ClusterInput,
    course_index: int,
    state: ClusterTopicState,
    response: CourseTopicsResponse,
) -> tuple[ClusterTopicState, set[str], set[str]]:
    course = cluster.courses[course_index]
    allowed_previous_ids = {
        item.course_id for item in cluster.courses[:course_index]
    }
    topics = dict(state.topics)
    memberships = {
        course_id: list(topic_keys)
        for course_id, topic_keys in state.memberships.items()
    }
    touched_topic_keys: set[str] = set()
    explicitly_updated_courses: set[str] = set()
    changed_descriptions: set[str] = set()

    for diff_index, diff in enumerate(response.topic_diffs, start=1):
        diff_topic_keys = set(diff.remove_topic_keys) | {
            topic.key for topic in diff.upsert_topics
        }
        conflicts = sorted(touched_topic_keys & diff_topic_keys)
        if conflicts:
            raise ValueError(
                f"topic diff {diff_index} conflicts with earlier diffs for keys {conflicts}"
            )
        touched_topic_keys.update(diff_topic_keys)
        missing_removals = sorted(set(diff.remove_topic_keys) - set(topics))
        if missing_removals:
            raise ValueError(
                f"topic diff {diff_index} removes unknown keys {missing_removals}"
            )
        for key in diff.remove_topic_keys:
            del topics[key]
        for topic in diff.upsert_topics:
            description = topic.description.strip()
            if topics.get(topic.key) != description:
                changed_descriptions.add(topic.key)
            topics[topic.key] = description
        for update in diff.course_topic_updates:
            if update.course_id not in allowed_previous_ids:
                raise ValueError(
                    f"topic diff {diff_index} updates course {update.course_id!r}; "
                    "only previously processed courses may be updated"
                )
            if update.course_id in explicitly_updated_courses:
                raise ValueError(
                    f"course {update.course_id!r} is updated by multiple topic diffs"
                )
            memberships[update.course_id] = sorted(update.topic_keys)
            explicitly_updated_courses.add(update.course_id)

    memberships[course.course_id] = sorted(response.covered_topic_keys)
    removed_keys = set(state.topics) - set(topics)
    dangling = {
        assigned_course_id: sorted(set(topic_keys) & removed_keys)
        for assigned_course_id, topic_keys in memberships.items()
        if set(topic_keys) & removed_keys
    }
    if dangling:
        raise ValueError(
            "removed topic keys remain assigned; explicit replacement assignments "
            f"are required: {dangling}"
        )
    for assigned_course_id, topic_keys in memberships.items():
        unknown = sorted(set(topic_keys) - set(topics))
        if unknown:
            raise ValueError(
                f"course {assigned_course_id!r} references unknown topics {unknown}"
            )

    rewritten_courses = set(explicitly_updated_courses)
    rewritten_courses.add(course.course_id)
    for assigned_course_id, topic_keys in memberships.items():
        if set(topic_keys) & changed_descriptions:
            rewritten_courses.add(assigned_course_id)
    return (
        ClusterTopicState(topics=topics, memberships=memberships),
        rewritten_courses,
        changed_descriptions,
    )


def _write_incremental_artifacts(
    output_dir: pathlib.Path,
    cluster: ClusterInput,
    state: ClusterTopicState,
    rewritten_course_ids: set[str],
    changed_descriptions: set[str],
) -> None:
    cluster_path = write_cluster_topics(output_dir, cluster, state.topics)
    LOGGER.info(
        "Cluster %s (%s): wrote canonical topic dictionary path=%s topics=%d",
        cluster.cluster_id,
        cluster.name,
        cluster_path,
        len(state.topics),
    )
    courses = {course.course_id: course for course in cluster.courses}
    for course_id in sorted(rewritten_course_ids, key=lambda value: (value.casefold(), value)):
        path = write_course_topics(
            output_dir,
            cluster,
            courses[course_id],
            state.topics,
            state.memberships[course_id],
        )
        reason = (
            "assignment and/or referenced description changed"
            if changed_descriptions
            else "assignment changed"
        )
        LOGGER.info(
            "Cluster %s (%s): wrote course topic assignment path=%s course=%s "
            "assigned_topics=%d reason=%s",
            cluster.cluster_id,
            cluster.name,
            path,
            course_id,
            len(state.memberships[course_id]),
            reason,
        )


def process_cluster_topics(
    cluster: ClusterInput,
    client: Any,
    config: ModelConfig,
    retry: RetryConfig,
    cache_dir: pathlib.Path,
    output_dir: pathlib.Path,
    *,
    syllabus_section_keys: tuple[str, ...] = PROMPT_SYLLABUS_SECTION_KEYS,
    topic_conversation_mode: str = "stateless",
    refresh_cache: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> ClusterConversation:
    if topic_conversation_mode not in TOPIC_CONVERSATION_MODES:
        raise ValueError(
            f"Unknown topic conversation mode {topic_conversation_mode!r}; "
            f"expected one of {TOPIC_CONVERSATION_MODES}"
        )
    normalized_section_keys = normalize_syllabus_section_keys(syllabus_section_keys)
    cache_key, metadata = conversation_cache_key(
        cluster,
        config,
        syllabus_section_keys=normalized_section_keys,
        topic_conversation_mode=topic_conversation_mode,
    )
    cache_path = cache_dir / f"{cache_key}.yml"
    system_message = {"role": "system", "content": SYSTEM_PROMPT}
    cached = [] if refresh_cache else load_cache(cache_path, metadata)
    if refresh_cache:
        LOGGER.info(
            "Cluster %s (%s): cache refresh requested; ignoring path=%s key=%s",
            cluster.cluster_id,
            cluster.name,
            cache_path,
            cache_key,
        )
    elif cached and cached[0] == system_message:
        LOGGER.info(
            "Cluster %s (%s): loaded cache path=%s key=%s messages=%d",
            cluster.cluster_id,
            cluster.name,
            cache_path,
            cache_key,
            len(cached),
        )
    else:
        if cached:
            LOGGER.warning(
                "Cluster %s (%s): cache system prompt mismatch; discarding "
                "messages=%d path=%s key=%s",
                cluster.cluster_id,
                cluster.name,
                len(cached),
                cache_path,
                cache_key,
            )
        else:
            LOGGER.info(
                "Cluster %s (%s): cache miss path=%s key=%s",
                cluster.cluster_id,
                cluster.name,
                cache_path,
                cache_key,
            )
        cached = []

    messages = [system_message]
    cursor = 1
    state = ClusterTopicState(topics={}, memberships={})
    initial_cluster_path = write_cluster_topics(output_dir, cluster, state.topics)
    LOGGER.info(
        "Cluster %s (%s): initialized incremental topic artifact before course "
        "analysis path=%s topics=0",
        cluster.cluster_id,
        cluster.name,
        initial_cluster_path,
    )
    total_courses = len(cluster.courses)
    for course_index, course in enumerate(cluster.courses):
        ordinal = course_index + 1
        user_message = {"role": "user", "content": course_prompt(course, state.topics)}
        parsed, next_cursor = _cached_response(
            cached, cursor, user_message, CourseTopicsResponse
        )
        next_state: ClusterTopicState | None = None
        rewritten_courses: set[str] = set()
        changed_descriptions: set[str] = set()
        if parsed is not None:
            try:
                next_state, rewritten_courses, changed_descriptions = apply_topic_response(
                    cluster, course_index, state, parsed
                )
            except ValueError as error:
                LOGGER.warning(
                    "Cluster %s (%s), course %s (%d/%d): cached response is "
                    "invalid for reconstructed state and stale suffix will be replaced: %s",
                    cluster.cluster_id,
                    cluster.name,
                    course.course_id,
                    ordinal,
                    total_courses,
                    error,
                )
                parsed = None
        if parsed is None:
            if cursor < len(cached):
                LOGGER.info(
                    "Cluster %s (%s), course %s (%d/%d): truncating stale cache "
                    "suffix at message=%d discarded_messages=%d",
                    cluster.cluster_id,
                    cluster.name,
                    course.course_id,
                    ordinal,
                    total_courses,
                    cursor,
                    len(cached) - cursor,
                )
                cached = cached[:cursor]
            request_messages = (
                messages + [user_message]
                if topic_conversation_mode == "full"
                else [system_message, user_message]
            )
            operation_name = (
                f"cluster={cluster.cluster_id} course={course.course_id} "
                f"topic-extraction {ordinal}/{total_courses}"
            )
            parsed = call_structured(
                client,
                request_messages,
                CourseTopicsResponse,
                config,
                retry,
                operation_name=operation_name,
                validator=lambda response, index=course_index, prior=state: (
                    apply_topic_response(cluster, index, prior, response)
                ),
                sleep=sleep,
                random_uniform=random_uniform,
            )
            next_state, rewritten_courses, changed_descriptions = apply_topic_response(
                cluster, course_index, state, parsed
            )
            assistant_message = {
                "role": "assistant",
                "content": parsed.model_dump_json(),
            }
            messages.extend([user_message, assistant_message])
            cached = list(messages)
            cursor = len(messages)
            write_cache(cache_path, cache_key, metadata, messages)
            LOGGER.info(
                "Cluster %s (%s), course %s (%d/%d): accepted model response "
                "diffs=%d covered_topics=%d resulting_topics=%d "
                "rewritten_courses=%d cache_messages=%d",
                cluster.cluster_id,
                cluster.name,
                course.course_id,
                ordinal,
                total_courses,
                len(parsed.topic_diffs),
                len(parsed.covered_topic_keys),
                len(next_state.topics),
                len(rewritten_courses),
                len(messages),
            )
        else:
            LOGGER.info(
                "Cluster %s (%s), course %s (%d/%d): cache hit at messages=%d-%d "
                "diffs=%d covered_topics=%d",
                cluster.cluster_id,
                cluster.name,
                course.course_id,
                ordinal,
                total_courses,
                cursor,
                next_cursor - 1,
                len(parsed.topic_diffs),
                len(parsed.covered_topic_keys),
            )
            messages.extend([user_message, cached[cursor + 1]])
            cursor = next_cursor
        assert next_state is not None
        state = next_state
        _write_incremental_artifacts(
            output_dir,
            cluster,
            state,
            rewritten_courses,
            changed_descriptions,
        )

    LOGGER.info(
        "Cluster %s (%s): topic phase complete courses=%d topics=%d "
        "course_artifacts=%d cluster_artifact=%s",
        cluster.cluster_id,
        cluster.name,
        total_courses,
        len(state.topics),
        len(state.memberships),
        output_dir / f"topics-of-cluster-{cluster.cluster_id}.yml",
    )
    return ClusterConversation(
        state=state,
        messages=messages,
        cached_messages=cached,
        cursor=cursor,
        cache_path=cache_path,
        cache_key=cache_key,
        cache_metadata=metadata,
    )


def create_openai_client(config: ModelConfig, request_timeout: float) -> Any:
    try:
        from openai import OpenAI
    except ImportError as error:
        raise RuntimeError(
            "The 'openai' package is required; install requirements.txt"
        ) from error
    LOGGER.info(
        "Creating OpenAI client endpoint=%s model=%s timeout=%.1fs sdk_retries=0",
        config.endpoint,
        config.model,
        request_timeout,
    )
    return OpenAI(
        api_key=os.environ.get("OPENAI_API_KEY"),
        base_url=config.endpoint,
        timeout=request_timeout,
        max_retries=0,
    )


def create_plantuml_renderer() -> Any:
    try:
        from plantumlcli import RemotePlantuml
    except ImportError as error:
        raise RuntimeError(
            "The 'plantumlcli' package is required; install requirements.txt"
        ) from error
    LOGGER.info(
        "Creating remote PlantUML renderer using plantumlcli configuration "
        "PLANTUML_HOST=%s",
        os.environ.get("PLANTUML_HOST", "<plantumlcli default>"),
    )
    return RemotePlantuml.autoload()


def render_plantuml_svg(
    plantuml: str,
    destination: pathlib.Path,
    retry: RetryConfig,
    *,
    renderer: Any | None = None,
    operation_name: str = "PlantUML remote SVG rendering",
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> None:
    try:
        resolved_renderer = renderer or create_plantuml_renderer()
    except Exception as error:
        raise PlantUMLRenderError(
            f"Could not initialize the PlantUML renderer: {error}"
        ) from error
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.stem}.tmp.svg")

    def operation() -> None:
        try:
            temporary.unlink(missing_ok=True)
            resolved_renderer.dump(str(temporary), "svg", plantuml)
            svg = temporary.read_text(encoding="utf-8")
        except Exception as error:
            temporary.unlink(missing_ok=True)
            raise PlantUMLRenderError(
                f"PlantUML remote rendering failed: {error}"
            ) from error
        normalized = svg.casefold()
        error_markers = ("syntax error", "error line", "[from string")
        if "<svg" not in normalized or any(
            marker in normalized for marker in error_markers
        ):
            temporary.unlink(missing_ok=True)
            raise PlantUMLSyntaxError(
                "PlantUML server returned an invalid or syntax-error SVG"
            )
        temporary.replace(destination)

    call_with_backoff(
        operation,
        retry,
        operation_name=operation_name,
        sleep=sleep,
        random_uniform=random_uniform,
    )
    LOGGER.info(
        "%s succeeded path=%s bytes=%d",
        operation_name,
        destination,
        destination.stat().st_size,
    )


def generate_cluster_plantuml(
    cluster: ClusterInput,
    conversation: ClusterConversation,
    client: Any,
    config: ModelConfig,
    retry: RetryConfig,
    output_dir: pathlib.Path,
    *,
    topic_conversation_mode: str,
    renderer: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> bool:
    initial_user = {
        "role": "user",
        "content": plantuml_prompt(
            cluster,
            conversation.state.topics,
            conversation.state.memberships,
        ),
    }
    plantuml_messages = (
        list(conversation.messages)
        if topic_conversation_mode == "full"
        else [conversation.messages[0]]
    )
    user_message = initial_user
    cursor = conversation.cursor
    last_error: Exception | None = None

    for generation_attempt in range(retry.max_retries + 1):
        parsed, next_cursor = _cached_response(
            conversation.cached_messages,
            cursor,
            user_message,
            PlantUMLResponse,
        )
        if parsed is None:
            if cursor < len(conversation.cached_messages):
                LOGGER.info(
                    "Cluster %s (%s): PlantUML cache mismatch at message=%d; "
                    "discarding stale_messages=%d",
                    cluster.cluster_id,
                    cluster.name,
                    cursor,
                    len(conversation.cached_messages) - cursor,
                )
                conversation.cached_messages = conversation.cached_messages[:cursor]
            try:
                parsed = call_structured(
                    client,
                    plantuml_messages + [user_message],
                    PlantUMLResponse,
                    config,
                    retry,
                    operation_name=(
                        f"cluster={cluster.cluster_id} PlantUML-generation "
                        f"{generation_attempt + 1}/{retry.max_retries + 1}"
                    ),
                    sleep=sleep,
                    random_uniform=random_uniform,
                )
            except Exception as error:
                plantuml_path = (
                    output_dir
                    / f"restructure-proposal-for-cluster-{cluster.cluster_id}.puml"
                )
                LOGGER.error(
                    "Cluster %s (%s): PlantUML LLM generation failed after "
                    "configured request retries at generation_attempt=%d/%d; "
                    "topic YAML remains definitive, existing_puml=%s, "
                    "continuing without SVG; error=%s: %s",
                    cluster.cluster_id,
                    cluster.name,
                    generation_attempt + 1,
                    retry.max_retries + 1,
                    plantuml_path if plantuml_path.exists() else "<none>",
                    error.__class__.__name__,
                    error,
                )
                return False
            assistant_message = {
                "role": "assistant",
                "content": parsed.model_dump_json(),
            }
            conversation.messages.extend([user_message, assistant_message])
            conversation.cached_messages = list(conversation.messages)
            cursor = len(conversation.messages)
            write_cache(
                conversation.cache_path,
                conversation.cache_key,
                conversation.cache_metadata,
                conversation.messages,
            )
            LOGGER.info(
                "Cluster %s (%s): cached PlantUML generation attempt=%d "
                "cache_path=%s cache_messages=%d",
                cluster.cluster_id,
                cluster.name,
                generation_attempt + 1,
                conversation.cache_path,
                len(conversation.messages),
            )
        else:
            LOGGER.info(
                "Cluster %s (%s): reusing cached PlantUML generation attempt=%d "
                "messages=%d-%d",
                cluster.cluster_id,
                cluster.name,
                generation_attempt + 1,
                cursor,
                next_cursor - 1,
            )
            conversation.messages.extend(
                [user_message, conversation.cached_messages[cursor + 1]]
            )
            cursor = next_cursor

        plantuml_messages.extend(
            [
                user_message,
                {"role": "assistant", "content": parsed.model_dump_json()},
            ]
        )
        plantuml_path = write_plantuml(
            output_dir, cluster.cluster_id, parsed.plantuml
        )
        LOGGER.info(
            "Cluster %s (%s): wrote PlantUML before validation path=%s "
            "generation_attempt=%d characters=%d",
            cluster.cluster_id,
            cluster.name,
            plantuml_path,
            generation_attempt + 1,
            len(parsed.plantuml),
        )
        try:
            validate_plantuml(cluster, parsed.plantuml)
            LOGGER.info(
                "Cluster %s (%s): local PlantUML validation succeeded "
                "generation_attempt=%d",
                cluster.cluster_id,
                cluster.name,
                generation_attempt + 1,
            )
            svg_path = plantuml_path.with_suffix(".svg")
            render_plantuml_svg(
                parsed.plantuml,
                svg_path,
                retry,
                renderer=renderer,
                operation_name=(
                    f"cluster={cluster.cluster_id} PlantUML remote SVG rendering"
                ),
                sleep=sleep,
                random_uniform=random_uniform,
            )
            return True
        except PlantUMLRenderError as error:
            LOGGER.error(
                "Cluster %s (%s): remote PlantUML rendering exhausted network/server "
                "retries; preserving puml=%s and omitting svg=%s; error=%s: %s",
                cluster.cluster_id,
                cluster.name,
                plantuml_path,
                plantuml_path.with_suffix(".svg"),
                error.__class__.__name__,
                error,
            )
            return False
        except (ValueError, PlantUMLSyntaxError) as error:
            last_error = error
            LOGGER.warning(
                "Cluster %s (%s): PlantUML validation failed generation_attempt=%d/%d "
                "puml=%s error=%s: %s",
                cluster.cluster_id,
                cluster.name,
                generation_attempt + 1,
                retry.max_retries + 1,
                plantuml_path,
                error.__class__.__name__,
                error,
            )
            if generation_attempt >= retry.max_retries:
                break
            user_message = {
                "role": "user",
                "content": plantuml_repair_prompt(parsed.plantuml, error),
            }

    plantuml_path = (
        output_dir / f"restructure-proposal-for-cluster-{cluster.cluster_id}.puml"
    )
    LOGGER.error(
        "Cluster %s (%s): PlantUML generation failed after %d attempt(s); "
        "preserving last puml=%s, omitting svg=%s, and continuing; final_error=%s: %s",
        cluster.cluster_id,
        cluster.name,
        retry.max_retries + 1,
        plantuml_path,
        plantuml_path.with_suffix(".svg"),
        last_error.__class__.__name__ if last_error else "unknown",
        last_error or "unknown validation failure",
    )
    return False


def create_attempt_directory(
    output_root: pathlib.Path,
    now: datetime | None = None,
) -> pathlib.Path:
    timestamp = (now or datetime.now()).strftime("%Y-%m-%d-%H-%M")
    output_dir = output_root / f"attempt-{timestamp}"
    try:
        output_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError as error:
        raise ValueError(f"Attempt directory already exists: {output_dir}") from error
    LOGGER.info("Created restructuring attempt directory path=%s", output_dir)
    return output_dir


def run_restructuring(
    input_path: pathlib.Path,
    config: ModelConfig,
    retry: RetryConfig,
    *,
    syllabus_section_keys: tuple[str, ...] = PROMPT_SYLLABUS_SECTION_KEYS,
    topic_conversation_mode: str = "stateless",
    cluster_ids: tuple[int, ...] = (),
    cluster_name_regexes: tuple[str, ...] = (),
    refresh_cache: bool = False,
    request_timeout: float = 120.0,
    cache_dir: pathlib.Path | None = None,
    output_root: pathlib.Path | None = None,
    client: Any | None = None,
    plantuml_renderer: Any | None = None,
    now: datetime | None = None,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> pathlib.Path:
    if topic_conversation_mode not in TOPIC_CONVERSATION_MODES:
        raise ValueError(
            f"Unknown topic conversation mode {topic_conversation_mode!r}; "
            f"expected one of {TOPIC_CONVERSATION_MODES}"
        )
    normalized_section_keys = normalize_syllabus_section_keys(syllabus_section_keys)
    clusters = select_clusters(
        load_clusters(input_path, normalized_section_keys),
        cluster_ids,
        cluster_name_regexes,
    )
    output_dir = create_attempt_directory(
        output_root or REPOSITORY_ROOT / "data" / "restructuring",
        now,
    )
    resolved_client = client or create_openai_client(config, request_timeout)
    resolved_cache_dir = cache_dir or REPOSITORY_ROOT / "data" / ".cache"
    total_courses = sum(len(cluster.courses) for cluster in clusters)
    LOGGER.info(
        "Starting restructuring run input=%s output=%s clusters=%d courses=%d "
        "model=%s endpoint=%s temperature=%s max_completion_tokens=%d "
        "topic_conversation_mode=%s syllabus_sections=%s cache_dir=%s "
        "refresh_cache=%s max_retries=%d backoff=%.2f..%.2fs",
        input_path.resolve(),
        output_dir,
        len(clusters),
        total_courses,
        config.model,
        config.endpoint,
        config.temperature,
        config.max_completion_tokens,
        topic_conversation_mode,
        ",".join(normalized_section_keys),
        resolved_cache_dir,
        refresh_cache,
        retry.max_retries,
        retry.initial_backoff,
        retry.max_backoff,
    )

    svg_successes = 0
    plantuml_failures = 0
    for cluster_index, cluster in enumerate(clusters, start=1):
        LOGGER.info(
            "Starting cluster %d/%d id=%s name=%s courses=%d",
            cluster_index,
            len(clusters),
            cluster.cluster_id,
            cluster.name,
            len(cluster.courses),
        )
        conversation = process_cluster_topics(
            cluster,
            resolved_client,
            config,
            retry,
            resolved_cache_dir,
            output_dir,
            syllabus_section_keys=normalized_section_keys,
            topic_conversation_mode=topic_conversation_mode,
            refresh_cache=refresh_cache,
            sleep=sleep,
            random_uniform=random_uniform,
        )
        LOGGER.info(
            "Cluster %s (%s): all definitive topic YAML artifacts are complete; "
            "starting isolated PlantUML phase",
            cluster.cluster_id,
            cluster.name,
        )
        if generate_cluster_plantuml(
            cluster,
            conversation,
            resolved_client,
            config,
            retry,
            output_dir,
            topic_conversation_mode=topic_conversation_mode,
            renderer=plantuml_renderer,
            sleep=sleep,
            random_uniform=random_uniform,
        ):
            svg_successes += 1
        else:
            plantuml_failures += 1

    LOGGER.info(
        "Restructuring run complete output=%s clusters_with_definitive_topics=%d "
        "courses_with_definitive_topics=%d svg_successes=%d plantuml_failures=%d "
        "exit_status=success",
        output_dir,
        len(clusters),
        total_courses,
        svg_successes,
        plantuml_failures,
    )
    return output_dir

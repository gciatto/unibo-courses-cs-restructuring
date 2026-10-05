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
    load_global_corpus,
    load_topic_artifacts,
    normalize_syllabus_section_keys,
    select_clusters,
    validate_restructuring_proposal,
    write_cache,
    write_cluster_topics,
    write_course_topics,
    write_global_topics,
    write_restructuring_proposal,
)
from restructuring.models import (
    ClusterInput,
    CourseInput,
    GlobalInput,
    CourseTopicsResponse,
    ModelConfig,
    RestructuringProposal,
    RetryConfig,
)

LOGGER = logging.getLogger(__name__)

PROMPTS_DIR = pathlib.Path(__file__).resolve().parent / "prompts"


def _load_prompt(name: str) -> str:
    return (PROMPTS_DIR / name).read_text(encoding="utf-8").strip()


SYSTEM_PROMPT = _load_prompt("system.txt")
COURSE_PROMPT = string.Template(_load_prompt("course.txt"))
PROPOSAL_PROMPT = string.Template(_load_prompt("proposal.txt"))
PROMPT_SYLLABUS_SECTION_KEYS = DEFAULT_SYLLABUS_SECTION_KEYS
TOPIC_CONVERSATION_MODES = ("stateless", "full")

T = TypeVar("T", bound=BaseModel)


class ResponseValidationError(ValueError):
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
    cache_writes_enabled: bool = True


def _corpus_label(corpus: ClusterInput | GlobalInput) -> tuple[str | int, str]:
    return (
        (corpus.cluster_id, corpus.name)
        if isinstance(corpus, ClusterInput) else ("global", "all-courses")
    )


def course_syllabus_markdown(course: CourseInput) -> str:
    lines: list[str] = []
    for heading, text in course.syllabus_sections:
        lines.extend(["", f"## {heading}", "", text])
    return "\n".join(lines).strip()


def course_prompt(
    course: CourseInput,
    current_topics: dict[str, str],
    prior_memberships: dict[str, list[str]],
) -> str:
    return COURSE_PROMPT.substitute(
        course_id=course.course_id,
        course_title=course.title,
        current_topics=json.dumps(
            current_topics, ensure_ascii=False, sort_keys=True
        ),
        prior_memberships=json.dumps(
            prior_memberships, ensure_ascii=False, sort_keys=True
        ),
        syllabus_markdown=course_syllabus_markdown(course),
    )


def proposal_prompt(
    cluster: ClusterInput | GlobalInput,
    topics: dict[str, str],
    memberships: dict[str, list[str]],
) -> str:
    old_courses = [
        {"id": course.course_id, "title": course.title}
        for course in cluster.courses
    ]
    return PROPOSAL_PROMPT.substitute(
        analysis_metadata=json.dumps(
            {"mode": "cluster", "cluster_id": cluster.cluster_id, "cluster_name": cluster.name}
            if isinstance(cluster, ClusterInput) else {"mode": "all-courses"},
            ensure_ascii=False,
            sort_keys=True,
        ),
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


def _is_retryable(error: Exception) -> bool:
    if isinstance(error, (ValidationError, ResponseValidationError)):
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
    cluster: ClusterInput | GlobalInput,
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
    cluster: ClusterInput | GlobalInput,
    state: ClusterTopicState,
    rewritten_course_ids: set[str],
    changed_descriptions: set[str],
) -> None:
    cluster_path = (
        write_cluster_topics(output_dir, cluster, state.topics)
        if isinstance(cluster, ClusterInput)
        else write_global_topics(output_dir, cluster, state.topics)
    )
    LOGGER.info(
        "Cluster %s (%s): wrote canonical topic dictionary path=%s topics=%d",
        getattr(cluster, "cluster_id", "global"),
        getattr(cluster, "name", "all-courses"),
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
            getattr(cluster, "cluster_id", "global"),
            getattr(cluster, "name", "all-courses"),
            path,
            course_id,
            len(state.memberships[course_id]),
            reason,
        )


def process_cluster_topics(
    cluster: ClusterInput | GlobalInput,
    client: Any,
    config: ModelConfig,
    retry: RetryConfig,
    cache_dir: pathlib.Path,
    output_dir: pathlib.Path,
    *,
    syllabus_section_keys: tuple[str, ...] = PROMPT_SYLLABUS_SECTION_KEYS,
    topic_conversation_mode: str = "stateless",
    refresh_cache: bool = False,
    reuse_topic_dirs: tuple[pathlib.Path, ...] = (),
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> ClusterConversation:
    if topic_conversation_mode not in TOPIC_CONVERSATION_MODES:
        raise ValueError(
            f"Unknown topic conversation mode {topic_conversation_mode!r}; "
            f"expected one of {TOPIC_CONVERSATION_MODES}"
        )
    if isinstance(cluster, GlobalInput) and reuse_topic_dirs:
        raise ValueError("--all-courses cannot be combined with --reuse-topics-from")
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
            getattr(cluster, "cluster_id", "global"),
            getattr(cluster, "name", "all-courses"),
            cache_path,
            cache_key,
        )
    elif cached and cached[0] == system_message:
        LOGGER.info(
            "Cluster %s (%s): loaded cache path=%s key=%s messages=%d",
            getattr(cluster, "cluster_id", "global"),
            getattr(cluster, "name", "all-courses"),
            cache_path,
            cache_key,
            len(cached),
        )
    else:
        if cached:
            LOGGER.warning(
                "Cluster %s (%s): cache system prompt mismatch; discarding "
                "messages=%d path=%s key=%s",
                getattr(cluster, "cluster_id", "global"),
                getattr(cluster, "name", "all-courses"),
                len(cached),
                cache_path,
                cache_key,
            )
        else:
            LOGGER.info(
                "Cluster %s (%s): cache miss path=%s key=%s",
                getattr(cluster, "cluster_id", "global"),
                getattr(cluster, "name", "all-courses"),
                cache_path,
                cache_key,
            )
        cached = []

    messages = [system_message]
    cursor = 1
    state = ClusterTopicState(topics={}, memberships={})
    for directory in reuse_topic_dirs:
        reused = load_topic_artifacts(directory, cluster)
        if reused is None:
            continue
        state = ClusterTopicState(*reused)
        _write_incremental_artifacts(
            output_dir,
            cluster,
            state,
            set(state.memberships),
            set(),
        )
        LOGGER.info(
            "Cluster %s (%s): reused complete topic artifacts from %s; "
            "skipping %d topic-extraction request(s)",
            *_corpus_label(cluster),
            directory,
            len(cluster.courses),
        )
        return ClusterConversation(
            state=state,
            messages=messages,
            cached_messages=[],
            cursor=cursor,
            cache_path=cache_path,
            cache_key=cache_key,
            cache_metadata=metadata,
            cache_writes_enabled=False,
        )
    initial_cluster_path = (
        write_cluster_topics(output_dir, cluster, state.topics)
        if isinstance(cluster, ClusterInput)
        else write_global_topics(output_dir, cluster, state.topics)
    )
    LOGGER.info(
        "Cluster %s (%s): initialized incremental topic artifact before course "
        "analysis path=%s topics=0",
        *_corpus_label(cluster),
        initial_cluster_path,
    )
    total_courses = len(cluster.courses)
    for course_index, course in enumerate(cluster.courses):
        ordinal = course_index + 1
        user_message = {"role": "user", "content": course_prompt(course, state.topics, state.memberships)}
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
                    *_corpus_label(cluster),
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
                    *_corpus_label(cluster),
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
                f"corpus={_corpus_label(cluster)[0]} course={course.course_id} "
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
                *_corpus_label(cluster),
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
                *_corpus_label(cluster),
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
        *_corpus_label(cluster),
        total_courses,
        len(state.topics),
        len(state.memberships),
        output_dir / (f"topics-of-cluster-{cluster.cluster_id}.yml" if isinstance(cluster, ClusterInput) else "topics-global.yml"),
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


def generate_cluster_proposal(
    cluster: ClusterInput | GlobalInput,
    conversation: ClusterConversation,
    client: Any,
    config: ModelConfig,
    retry: RetryConfig,
    output_dir: pathlib.Path,
    *,
    topic_conversation_mode: str,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> bool:
    user_message = {
        "role": "user",
        "content": proposal_prompt(
            cluster,
            conversation.state.topics,
            conversation.state.memberships,
        ),
    }
    request_messages = (
        list(conversation.messages)
        if topic_conversation_mode == "full"
        else [conversation.messages[0]]
    )
    cursor = conversation.cursor
    parsed, next_cursor = _cached_response(
        conversation.cached_messages,
        cursor,
        user_message,
        RestructuringProposal,
    )
    if parsed is not None:
        try:
            validate_restructuring_proposal(
                cluster,
                conversation.state.topics,
                conversation.state.memberships,
                parsed,
            )
        except ValueError as error:
            LOGGER.warning(
                "Cluster %s (%s): cached proposal is invalid; regenerating: %s",
                *_corpus_label(cluster),
                error,
            )
            parsed = None
    if parsed is None:
        if cursor < len(conversation.cached_messages):
            conversation.cached_messages = conversation.cached_messages[:cursor]
        try:
            parsed = call_structured(
                client,
                request_messages + [user_message],
                RestructuringProposal,
                config,
                retry,
                operation_name=f"corpus={_corpus_label(cluster)[0]} restructuring-proposal",
                validator=lambda proposal: validate_restructuring_proposal(
                    cluster,
                    conversation.state.topics,
                    conversation.state.memberships,
                    proposal,
                ),
                sleep=sleep,
                random_uniform=random_uniform,
            )
        except Exception as error:
            LOGGER.error(
                "Cluster %s (%s): restructuring proposal generation failed; "
                "topic YAML remains definitive; error=%s: %s",
                *_corpus_label(cluster),
                error.__class__.__name__,
                error,
            )
            return False
        assistant_message = {"role": "assistant", "content": parsed.model_dump_json()}
        conversation.messages.extend([user_message, assistant_message])
        conversation.cached_messages = list(conversation.messages)
        if conversation.cache_writes_enabled:
            write_cache(
                conversation.cache_path,
                conversation.cache_key,
                conversation.cache_metadata,
                conversation.messages,
            )
    else:
        conversation.messages.extend(
            [user_message, conversation.cached_messages[cursor + 1]]
        )

    yaml_path, mermaid_path = write_restructuring_proposal(
        output_dir,
        cluster,
        conversation.state.topics,
        conversation.state.memberships,
        parsed,
    )
    LOGGER.info(
        "Cluster %s (%s): wrote validated restructuring proposal yaml=%s mermaid=%s",
        *_corpus_label(cluster),
        yaml_path,
        mermaid_path,
    )
    return True


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
    reuse_topic_dirs: tuple[pathlib.Path, ...] = (),
    all_courses: bool = False,
    request_timeout: float = 120.0,
    cache_dir: pathlib.Path | None = None,
    output_root: pathlib.Path | None = None,
    client: Any | None = None,
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
    loaded_clusters = load_clusters(input_path, normalized_section_keys)
    clusters: list[ClusterInput | GlobalInput] = (
        [load_global_corpus(loaded_clusters)] if all_courses else select_clusters(
            loaded_clusters, cluster_ids, cluster_name_regexes
        )
    )
    if all_courses and (cluster_ids or cluster_name_regexes):
        raise ValueError("--all-courses cannot be combined with cluster selectors")
    if all_courses and reuse_topic_dirs:
        raise ValueError("--all-courses cannot be combined with --reuse-topics-from")
    missing_reuse_dirs = [path for path in reuse_topic_dirs if not path.is_dir()]
    if missing_reuse_dirs:
        raise ValueError(f"Topic reuse directories do not exist: {missing_reuse_dirs}")
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

    proposal_successes = 0
    proposal_failures = 0
    for cluster_index, cluster in enumerate(clusters, start=1):
        LOGGER.info(
            "Starting cluster %d/%d id=%s name=%s courses=%d",
            cluster_index,
            len(clusters),
            *_corpus_label(cluster),
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
            reuse_topic_dirs=reuse_topic_dirs,
            sleep=sleep,
            random_uniform=random_uniform,
        )
        LOGGER.info(
            "Cluster %s (%s): all definitive topic YAML artifacts are complete; "
            "starting isolated restructuring proposal phase",
            *_corpus_label(cluster),
        )
        if generate_cluster_proposal(
            cluster,
            conversation,
            resolved_client,
            config,
            retry,
            output_dir,
            topic_conversation_mode=topic_conversation_mode,
            sleep=sleep,
            random_uniform=random_uniform,
        ):
            proposal_successes += 1
        else:
            proposal_failures += 1

    LOGGER.info(
        "Restructuring run complete output=%s clusters_with_definitive_topics=%d "
        "courses_with_definitive_topics=%d proposal_successes=%d proposal_failures=%d "
        "exit_status=success",
        output_dir,
        len(clusters),
        total_courses,
        proposal_successes,
        proposal_failures,
    )
    return output_dir

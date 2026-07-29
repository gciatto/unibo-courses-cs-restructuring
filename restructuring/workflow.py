from __future__ import annotations

import json
import logging
import os
import pathlib
import random
import string
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

PROMPTS_DIR = pathlib.Path(__file__).resolve().parent / "prompts"


def _load_prompt(name: str) -> str:
    return (PROMPTS_DIR / name).read_text(encoding="utf-8").strip()


SYSTEM_PROMPT = _load_prompt("system.txt")
COURSE_PROMPT = string.Template(_load_prompt("course.txt"))
FINAL_PROMPT = string.Template(_load_prompt("final.txt"))


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
    return COURSE_PROMPT.substitute(
        course_id=course.course_id,
        course_title=course.title,
        current_topics=json.dumps(
            current_topics, ensure_ascii=False, sort_keys=True, indent=2
        ),
        syllabus_evidence=json.dumps(
            evidence, ensure_ascii=False, sort_keys=True, indent=2
        ),
    )


def final_prompt(
    cluster: ClusterInput,
    current_topics: dict[str, str],
    provisional_memberships: dict[str, list[str]],
) -> str:
    old_courses = [
        {"id": course.course_id, "title": course.title}
        for course in cluster.courses
    ]
    return FINAL_PROMPT.substitute(
        cluster_id=cluster.cluster_id,
        cluster_name=cluster.name,
        current_topics=json.dumps(
            current_topics, ensure_ascii=False, sort_keys=True, indent=2
        ),
        provisional_memberships=json.dumps(
            provisional_memberships, ensure_ascii=False, sort_keys=True, indent=2
        ),
        old_courses=json.dumps(
            old_courses, ensure_ascii=False, sort_keys=True, indent=2
        ),
    )


def _is_retryable(error: Exception) -> bool:
    if isinstance(error, (ValidationError, ResponseValidationError, PlantUMLRenderError)):
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


class PlantUMLRenderError(RuntimeError):
    pass


class PlantUMLSyntaxError(ValueError):
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
                "Retryable operation error %s; retrying in %.2f seconds (%d/%d)",
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


def create_plantuml_renderer() -> Any:
    try:
        from plantumlcli import RemotePlantuml
    except ImportError as error:
        raise RuntimeError(
            "The 'plantumlcli' package is required; install requirements.txt"
        ) from error
    return RemotePlantuml.autoload()


def render_plantuml_svg(
    plantuml: str,
    destination: pathlib.Path,
    retry: RetryConfig,
    *,
    renderer: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> None:
    resolved_renderer = renderer or create_plantuml_renderer()
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
        if "<svg" not in normalized or any(marker in normalized for marker in error_markers):
            temporary.unlink(missing_ok=True)
            raise PlantUMLSyntaxError(
                "PlantUML server returned an invalid or syntax-error SVG"
            )
        temporary.replace(destination)

    call_with_backoff(
        operation,
        retry,
        sleep=sleep,
        random_uniform=random_uniform,
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
    plantuml_renderer: Any | None = None,
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
        svg_path = (
            output_dir
            / f"restructure-proposal-for-cluster-{cluster.cluster_id}.svg"
        )
        LOGGER.info("Cluster %s: validating PlantUML and rendering SVG", cluster.cluster_id)
        render_plantuml_svg(
            response.plantuml,
            svg_path,
            retry,
            renderer=plantuml_renderer,
            sleep=sleep,
            random_uniform=random_uniform,
        )
        write_cluster_artifacts(output_dir, cluster, response)
    return output_dir

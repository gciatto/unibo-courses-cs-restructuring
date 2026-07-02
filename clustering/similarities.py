from __future__ import annotations

import logging
from typing import Any, Sequence

import numpy as np

from clustering.cache import (
    SIMILARITY_SCHEMA_VERSION,
    read_yaml_cache,
    similarity_cache_path,
    write_yaml_cache,
)
from clustering.course_io import CourseRecord
from clustering.sections import SECTION_COURSE_CONTENTS, SECTION_LEARNING_OUTCOMES, SECTION_NAMES, SECTION_READINGS, SECTION_TITLE


LOGGER = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {
    SECTION_TITLE: 0.15,
    SECTION_LEARNING_OUTCOMES: 0.35,
    SECTION_COURSE_CONTENTS: 0.35,
    SECTION_READINGS: 0.15,
}


def normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    normalized_input: dict[str, float] = {}
    for section_name in SECTION_NAMES:
        value = float(weights.get(section_name, 0.0))
        if value < 0:
            raise ValueError(f"Weight for {section_name!r} must be >= 0")
        normalized_input[section_name] = value

    total = sum(normalized_input.values())
    if total <= 0:
        raise ValueError("At least one section weight must be > 0")
    return {section_name: value / total for section_name, value in normalized_input.items()}


def cosine_similarity(vector_a: np.ndarray | None, vector_b: np.ndarray | None) -> float | None:
    if vector_a is None or vector_b is None:
        return None
    norm_a = float(np.linalg.norm(vector_a))
    norm_b = float(np.linalg.norm(vector_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return None
    value = float(np.dot(vector_a, vector_b) / (norm_a * norm_b))
    return max(-1.0, min(1.0, value))


def combine_section_similarities(
    section_similarities: dict[str, float | None],
    weights: dict[str, float],
) -> float:
    available = {
        section_name: similarity
        for section_name, similarity in section_similarities.items()
        if similarity is not None and weights.get(section_name, 0.0) > 0
    }
    if not available:
        return 0.0

    total_weight = sum(weights[section_name] for section_name in available)
    return sum(float(similarity) * weights[section_name] / total_weight for section_name, similarity in available.items())


def compute_pair_similarity(
    course_a: CourseRecord,
    course_b: CourseRecord,
    embeddings_by_sha: dict[str, dict[str, np.ndarray | None]],
    weights: dict[str, float],
    model_config_hash: str,
) -> dict[str, Any]:
    embeddings_a = embeddings_by_sha[course_a.sha256]
    embeddings_b = embeddings_by_sha[course_b.sha256]
    section_similarities = {
        section_name: cosine_similarity(embeddings_a.get(section_name), embeddings_b.get(section_name))
        for section_name in SECTION_NAMES
    }
    weighted_similarity = combine_section_similarities(section_similarities, weights)
    return {
        "schema_version": SIMILARITY_SCHEMA_VERSION,
        "model_config_hash": model_config_hash,
        "course_a": {
            "id": course_a.course_id,
            "path": str(course_a.path),
            "sha256": course_a.sha256,
        },
        "course_b": {
            "id": course_b.course_id,
            "path": str(course_b.path),
            "sha256": course_b.sha256,
        },
        "section_similarities": section_similarities,
        "weights": weights,
        "weighted_similarity": float(weighted_similarity),
        "distance": float(1.0 - weighted_similarity),
    }


def _valid_similarity_cache(
    payload: dict[str, Any],
    sha_a: str,
    sha_b: str,
    model_config_hash: str,
) -> bool:
    if payload.get("schema_version") != SIMILARITY_SCHEMA_VERSION:
        return False
    if payload.get("model_config_hash") != model_config_hash:
        return False
    course_a = payload.get("course_a")
    course_b = payload.get("course_b")
    if not isinstance(course_a, dict) or not isinstance(course_b, dict):
        return False
    cached_shas = {course_a.get("sha256"), course_b.get("sha256")}
    return cached_shas == {sha_a, sha_b}


def load_or_compute_pair_similarity(
    year_dir,
    course_a: CourseRecord,
    course_b: CourseRecord,
    embeddings_by_sha: dict[str, dict[str, np.ndarray | None]],
    weights: dict[str, float],
    model_config_hash: str,
    *,
    no_cache: bool = False,
    refresh: bool = False,
) -> dict[str, Any]:
    cache_path = similarity_cache_path(year_dir, course_a.sha256, course_b.sha256, model_config_hash)
    if not no_cache and not refresh:
        cached = read_yaml_cache(cache_path)
        if cached is not None and _valid_similarity_cache(cached, course_a.sha256, course_b.sha256, model_config_hash):
            return cached

    payload = compute_pair_similarity(course_a, course_b, embeddings_by_sha, weights, model_config_hash)
    if not no_cache:
        write_yaml_cache(cache_path, payload)
    return payload


def build_similarity_matrices(
    courses: Sequence[CourseRecord],
    embeddings_by_sha: dict[str, dict[str, np.ndarray | None]],
    weights: dict[str, float],
    model_config_hash: str,
    *,
    year_dir,
    no_cache: bool = False,
    refresh: bool = False,
) -> tuple[np.ndarray, np.ndarray, list[dict[str, Any]]]:
    count = len(courses)
    similarity_matrix = np.eye(count, dtype=float)
    distance_matrix = np.zeros((count, count), dtype=float)
    pair_payloads: list[dict[str, Any]] = []

    for index_a in range(count):
        for index_b in range(index_a + 1, count):
            payload = load_or_compute_pair_similarity(
                year_dir,
                courses[index_a],
                courses[index_b],
                embeddings_by_sha,
                weights,
                model_config_hash,
                no_cache=no_cache,
                refresh=refresh,
            )
            similarity = float(payload["weighted_similarity"])
            distance = float(payload["distance"])
            similarity_matrix[index_a, index_b] = similarity_matrix[index_b, index_a] = similarity
            distance_matrix[index_a, index_b] = distance_matrix[index_b, index_a] = distance
            pair_payloads.append(payload)

    return similarity_matrix, distance_matrix, pair_payloads

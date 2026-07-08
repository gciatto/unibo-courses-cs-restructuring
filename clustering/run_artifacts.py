from __future__ import annotations

import csv
import logging
import pathlib
from dataclasses import dataclass
from typing import Any

import numpy as np
import yaml

from clustering.algorithms import build_feature_matrix
from clustering.cache import embedding_cache_path, read_yaml_cache
from clustering.charts import generate_charts
from clustering.course_io import CourseRecord, load_course
from clustering.reports import build_cluster_summary, write_cluster_courses_short_yaml
from clustering.sections import SECTION_NAMES


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunArtifacts:
    run_dir: pathlib.Path
    courses: list[CourseRecord]
    similarity_matrix: np.ndarray
    distance_matrix: np.ndarray
    labels: np.ndarray
    algorithm: str
    agglomerative_linkage: str
    cluster_name_by_id: dict[int, str]
    features: np.ndarray | None


def _read_csv_rows(path: pathlib.Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def _load_course_from_run_row(row: dict[str, str]) -> CourseRecord:
    course_id = str(row.get("course_id") or "").strip()
    course_path = pathlib.Path(str(row.get("course_path") or ""))
    course_sha256 = str(row.get("course_sha256") or "").strip()
    course_title = str(row.get("course_title") or "").strip()

    raw: dict[str, Any] = {}
    sections: dict[str, str | None] = {}
    section_languages: dict[str, str | None] = {}
    if course_path.exists():
        try:
            loaded = load_course(course_path)
            raw = loaded.raw
            sections = loaded.sections
            section_languages = loaded.section_languages
        except Exception as error:
            LOGGER.warning("Could not load course metadata from %s: %s", course_path, error)

    return CourseRecord(
        course_id=course_id,
        path=course_path,
        sha256=course_sha256,
        title=course_title,
        sections=sections,
        section_languages=section_languages,
        raw=raw,
    )


def read_run_courses(run_dir: pathlib.Path) -> list[CourseRecord]:
    rows = _read_csv_rows(run_dir / "courses.csv")
    courses = [_load_course_from_run_row(row) for row in rows]
    if not courses:
        raise ValueError(f"No courses found in {run_dir / 'courses.csv'}")
    missing_ids = [index for index, course in enumerate(courses) if not course.course_id]
    if missing_ids:
        raise ValueError(f"Missing course_id in courses.csv rows: {missing_ids}")
    return courses


def read_run_matrix(path: pathlib.Path, course_ids: list[str]) -> np.ndarray:
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream)
        try:
            header = next(reader)
        except StopIteration as error:
            raise ValueError(f"Matrix CSV is empty: {path}") from error

        column_ids = header[1:]
        column_index = {course_id: index for index, course_id in enumerate(column_ids)}
        rows_by_id: dict[str, list[float]] = {}
        for row in reader:
            if not row:
                continue
            rows_by_id[row[0]] = [float(value) for value in row[1:]]

    missing_rows = [course_id for course_id in course_ids if course_id not in rows_by_id]
    missing_columns = [course_id for course_id in course_ids if course_id not in column_index]
    if missing_rows or missing_columns:
        raise ValueError(f"Matrix {path} does not cover all run courses")

    matrix = np.zeros((len(course_ids), len(course_ids)), dtype=float)
    for row_index, course_id in enumerate(course_ids):
        row = rows_by_id[course_id]
        for column_position, other_id in enumerate(course_ids):
            matrix[row_index, column_position] = row[column_index[other_id]]
    return matrix


def read_run_labels(run_dir: pathlib.Path, courses: list[CourseRecord]) -> tuple[np.ndarray, dict[int, str]]:
    rows = _read_csv_rows(run_dir / "clusters.csv")
    label_by_course_id: dict[str, int] = {}
    cluster_name_by_id: dict[int, str] = {}
    for row in rows:
        course_id = str(row.get("course_id") or "").strip()
        if not course_id:
            continue
        cluster_id = int(str(row.get("cluster") or "0"))
        label_by_course_id[course_id] = cluster_id
        cluster_name = str(row.get("cluster_name") or "").strip()
        if cluster_name:
            cluster_name_by_id.setdefault(cluster_id, cluster_name)

    missing = [course.course_id for course in courses if course.course_id not in label_by_course_id]
    if missing:
        raise ValueError(f"clusters.csv does not cover all run courses: {missing[:5]}")

    labels = np.asarray([label_by_course_id[course.course_id] for course in courses], dtype=int)
    return labels, cluster_name_by_id


def read_run_config(run_dir: pathlib.Path) -> dict[str, Any]:
    path = run_dir / "run_config.yml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _cluster_names_from_summary(run_dir: pathlib.Path) -> dict[int, str]:
    path = run_dir / "cluster_summary.yml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {}

    result: dict[int, str] = {}
    for raw_cluster_id, item in payload.items():
        if not isinstance(item, dict):
            continue
        try:
            cluster_id = int(raw_cluster_id)
        except (TypeError, ValueError):
            continue
        cluster_name = item.get("cluster_name")
        if isinstance(cluster_name, str) and cluster_name.strip():
            result[cluster_id] = cluster_name.strip()
    return result


def _infer_algorithm(run_dir: pathlib.Path, run_config: dict[str, Any]) -> str:
    clustering = run_config.get("clustering")
    if isinstance(clustering, dict):
        algorithm = clustering.get("algorithm")
        if isinstance(algorithm, str) and algorithm.strip():
            return algorithm.strip()

    for candidate in ("agglomerative", "hdbscan", "spectral", "kmeans"):
        if run_dir.name.endswith(f"-{candidate}") or f"-{candidate}-" in run_dir.name:
            return candidate
    return "unknown"


def _agglomerative_linkage(run_config: dict[str, Any]) -> str:
    arguments = run_config.get("arguments")
    if isinstance(arguments, dict):
        linkage = arguments.get("agglomerative_linkage")
        if isinstance(linkage, str) and linkage.strip():
            return linkage.strip()
    return "average"


def _read_cached_embeddings(course: CourseRecord, model_config_hash: str) -> dict[str, np.ndarray | None] | None:
    payload = read_yaml_cache(embedding_cache_path(course.path, course.sha256, model_config_hash))
    if payload is None:
        return None

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return None
    if metadata.get("course_sha256") != course.sha256:
        return None
    if metadata.get("model_config_hash") != model_config_hash:
        return None

    raw_embeddings = payload.get("embeddings")
    if not isinstance(raw_embeddings, dict):
        return None

    embeddings: dict[str, np.ndarray | None] = {}
    for section_name in SECTION_NAMES:
        value = raw_embeddings.get(section_name)
        embeddings[section_name] = None if value is None else np.asarray(value, dtype=float)
    return embeddings


def read_cached_feature_matrix(courses: list[CourseRecord], run_config: dict[str, Any]) -> np.ndarray | None:
    embedding = run_config.get("embedding")
    model_config_hash = embedding.get("model_config_hash") if isinstance(embedding, dict) else None
    if not isinstance(model_config_hash, str) or not model_config_hash.strip():
        return None

    weights_payload = run_config.get("weights")
    if not isinstance(weights_payload, dict):
        return None
    weights = {
        section_name: float(weights_payload.get(section_name, 0.0) or 0.0)
        for section_name in SECTION_NAMES
    }

    embeddings_by_sha: dict[str, dict[str, np.ndarray | None]] = {}
    embedding_dimension = 0
    for course in courses:
        embeddings = _read_cached_embeddings(course, model_config_hash)
        if embeddings is None:
            return None
        embeddings_by_sha[course.sha256] = embeddings
        first_vector = next((vector for vector in embeddings.values() if vector is not None), None)
        if first_vector is not None:
            embedding_dimension = int(first_vector.shape[0])

    if embedding_dimension <= 0:
        return None
    return build_feature_matrix(courses, embeddings_by_sha, weights, embedding_dimension)


def load_run_artifacts(run_dir: pathlib.Path) -> RunArtifacts:
    run_dir = run_dir.expanduser()
    if not run_dir.exists() or not run_dir.is_dir():
        raise ValueError(f"Experiment directory does not exist: {run_dir}")

    run_config = read_run_config(run_dir)
    courses = read_run_courses(run_dir)
    course_ids = [course.course_id for course in courses]
    similarity_matrix = read_run_matrix(run_dir / "similarity_matrix.csv", course_ids)
    distance_matrix = read_run_matrix(run_dir / "distance_matrix.csv", course_ids)
    labels, cluster_name_by_id = read_run_labels(run_dir, courses)
    cluster_name_by_id = {
        **_cluster_names_from_summary(run_dir),
        **cluster_name_by_id,
    }
    features = read_cached_feature_matrix(courses, run_config)

    return RunArtifacts(
        run_dir=run_dir,
        courses=courses,
        similarity_matrix=similarity_matrix,
        distance_matrix=distance_matrix,
        labels=labels,
        algorithm=_infer_algorithm(run_dir, run_config),
        agglomerative_linkage=_agglomerative_linkage(run_config),
        cluster_name_by_id=cluster_name_by_id,
        features=features,
    )


def regenerate_run_artifacts(run_dir: pathlib.Path) -> pathlib.Path:
    artifacts = load_run_artifacts(run_dir)
    summary = build_cluster_summary(artifacts.courses, artifacts.labels, artifacts.similarity_matrix)
    for cluster_id, cluster_name in artifacts.cluster_name_by_id.items():
        summary.setdefault(cluster_id, {})["cluster_name"] = cluster_name

    write_cluster_courses_short_yaml(
        artifacts.run_dir / "cluster_courses.short.yml",
        artifacts.courses,
        artifacts.labels,
        summary,
    )
    generate_charts(
        artifacts.run_dir,
        artifacts.courses,
        artifacts.similarity_matrix,
        artifacts.distance_matrix,
        artifacts.features,
        artifacts.labels,
        algorithm=artifacts.algorithm,
        agglomerative_linkage=artifacts.agglomerative_linkage,
        cluster_name_by_id=artifacts.cluster_name_by_id,
    )
    return artifacts.run_dir

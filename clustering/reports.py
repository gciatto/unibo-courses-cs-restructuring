from __future__ import annotations

import csv
import pathlib
import re
from collections import Counter
from typing import Any, Sequence

import numpy as np
import yaml

from clustering.algorithms import ClusterResult
from clustering.course_io import CourseRecord
from clustering.sections import SECTION_NAMES


STOPWORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "con",
    "degli",
    "della",
    "delle",
    "dello",
    "di",
    "for",
    "from",
    "gli",
    "il",
    "in",
    "la",
    "le",
    "lo",
    "of",
    "per",
    "that",
    "the",
    "this",
    "to",
    "un",
    "una",
    "with",
}


def write_run_config(path: pathlib.Path, config: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8")


def write_courses_csv(path: pathlib.Path, courses: Sequence[CourseRecord]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=["course_id", "course_path", "course_sha256", "course_title", "missing_sections"],
        )
        writer.writeheader()
        for course in courses:
            writer.writerow(
                {
                    "course_id": course.course_id,
                    "course_path": str(course.path),
                    "course_sha256": course.sha256,
                    "course_title": course.title,
                    "missing_sections": ";".join(section for section in SECTION_NAMES if not course.sections.get(section)),
                }
            )


def write_matrix_csv(path: pathlib.Path, courses: Sequence[CourseRecord], matrix: np.ndarray) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["course_id", *[course.course_id for course in courses]])
        for course, row in zip(courses, matrix):
            writer.writerow([course.course_id, *[f"{float(value):.8f}" for value in row]])


def cluster_indices(labels: np.ndarray) -> dict[int, list[int]]:
    result: dict[int, list[int]] = {}
    for index, label in enumerate(labels):
        result.setdefault(int(label), []).append(index)
    return result


def medoid_indices(similarity_matrix: np.ndarray, labels: np.ndarray) -> dict[int, int]:
    medoids: dict[int, int] = {}
    for cluster_id, indices in cluster_indices(labels).items():
        if len(indices) == 1:
            medoids[cluster_id] = indices[0]
            continue
        submatrix = similarity_matrix[np.ix_(indices, indices)]
        mean_similarity = submatrix.mean(axis=1)
        medoids[cluster_id] = indices[int(np.argmax(mean_similarity))]
    return medoids


def write_clusters_csv(
    path: pathlib.Path,
    courses: Sequence[CourseRecord],
    labels: np.ndarray,
    similarity_matrix: np.ndarray,
) -> None:
    medoids = medoid_indices(similarity_matrix, labels)
    summary = build_cluster_summary(courses, labels, similarity_matrix)
    cluster_names = {cluster_id: data["cluster_name"] for cluster_id, data in summary.items()}
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "course_id",
                "course_path",
                "course_sha256",
                "course_title",
                "cluster",
                "cluster_name",
                "is_noise",
                "similarity_to_medoid",
            ],
        )
        writer.writeheader()
        for index, course in enumerate(courses):
            cluster_id = int(labels[index])
            medoid_index = medoids[cluster_id]
            writer.writerow(
                {
                    "course_id": course.course_id,
                    "course_path": str(course.path),
                    "course_sha256": course.sha256,
                    "course_title": course.title,
                    "cluster": cluster_id,
                    "cluster_name": cluster_names.get(cluster_id, f"Cluster {cluster_id}"),
                    "is_noise": cluster_id == -1,
                    "similarity_to_medoid": f"{float(similarity_matrix[index, medoid_index]):.8f}",
                }
            )


def _keywords_for_courses(courses: Sequence[CourseRecord], indices: list[int], limit: int = 12) -> list[str]:
    counter: Counter[str] = Counter()
    for index in indices:
        course = courses[index]
        text = "\n".join(course.sections.get(section) or "" for section in SECTION_NAMES)
        for token in re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ0-9_'-]{3,}", text.casefold()):
            if token not in STOPWORDS:
                counter[token] += 1
    return [token for token, _ in counter.most_common(limit)]


def _strongest_pairs_for_cluster(
    courses: Sequence[CourseRecord],
    indices: list[int],
    similarity_matrix: np.ndarray,
    limit: int = 5,
) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for offset, index_a in enumerate(indices):
        for index_b in indices[offset + 1 :]:
            pairs.append(
                {
                    "course_a": courses[index_a].course_id,
                    "course_b": courses[index_b].course_id,
                    "similarity": float(similarity_matrix[index_a, index_b]),
                }
            )
    pairs.sort(key=lambda item: item["similarity"], reverse=True)
    return pairs[:limit]


def _first_non_empty_string(values: Sequence[str | None]) -> str | None:
    for value in values:
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


def _extract_cluster_name(cluster_id: int, summary_item: dict[str, Any]) -> str:
    if cluster_id == -1:
        return "Noise"
    keywords = [str(token).strip() for token in summary_item.get("top_keywords", []) if str(token).strip()]
    if keywords:
        return f"{' '.join(token.title() for token in keywords[:3])} ({cluster_id})"
    representative_titles = summary_item.get("representative_titles", [])
    if representative_titles:
        raw_title = str(representative_titles[0]).strip()
        words = [word for word in re.findall(r"[A-Za-zÀ-ÿ0-9]+", raw_title) if word]
        if words:
            return f"{' '.join(words[:4])} ({cluster_id})"
    return f"Cluster {cluster_id}"


def _slugify_teacher(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", ".", name.casefold()).strip(".")
    return cleaned


def _teacher_handle(teacher: Any) -> str | None:
    if not isinstance(teacher, dict):
        return None
    website = teacher.get("website")
    if isinstance(website, str):
        candidate = website.rstrip("/").split("/")[-1].strip()
        if candidate:
            return candidate
    raw_name = teacher.get("name")
    if not isinstance(raw_name, str):
        return None
    if "," in raw_name:
        surname, first_name = [part.strip() for part in raw_name.split(",", maxsplit=1)]
        if first_name and surname:
            return _slugify_teacher(f"{first_name}.{surname}")
    return _slugify_teacher(raw_name)


def _course_year(course: CourseRecord) -> int | None:
    year_value = course.raw.get("year") if isinstance(course.raw, dict) else None
    if isinstance(year_value, int):
        return year_value
    if isinstance(year_value, str) and year_value.isdigit():
        return int(year_value)
    parent_name = course.path.parent.name
    if parent_name.isdigit():
        return int(parent_name)
    return None


def _course_campi(course: CourseRecord) -> list[str]:
    raw_campi = course.raw.get("campi") if isinstance(course.raw, dict) else None
    if isinstance(raw_campi, list):
        values = sorted({str(campus).strip() for campus in raw_campi if str(campus).strip()})
        if values:
            return values

    inferred: set[str] = set()
    teachers = course.raw.get("teachers") if isinstance(course.raw, dict) else None
    if isinstance(teachers, list):
        for teacher in teachers:
            if not isinstance(teacher, dict):
                continue
            modules = teacher.get("modules")
            if not isinstance(modules, list):
                continue
            for module in modules:
                if not isinstance(module, dict):
                    continue
                campus = str(module.get("campus") or "").strip()
                if campus:
                    inferred.add(campus)
    return sorted(inferred)


def _course_programmes(course: CourseRecord) -> list[dict[str, str]]:
    programmes = course.raw.get("programmes") if isinstance(course.raw, dict) else None
    if not isinstance(programmes, list):
        return []
    result: list[dict[str, str]] = []
    for programme in programmes:
        if not isinstance(programme, dict):
            continue
        programme_id = str(programme.get("code") or programme.get("id") or "").strip()
        if not programme_id:
            continue
        name_value = programme.get("name")
        if isinstance(name_value, dict):
            programme_name = _first_non_empty_string(
                [
                    name_value.get("en") if isinstance(name_value.get("en"), str) else None,
                    name_value.get("it") if isinstance(name_value.get("it"), str) else None,
                ]
            )
        else:
            programme_name = str(name_value).strip() if name_value is not None else None
        result.append(
            {
                "id": programme_id,
                "name": programme_name or "",
            }
        )
    return result


def _course_cluster_entry(course: CourseRecord) -> dict[str, Any]:
    teachers = course.raw.get("teachers") if isinstance(course.raw, dict) else None
    teacher_handles = []
    if isinstance(teachers, list):
        teacher_handles = sorted({handle for handle in (_teacher_handle(item) for item in teachers) if handle})
    return {
        "path": str(course.path),
        "year": _course_year(course),
        "id": course.course_id,
        "name": course.title,
        "campi": _course_campi(course),
        "teachers": teacher_handles,
        "programmes": _course_programmes(course),
    }


def write_cluster_courses_yaml(
    path: pathlib.Path,
    courses: Sequence[CourseRecord],
    labels: np.ndarray,
    summary: dict[int, dict[str, Any]],
) -> None:
    clusters = cluster_indices(labels)
    payload: dict[str, dict[str, Any]] = {}
    for cluster_id in sorted(clusters):
        summary_item = summary[cluster_id]
        cluster_name = str(summary_item.get("cluster_name") or f"Cluster {cluster_id}")
        payload[cluster_name] = {
            "index": cluster_id,
            "size": summary_item.get("size"),
            "medoid_course_id": summary_item.get("medoid_course_id"),
            "representative_titles": summary_item.get("representative_titles", []),
            "top_keywords": summary_item.get("top_keywords", []),
            "strongest_pairs": summary_item.get("strongest_pairs", []),
            "courses": [_course_cluster_entry(courses[index]) for index in clusters[cluster_id]],
        }
    path.write_text(yaml.safe_dump(payload, sort_keys=True, allow_unicode=True), encoding="utf-8")


def build_cluster_summary(
    courses: Sequence[CourseRecord],
    labels: np.ndarray,
    similarity_matrix: np.ndarray,
) -> dict[int, dict[str, Any]]:
    medoids = medoid_indices(similarity_matrix, labels)
    summary: dict[int, dict[str, Any]] = {}
    for cluster_id, indices in sorted(cluster_indices(labels).items()):
        medoid_index = medoids[cluster_id]
        ranked_indices = sorted(indices, key=lambda index: similarity_matrix[index, medoid_index], reverse=True)
        summary_item = {
            "size": len(indices),
            "medoid_course_id": courses[medoid_index].course_id,
            "representative_titles": [courses[index].title for index in ranked_indices[:5] if courses[index].title],
            "top_keywords": _keywords_for_courses(courses, indices),
            "strongest_pairs": _strongest_pairs_for_cluster(courses, indices, similarity_matrix),
        }
        summary_item["cluster_name"] = _extract_cluster_name(cluster_id, summary_item)
        summary[cluster_id] = summary_item
    return summary


def write_cluster_summary(path: pathlib.Path, summary: dict[int, dict[str, Any]]) -> None:
    path.write_text(yaml.safe_dump(summary, sort_keys=True, allow_unicode=True), encoding="utf-8")


def write_nearest_neighbors_csv(
    path: pathlib.Path,
    courses: Sequence[CourseRecord],
    similarity_matrix: np.ndarray,
    top_n: int = 10,
) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "course_id",
                "course_title",
                "neighbor_rank",
                "neighbor_course_id",
                "neighbor_title",
                "similarity",
            ],
        )
        writer.writeheader()
        for index, course in enumerate(courses):
            candidates = [
                (other_index, float(similarity_matrix[index, other_index]))
                for other_index in range(len(courses))
                if other_index != index
            ]
            candidates.sort(key=lambda item: item[1], reverse=True)
            for rank, (neighbor_index, similarity) in enumerate(candidates[:top_n], start=1):
                neighbor = courses[neighbor_index]
                writer.writerow(
                    {
                        "course_id": course.course_id,
                        "course_title": course.title,
                        "neighbor_rank": rank,
                        "neighbor_course_id": neighbor.course_id,
                        "neighbor_title": neighbor.title,
                        "similarity": f"{similarity:.8f}",
                    }
                )


def write_top_pairs_csv(path: pathlib.Path, pair_payloads: Sequence[dict[str, Any]]) -> None:
    rows = sorted(pair_payloads, key=lambda payload: float(payload["weighted_similarity"]), reverse=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=["course_a", "course_b", "path_a", "path_b", "similarity", "distance"],
        )
        writer.writeheader()
        for payload in rows:
            course_a = payload["course_a"]
            course_b = payload["course_b"]
            writer.writerow(
                {
                    "course_a": course_a["id"],
                    "course_b": course_b["id"],
                    "path_a": course_a["path"],
                    "path_b": course_b["path"],
                    "similarity": f"{float(payload['weighted_similarity']):.8f}",
                    "distance": f"{float(payload['distance']):.8f}",
                }
            )


def write_reports(
    run_dir: pathlib.Path,
    courses: Sequence[CourseRecord],
    similarity_matrix: np.ndarray,
    distance_matrix: np.ndarray,
    cluster_result: ClusterResult,
    pair_payloads: Sequence[dict[str, Any]],
    run_config: dict[str, Any],
    *,
    nearest_neighbor_count: int = 10,
) -> dict[int, dict[str, Any]]:
    run_dir.mkdir(parents=True, exist_ok=True)
    write_run_config(run_dir / "run_config.yml", run_config)
    write_courses_csv(run_dir / "courses.csv", courses)
    write_matrix_csv(run_dir / "similarity_matrix.csv", courses, similarity_matrix)
    write_matrix_csv(run_dir / "distance_matrix.csv", courses, distance_matrix)
    summary = build_cluster_summary(courses, cluster_result.labels, similarity_matrix)
    write_clusters_csv(run_dir / "clusters.csv", courses, cluster_result.labels, similarity_matrix)
    write_cluster_summary(run_dir / "cluster_summary.yml", summary)
    write_cluster_courses_yaml(run_dir / "cluster_courses.yml", courses, cluster_result.labels, summary)
    write_nearest_neighbors_csv(run_dir / "nearest_neighbors.csv", courses, similarity_matrix, top_n=nearest_neighbor_count)
    write_top_pairs_csv(run_dir / "top_pairs.csv", pair_payloads)
    return summary

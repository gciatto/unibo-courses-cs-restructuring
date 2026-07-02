from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np

from clustering.course_io import CourseRecord
from clustering.sections import SECTION_NAMES


@dataclass(frozen=True)
class ClusterResult:
    labels: np.ndarray
    algorithm: str
    n_clusters: int | None
    cluster_count_scores: list[dict[str, float | int]] = field(default_factory=list)


def build_feature_matrix(
    courses: Sequence[CourseRecord],
    embeddings_by_sha: dict[str, dict[str, np.ndarray | None]],
    weights: dict[str, float],
    embedding_dimension: int,
) -> np.ndarray:
    if embedding_dimension <= 0:
        raise ValueError("Cannot build course vectors without a positive embedding dimension")

    rows: list[np.ndarray] = []
    for course in courses:
        pieces: list[np.ndarray] = []
        course_embeddings = embeddings_by_sha[course.sha256]
        for section_name in SECTION_NAMES:
            vector = course_embeddings.get(section_name)
            if vector is None:
                vector = np.zeros(embedding_dimension, dtype=float)
            scaled = np.asarray(vector, dtype=float) * np.sqrt(weights.get(section_name, 0.0))
            pieces.append(scaled)
        rows.append(np.concatenate(pieces))
    return np.vstack(rows) if rows else np.zeros((0, embedding_dimension * len(SECTION_NAMES)), dtype=float)


def _make_kmeans(n_clusters: int, n_init: str | int, max_iter: int, random_state: int | None):
    from sklearn.cluster import KMeans

    try:
        return KMeans(n_clusters=n_clusters, n_init=n_init, max_iter=max_iter, random_state=random_state)
    except TypeError:
        fallback_n_init = 10 if n_init == "auto" else int(n_init)
        return KMeans(n_clusters=n_clusters, n_init=fallback_n_init, max_iter=max_iter, random_state=random_state)


def _score_labels(features: np.ndarray, labels: np.ndarray, method: str) -> float | None:
    unique = set(int(label) for label in labels)
    if len(unique) < 2 or len(unique) >= len(labels):
        return None

    if method == "silhouette":
        from sklearn.metrics import silhouette_score

        return float(silhouette_score(features, labels))
    if method == "calinski_harabasz":
        from sklearn.metrics import calinski_harabasz_score

        return float(calinski_harabasz_score(features, labels))
    if method == "davies_bouldin":
        from sklearn.metrics import davies_bouldin_score

        return float(davies_bouldin_score(features, labels))
    raise ValueError(f"Unsupported cluster count method: {method}")


def estimate_n_clusters(
    features: np.ndarray,
    *,
    k_min: int,
    k_max: int,
    method: str,
    random_state: int | None,
) -> tuple[int, list[dict[str, float | int]]]:
    sample_count = int(features.shape[0])
    if sample_count <= 2:
        return 1, []

    lower = max(2, k_min)
    upper = min(k_max, sample_count - 1)
    if lower > upper:
        return 1, []

    best_k = lower
    best_score: float | None = None
    scores: list[dict[str, float | int]] = []

    for k in range(lower, upper + 1):
        model = _make_kmeans(k, "auto", 300, random_state)
        labels = model.fit_predict(features)
        score = _score_labels(features, labels, method)
        if score is None:
            continue
        scores.append({"k": k, "score": score})

        better = (
            best_score is None
            or (method == "davies_bouldin" and score < best_score)
            or (method != "davies_bouldin" and score > best_score)
        )
        if better:
            best_k = k
            best_score = score

    return best_k, scores


def resolve_n_clusters(value: int | str, features: np.ndarray, args: Any) -> tuple[int, list[dict[str, float | int]]]:
    if value != "auto":
        return int(value), []
    return estimate_n_clusters(
        features,
        k_min=args.k_min,
        k_max=args.k_max,
        method=args.cluster_count_method,
        random_state=args.random_state,
    )


def cluster_agglomerative(
    features: np.ndarray,
    distance_matrix: np.ndarray,
    args: Any,
) -> ClusterResult:
    from sklearn.cluster import AgglomerativeClustering

    if len(features) <= 1:
        return ClusterResult(labels=np.zeros(len(features), dtype=int), algorithm="agglomerative", n_clusters=1)

    scores: list[dict[str, float | int]] = []
    n_clusters: int | None
    if args.distance_threshold is not None:
        n_clusters = None
    else:
        n_clusters, scores = resolve_n_clusters(args.n_clusters, features, args)

    kwargs = {
        "n_clusters": n_clusters,
        "linkage": args.agglomerative_linkage,
        "distance_threshold": args.distance_threshold,
    }
    if args.distance_threshold is not None:
        kwargs["compute_full_tree"] = True

    if args.agglomerative_linkage == "ward":
        model = AgglomerativeClustering(**kwargs)
        labels = model.fit_predict(features)
    else:
        try:
            model = AgglomerativeClustering(metric="precomputed", **kwargs)
        except TypeError:
            model = AgglomerativeClustering(affinity="precomputed", **kwargs)
        labels = model.fit_predict(distance_matrix)

    return ClusterResult(labels=np.asarray(labels, dtype=int), algorithm="agglomerative", n_clusters=n_clusters, cluster_count_scores=scores)


def cluster_hdbscan(features: np.ndarray, distance_matrix: np.ndarray, args: Any) -> ClusterResult:
    if len(features) <= 1:
        return ClusterResult(labels=np.zeros(len(features), dtype=int), algorithm="hdbscan", n_clusters=None)

    try:
        from sklearn.cluster import HDBSCAN
    except ImportError as error:
        raise RuntimeError("scikit-learn HDBSCAN is not available in this environment") from error

    kwargs = {
        "min_cluster_size": args.hdbscan_min_cluster_size,
        "min_samples": args.hdbscan_min_samples,
        "cluster_selection_epsilon": args.hdbscan_cluster_selection_epsilon,
        "cluster_selection_method": args.hdbscan_cluster_selection_method,
    }
    try:
        labels = HDBSCAN(metric="precomputed", **kwargs).fit_predict(distance_matrix)
    except Exception:
        labels = HDBSCAN(metric="euclidean", **kwargs).fit_predict(features)
    return ClusterResult(labels=np.asarray(labels, dtype=int), algorithm="hdbscan", n_clusters=None)


def cluster_spectral(features: np.ndarray, similarity_matrix: np.ndarray, args: Any) -> ClusterResult:
    from sklearn.cluster import SpectralClustering

    if len(features) <= 1:
        return ClusterResult(labels=np.zeros(len(features), dtype=int), algorithm="spectral", n_clusters=1)

    n_clusters, scores = resolve_n_clusters(args.n_clusters, features, args)
    affinity = np.clip(similarity_matrix, 0.0, 1.0)
    np.fill_diagonal(affinity, 1.0)
    model = SpectralClustering(
        n_clusters=n_clusters,
        affinity="precomputed",
        assign_labels=args.spectral_assign_labels,
        n_init=args.spectral_n_init,
        random_state=args.random_state,
    )
    labels = model.fit_predict(affinity)
    return ClusterResult(labels=np.asarray(labels, dtype=int), algorithm="spectral", n_clusters=n_clusters, cluster_count_scores=scores)


def cluster_kmeans(features: np.ndarray, args: Any) -> ClusterResult:
    if len(features) <= 1:
        return ClusterResult(labels=np.zeros(len(features), dtype=int), algorithm="kmeans", n_clusters=1)

    n_clusters, scores = resolve_n_clusters(args.n_clusters, features, args)
    model = _make_kmeans(n_clusters, args.kmeans_n_init, args.kmeans_max_iter, args.random_state)
    labels = model.fit_predict(features)
    return ClusterResult(labels=np.asarray(labels, dtype=int), algorithm="kmeans", n_clusters=n_clusters, cluster_count_scores=scores)


def run_clustering(
    features: np.ndarray,
    similarity_matrix: np.ndarray,
    distance_matrix: np.ndarray,
    args: Any,
) -> ClusterResult:
    if args.algorithm == "agglomerative":
        return cluster_agglomerative(features, distance_matrix, args)
    if args.algorithm == "hdbscan":
        return cluster_hdbscan(features, distance_matrix, args)
    if args.algorithm == "spectral":
        return cluster_spectral(features, similarity_matrix, args)
    if args.algorithm == "kmeans":
        return cluster_kmeans(features, args)
    raise ValueError(f"Unsupported algorithm: {args.algorithm}")

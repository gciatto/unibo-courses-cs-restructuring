from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib
import shlex
import sys
from typing import Any, Sequence

import yaml

from clustering.algorithms import build_feature_matrix, run_clustering
from clustering.cache import prune_embedding_caches, prune_similarity_caches
from clustering.charts import generate_charts
from clustering.course_io import CourseRecord, load_courses
from clustering.embeddings import (
    DEFAULT_EMBEDDING_MODEL,
    DeterministicHashEmbedder,
    EmbeddingConfig,
    SentenceTransformerEmbedder,
    TextEmbedder,
    load_or_compute_embeddings,
)
from clustering.reports import write_reports
from clustering.sections import SECTION_COURSE_CONTENTS, SECTION_LEARNING_OUTCOMES, SECTION_READINGS, SECTION_TITLE
from clustering.similarities import DEFAULT_WEIGHTS, build_similarity_matrices, normalize_weights


LOGGER = logging.getLogger(__name__)
DEFAULT_COURSES_DIR = pathlib.Path("data/courses/.files")
DEFAULT_OUTPUT_DIR = pathlib.Path("data/clusters/runs")


def default_year() -> int:
    return dt.date.today().year - 1


def n_clusters_value(value: str) -> int | str:
    if value == "auto":
        return value
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("--n-clusters must be a positive integer or 'auto'")
    return parsed


def kmeans_n_init_value(value: str) -> int | str:
    if value == "auto":
        return value
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("--kmeans-n-init must be a positive integer or 'auto'")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cluster UniBo courses by selected syllabus sections.")
    parser.add_argument("--year", type=int, default=default_year(), help="Reference academic year (default: current year - 1).")
    parser.add_argument("--courses-dir", type=pathlib.Path, default=DEFAULT_COURSES_DIR, help=f"Base course directory (default: {DEFAULT_COURSES_DIR}).")
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR, help=f"Base output directory (default: {DEFAULT_OUTPUT_DIR}).")
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL, help=f"SentenceTransformer model name (default: {DEFAULT_EMBEDDING_MODEL}).")
    parser.add_argument("--device", choices=["auto", "cpu", "cuda", "mps"], default="auto")
    parser.add_argument("--embedding-batch-size", type=int, default=32)
    parser.add_argument("--chunk-token-limit", type=int, default=384)

    parser.add_argument("--weight-title", type=float, default=DEFAULT_WEIGHTS[SECTION_TITLE])
    parser.add_argument("--weight-learning-outcomes", type=float, default=DEFAULT_WEIGHTS[SECTION_LEARNING_OUTCOMES])
    parser.add_argument("--weight-course-contents", type=float, default=DEFAULT_WEIGHTS[SECTION_COURSE_CONTENTS])
    parser.add_argument("--weight-readings", type=float, default=DEFAULT_WEIGHTS[SECTION_READINGS])

    parser.add_argument("--algorithm", choices=["agglomerative", "hdbscan", "spectral", "kmeans"], default="agglomerative")
    parser.add_argument("--n-clusters", type=n_clusters_value, default="auto")
    parser.add_argument("--cluster-count-method", choices=["silhouette", "calinski_harabasz", "davies_bouldin"], default="silhouette")
    parser.add_argument("--k-min", type=int, default=2)
    parser.add_argument("--k-max", type=int, default=20)

    parser.add_argument("--agglomerative-linkage", choices=["average", "complete", "single", "ward"], default="average")
    parser.add_argument("--distance-threshold", type=float, default=None)

    parser.add_argument("--hdbscan-min-cluster-size", type=int, default=5)
    parser.add_argument("--hdbscan-min-samples", type=int, default=None)
    parser.add_argument("--hdbscan-cluster-selection-epsilon", type=float, default=0.0)
    parser.add_argument("--hdbscan-cluster-selection-method", choices=["eom", "leaf"], default="eom")

    parser.add_argument("--spectral-assign-labels", choices=["kmeans", "discretize", "cluster_qr"], default="kmeans")
    parser.add_argument("--spectral-n-init", type=int, default=10)

    parser.add_argument("--kmeans-n-init", type=kmeans_n_init_value, default="auto")
    parser.add_argument("--kmeans-max-iter", type=int, default=300)

    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--refresh-embeddings", action="store_true")
    parser.add_argument("--refresh-similarities", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--prune-cache", action="store_true")
    parser.add_argument("--fake-embeddings", action="store_true", help=argparse.SUPPRESS)
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.year <= 0:
        raise ValueError("--year must be > 0")
    if args.embedding_batch_size <= 0:
        raise ValueError("--embedding-batch-size must be > 0")
    if args.chunk_token_limit <= 0:
        raise ValueError("--chunk-token-limit must be > 0")
    if args.k_min <= 0 or args.k_max <= 0:
        raise ValueError("--k-min and --k-max must be > 0")
    if args.k_min > args.k_max:
        raise ValueError("--k-min must be <= --k-max")
    if args.hdbscan_min_cluster_size <= 1:
        raise ValueError("--hdbscan-min-cluster-size must be > 1")
    if args.hdbscan_min_samples is not None and args.hdbscan_min_samples <= 0:
        raise ValueError("--hdbscan-min-samples must be > 0 when provided")
    if args.spectral_n_init <= 0:
        raise ValueError("--spectral-n-init must be > 0")
    if args.kmeans_max_iter <= 0:
        raise ValueError("--kmeans-max-iter must be > 0")
    if args.distance_threshold is not None and args.distance_threshold < 0:
        raise ValueError("--distance-threshold must be >= 0")
    if args.agglomerative_linkage == "ward" and args.distance_threshold is not None:
        LOGGER.info("Ward linkage selected; using feature vectors with Euclidean geometry.")


def weights_from_args(args: argparse.Namespace) -> dict[str, float]:
    return normalize_weights(
        {
            SECTION_TITLE: args.weight_title,
            SECTION_LEARNING_OUTCOMES: args.weight_learning_outcomes,
            SECTION_COURSE_CONTENTS: args.weight_course_contents,
            SECTION_READINGS: args.weight_readings,
        }
    )


def make_run_dir(output_dir: pathlib.Path, year: int, algorithm: str) -> pathlib.Path:
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    base = output_dir / f"{year}-{timestamp}-{algorithm}"
    if not base.exists():
        return base
    counter = 2
    while True:
        candidate = output_dir / f"{year}-{timestamp}-{algorithm}-{counter}"
        if not candidate.exists():
            return candidate
        counter += 1


def serialize_args(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in vars(args).items():
        result[key] = str(value) if isinstance(value, pathlib.Path) else value
    return result


def build_run_config(
    args: argparse.Namespace,
    courses: Sequence[CourseRecord],
    weights: dict[str, float],
    embedding_config: EmbeddingConfig,
    model_config_hash: str,
    cluster_result,
) -> dict[str, Any]:
    return {
        "command": shlex.join(sys.argv),
        "arguments": serialize_args(args),
        "course_count": len(courses),
        "weights": weights,
        "embedding": {
            **embedding_config.model_config_payload(),
            "model_config_hash": model_config_hash,
        },
        "clustering": {
            "algorithm": cluster_result.algorithm,
            "n_clusters": cluster_result.n_clusters,
            "cluster_count_scores": cluster_result.cluster_count_scores,
        },
    }


def make_embedder(args: argparse.Namespace, embedding_config: EmbeddingConfig) -> TextEmbedder:
    if args.fake_embeddings:
        return DeterministicHashEmbedder()
    return SentenceTransformerEmbedder(embedding_config.model_name, device=embedding_config.device)


def run(args: argparse.Namespace, *, embedder: TextEmbedder | None = None) -> pathlib.Path:
    validate_args(args)

    year_dir = args.courses_dir / str(args.year)
    courses = load_courses(args.courses_dir, args.year, args.limit)
    if not courses:
        raise ValueError(f"No courses found under {year_dir}")

    weights = weights_from_args(args)
    embedding_config = EmbeddingConfig(
        model_name=args.embedding_model,
        chunk_token_limit=args.chunk_token_limit,
        embedding_batch_size=args.embedding_batch_size,
        device=args.device,
    )
    model_config_hash = embedding_config.model_config_hash

    valid_course_shas = {course.sha256 for course in courses}
    if args.prune_cache:
        removed_embeddings = prune_embedding_caches(year_dir, valid_course_shas, model_config_hash)
        removed_similarities = prune_similarity_caches(year_dir, valid_course_shas, model_config_hash)
        LOGGER.info("Pruned cache files: embeddings=%s similarities=%s", removed_embeddings, removed_similarities)

    resolved_embedder = embedder or make_embedder(args, embedding_config)
    embeddings_by_sha, embedding_dimension = load_or_compute_embeddings(
        courses,
        resolved_embedder,
        embedding_config,
        no_cache=args.no_cache,
        refresh=args.refresh_embeddings,
    )

    similarity_matrix, distance_matrix, pair_payloads = build_similarity_matrices(
        courses,
        embeddings_by_sha,
        weights,
        model_config_hash,
        year_dir=year_dir,
        no_cache=args.no_cache,
        refresh=args.refresh_similarities,
    )

    features = build_feature_matrix(courses, embeddings_by_sha, weights, embedding_dimension)
    cluster_result = run_clustering(features, similarity_matrix, distance_matrix, args)

    run_dir = make_run_dir(args.output_dir, args.year, args.algorithm)
    run_config = build_run_config(args, courses, weights, embedding_config, model_config_hash, cluster_result)
    write_reports(run_dir, courses, similarity_matrix, distance_matrix, cluster_result, pair_payloads, run_config)
    generate_charts(
        run_dir,
        courses,
        similarity_matrix,
        distance_matrix,
        features,
        cluster_result.labels,
        algorithm=args.algorithm,
        agglomerative_linkage=args.agglomerative_linkage,
    )
    return run_dir


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        run_dir = run(args)
    except Exception as error:
        LOGGER.error("%s", error)
        return 1

    print(yaml.safe_dump({"run_dir": str(run_dir)}, sort_keys=False).strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

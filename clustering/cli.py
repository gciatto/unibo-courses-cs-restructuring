from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib
import shlex
import sys
from textwrap import dedent
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


class HelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    """Show defaults and preserve line breaks in long help sections."""


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
    epilog = dedent(
                """
                Algorithm guidance:
                    agglomerative
                        + Interpretable hierarchy; works directly with precomputed distances.
                        + Good default for medium-size datasets and dendrogram analysis.
                        - Can be slower/more memory-heavy than centroid methods.
                        - Requires n-clusters or careful distance-threshold tuning.

                    hdbscan
                        + Detects noise/outliers (label -1) and handles varying densities.
                        + Does not require n-clusters.
                        - Sensitive to min-cluster-size/min-samples choices.
                        - May classify many borderline courses as noise if too strict.

                    spectral
                        + Captures non-convex structure using similarity affinity.
                        + Useful when pairwise similarity matrix is high quality.
                        - Requires n-clusters.
                        - More computationally expensive than kmeans at larger scale.

                    kmeans
                        + Fast, scalable, strong baseline.
                        + Easy to compare across runs with fixed random-state.
                        - Requires n-clusters.
                        - Assumes roughly compact/spherical cluster geometry.

                Cluster-count guidance (used when --n-clusters auto):
                    silhouette
                        + Balanced compactness/separation, intuitive default.
                        - Can prefer too few clusters in some datasets.

                    calinski_harabasz
                        + Fast and effective for compact, well-separated groups.
                        - May favor larger k in some cases.

                    davies_bouldin
                        + Penalizes overlapping clusters.
                        - Lower-is-better metric can be less intuitive; sensitive to shape assumptions.
                """
    )
    parser = argparse.ArgumentParser(
        description="Cluster UniBo courses by selected syllabus sections.",
        epilog=epilog,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "--year",
        type=int,
        default=default_year(),
        help="Reference academic year to read from <courses-dir>/<year>/course-*.yml.",
    )
    parser.add_argument(
        "--courses-dir",
        type=pathlib.Path,
        default=DEFAULT_COURSES_DIR,
        help="Base directory containing year subdirectories with merged course files.",
    )
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Base directory where run outputs are written as <year>-<timestamp>-<algorithm>.",
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_EMBEDDING_MODEL,
        help="Sentence-Transformers model used to embed section text.",
    )
    parser.add_argument(
        "--device",
        choices=["auto", "cpu", "cuda", "mps"],
        default="auto",
        help="Execution device for embedding inference.",
    )
    parser.add_argument(
        "--embedding-batch-size",
        type=int,
        default=32,
        help="Number of text chunks embedded per forward pass.",
    )
    parser.add_argument(
        "--chunk-token-limit",
        type=int,
        default=384,
        help="Approximate max token budget per chunk before splitting long sections.",
    )

    parser.add_argument(
        "--weight-title",
        type=float,
        default=DEFAULT_WEIGHTS[SECTION_TITLE],
        help="Weight assigned to course title similarity.",
    )
    parser.add_argument(
        "--weight-learning-outcomes",
        type=float,
        default=DEFAULT_WEIGHTS[SECTION_LEARNING_OUTCOMES],
        help="Weight assigned to Learning outcomes similarity.",
    )
    parser.add_argument(
        "--weight-course-contents",
        type=float,
        default=DEFAULT_WEIGHTS[SECTION_COURSE_CONTENTS],
        help="Weight assigned to Course contents similarity.",
    )
    parser.add_argument(
        "--weight-readings",
        type=float,
        default=DEFAULT_WEIGHTS[SECTION_READINGS],
        help="Weight assigned to Readings/Bibliography similarity.",
    )

    parser.add_argument(
        "--algorithm",
        choices=["agglomerative", "hdbscan", "spectral", "kmeans"],
        default="agglomerative",
        help="Clustering algorithm to apply to the computed course representation.",
    )
    parser.add_argument(
        "--n-clusters",
        type=n_clusters_value,
        default="auto",
        help=(
            "Number of clusters for algorithms that require it, or auto to search in [k-min, k-max]. "
            "Applies to agglomerative/spectral/kmeans; ignored by hdbscan."
        ),
    )
    parser.add_argument(
        "--cluster-count-method",
        choices=["silhouette", "calinski_harabasz", "davies_bouldin"],
        default="silhouette",
        help="Metric used when --n-clusters is auto.",
    )
    parser.add_argument(
        "--k-min",
        type=int,
        default=2,
        help="Lower bound for auto cluster-count search (agglomerative/spectral/kmeans only).",
    )
    parser.add_argument(
        "--k-max",
        type=int,
        default=20,
        help="Upper bound for auto cluster-count search (agglomerative/spectral/kmeans only).",
    )

    parser.add_argument(
        "--agglomerative-linkage",
        choices=["average", "complete", "single", "ward"],
        default="average",
        help="Linkage criterion for agglomerative clustering.",
    )
    parser.add_argument(
        "--distance-threshold",
        type=float,
        default=None,
        help="Optional merge-stop threshold for agglomerative clustering distance.",
    )

    parser.add_argument(
        "--hdbscan-min-cluster-size",
        type=int,
        default=5,
        help="Minimum size of a dense group to be considered a cluster by HDBSCAN.",
    )
    parser.add_argument(
        "--hdbscan-min-samples",
        type=int,
        default=None,
        help="Core-point neighborhood size for HDBSCAN; when omitted, implementation default is used.",
    )
    parser.add_argument(
        "--hdbscan-cluster-selection-epsilon",
        type=float,
        default=0.0,
        help="Distance tolerance for merging nearby HDBSCAN clusters.",
    )
    parser.add_argument(
        "--hdbscan-cluster-selection-method",
        choices=["eom", "leaf"],
        default="eom",
        help="HDBSCAN strategy for selecting final clusters from the hierarchy.",
    )

    parser.add_argument(
        "--spectral-assign-labels",
        choices=["kmeans", "discretize", "cluster_qr"],
        default="kmeans",
        help="Label assignment strategy after spectral embedding.",
    )
    parser.add_argument(
        "--spectral-n-init",
        type=int,
        default=10,
        help="Number of initializations for spectral clustering internals (when applicable).",
    )

    parser.add_argument(
        "--kmeans-n-init",
        type=kmeans_n_init_value,
        default="auto",
        help="Number of centroid initializations for KMeans, or auto.",
    )
    parser.add_argument(
        "--kmeans-max-iter",
        type=int,
        default=300,
        help="Maximum number of iterations for each KMeans initialization.",
    )

    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for stochastic clustering components.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of discovered course files for faster smoke runs.",
    )
    parser.add_argument(
        "--refresh-embeddings",
        action="store_true",
        help="Recompute and overwrite embedding cache files for selected courses.",
    )
    parser.add_argument(
        "--refresh-similarities",
        action="store_true",
        help="Recompute and overwrite pairwise similarity cache files.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable reading and writing all caches for this run.",
    )
    parser.add_argument(
        "--prune-cache",
        action="store_true",
        help="Remove stale cache files under the selected year/model before clustering.",
    )
    parser.add_argument(
        "--fake-embeddings",
        action="store_true",
        help="Use deterministic hash-based embeddings (testing/debug only, no semantic meaning).",
    )
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
    summary = write_reports(run_dir, courses, similarity_matrix, distance_matrix, cluster_result, pair_payloads, run_config)
    cluster_name_by_id = {
        int(cluster_id): str(item.get("cluster_name") or f"Cluster {cluster_id}")
        for cluster_id, item in summary.items()
    }
    generate_charts(
        run_dir,
        courses,
        similarity_matrix,
        distance_matrix,
        features,
        cluster_result.labels,
        algorithm=args.algorithm,
        agglomerative_linkage=args.agglomerative_linkage,
        cluster_name_by_id=cluster_name_by_id,
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

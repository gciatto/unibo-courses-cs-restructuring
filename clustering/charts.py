from __future__ import annotations

import base64
import logging
import os
import pathlib
from typing import Sequence

import numpy as np

from clustering.course_io import CourseRecord


LOGGER = logging.getLogger(__name__)

PLACEHOLDER_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


def write_placeholder_png(path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(PLACEHOLDER_PNG)


def _load_pyplot():
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def plot_similarity_heatmap(
    path: pathlib.Path,
    similarity_matrix: np.ndarray,
    labels: np.ndarray,
) -> None:
    try:
        plt = _load_pyplot()
        order = np.lexsort((np.arange(len(labels)), labels))
        sorted_matrix = similarity_matrix[np.ix_(order, order)] if len(order) else similarity_matrix
        fig, ax = plt.subplots(figsize=(10, 8))
        try:
            import seaborn as sns

            sns.heatmap(sorted_matrix, cmap="viridis", vmin=0, vmax=1, ax=ax, cbar_kws={"label": "Similarity"})
        except Exception:
            image = ax.imshow(sorted_matrix, cmap="viridis", vmin=0, vmax=1, aspect="auto")
            fig.colorbar(image, ax=ax, label="Similarity")
        ax.set_title("Course Similarity")
        ax.set_xlabel("Courses sorted by cluster")
        ax.set_ylabel("Courses sorted by cluster")
        fig.tight_layout()
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=160)
        plt.close(fig)
    except Exception as error:
        LOGGER.warning("Could not generate similarity heatmap: %s", error)
        write_placeholder_png(path)


def _cluster_color_map(
    labels: np.ndarray,
    cluster_name_by_id: dict[int, str],
) -> tuple[list[int], dict[int, str], dict[int, str]]:
    unique = sorted(int(label) for label in np.unique(labels))
    color_by_id: dict[int, str] = {}
    name_by_id: dict[int, str] = {}
    palette = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
    ]
    for index, cluster_id in enumerate(unique):
        if cluster_id == -1:
            color_by_id[cluster_id] = "#6b7280"
        else:
            color_by_id[cluster_id] = palette[index % len(palette)]
        name_by_id[cluster_id] = cluster_name_by_id.get(cluster_id, f"Cluster {cluster_id}")
    return unique, color_by_id, name_by_id


def plot_cluster_sizes(
    path: pathlib.Path,
    labels: np.ndarray,
    cluster_name_by_id: dict[int, str],
) -> None:
    try:
        plt = _load_pyplot()
        unique, counts = np.unique(labels, return_counts=True)
        unique_ids, color_by_id, name_by_id = _cluster_color_map(labels, cluster_name_by_id)
        count_by_id = {int(label): int(count) for label, count in zip(unique, counts)}
        fig, ax = plt.subplots(figsize=(9, 5))
        positions = np.arange(len(unique_ids))
        bar_counts = [count_by_id.get(cluster_id, 0) for cluster_id in unique_ids]
        bar_colors = [color_by_id[cluster_id] for cluster_id in unique_ids]
        ax.bar(positions, bar_counts, color=bar_colors)
        ax.set_xticks(positions, [str(cluster_id) for cluster_id in unique_ids])
        ax.set_title("Cluster Sizes")
        ax.set_xlabel("Cluster")
        ax.set_ylabel("Courses")

        from matplotlib.patches import Patch

        legend_items = [
            Patch(facecolor=color_by_id[cluster_id], edgecolor="none", label=name_by_id[cluster_id])
            for cluster_id in unique_ids
        ]
        if legend_items:
            ax.legend(handles=legend_items, title="Cluster names", loc="best", fontsize=8)

        fig.tight_layout()
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=160)
        plt.close(fig)
    except Exception as error:
        LOGGER.warning("Could not generate cluster size chart: %s", error)
        write_placeholder_png(path)


def plot_projection_pca(
    path: pathlib.Path,
    features: np.ndarray,
    labels: np.ndarray,
    cluster_name_by_id: dict[int, str],
) -> None:
    try:
        plt = _load_pyplot()
        if len(features) < 2:
            write_placeholder_png(path)
            return

        from sklearn.decomposition import PCA

        projection = PCA(n_components=2, random_state=0).fit_transform(features)
        unique_ids, color_by_id, name_by_id = _cluster_color_map(labels, cluster_name_by_id)
        fig, ax = plt.subplots(figsize=(8, 6))
        for cluster_id in unique_ids:
            mask = labels == cluster_id
            ax.scatter(
                projection[mask, 0],
                projection[mask, 1],
                s=36,
                alpha=0.9,
                color=color_by_id[cluster_id],
                label=name_by_id[cluster_id],
            )
        ax.set_title("PCA Projection")
        ax.set_xlabel("PC1")
        ax.set_ylabel("PC2")
        ax.legend(title="Cluster names", loc="best", fontsize=8)
        fig.tight_layout()
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=160)
        plt.close(fig)
    except Exception as error:
        LOGGER.warning("Could not generate PCA projection: %s", error)
        write_placeholder_png(path)


def plot_dendrogram(
    path: pathlib.Path,
    courses: Sequence[CourseRecord],
    distance_matrix: np.ndarray,
    features: np.ndarray,
    linkage_method: str,
) -> None:
    try:
        plt = _load_pyplot()
        if len(courses) < 2:
            write_placeholder_png(path)
            return

        from scipy.cluster.hierarchy import dendrogram, linkage
        from scipy.spatial.distance import squareform

        if linkage_method == "ward":
            linkage_matrix = linkage(features, method="ward")
        else:
            condensed = squareform(distance_matrix, checks=False)
            linkage_matrix = linkage(condensed, method=linkage_method)

        labels = [course.course_id for course in courses]
        width = max(10, min(36, len(courses) * 0.25))
        fig, ax = plt.subplots(figsize=(width, 8))
        dendrogram(linkage_matrix, labels=labels, leaf_rotation=90, leaf_font_size=6, ax=ax)
        ax.set_title("Agglomerative Dendrogram")
        ax.set_ylabel("Distance")
        fig.tight_layout()
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=160)
        plt.close(fig)
    except Exception as error:
        LOGGER.warning("Could not generate dendrogram: %s", error)
        write_placeholder_png(path)


def generate_charts(
    run_dir: pathlib.Path,
    courses: Sequence[CourseRecord],
    similarity_matrix: np.ndarray,
    distance_matrix: np.ndarray,
    features: np.ndarray,
    labels: np.ndarray,
    *,
    algorithm: str,
    agglomerative_linkage: str,
    cluster_name_by_id: dict[int, str],
) -> None:
    charts_dir = run_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    plot_similarity_heatmap(charts_dir / "similarity_heatmap.png", similarity_matrix, labels)
    plot_cluster_sizes(charts_dir / "cluster_sizes.png", labels, cluster_name_by_id)
    plot_projection_pca(charts_dir / "projection_pca.png", features, labels, cluster_name_by_id)
    if algorithm == "agglomerative":
        plot_dendrogram(
            charts_dir / "dendrogram.png",
            courses,
            distance_matrix,
            features,
            agglomerative_linkage,
        )

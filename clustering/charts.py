from __future__ import annotations

import base64
import logging
import os
import pathlib
from typing import Sequence

import numpy as np

from clustering.course_io import CourseRecord
from clustering.reports import course_short_label


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
        fig, ax = plt.subplots(figsize=(24, 8))
        positions = np.arange(len(unique_ids))
        bar_counts = [count_by_id.get(cluster_id, 0) for cluster_id in unique_ids]
        bar_colors = [color_by_id[cluster_id] for cluster_id in unique_ids]
        bars = ax.bar(positions, bar_counts, color=bar_colors)
        for bar, count in zip(bars, bar_counts):
            ax.annotate(
                str(count),
                xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=9,
            )
        if bar_counts:
            ax.set_ylim(0, max(bar_counts) * 1.15 + 1)
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
            ax.legend(
                handles=legend_items,
                title="Cluster names",
                loc="upper left",
                bbox_to_anchor=(1.02, 1.0),
                borderaxespad=0.0,
                fontsize=8,
            )

        fig.tight_layout(rect=(0.0, 0.0, 0.58, 1.0))
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=160, bbox_inches="tight", pad_inches=0.3)
        plt.close(fig)
    except Exception as error:
        LOGGER.warning("Could not generate cluster size chart: %s", error)
        write_placeholder_png(path)


def _classical_mds_projection(distance_matrix: np.ndarray) -> np.ndarray:
    distances = np.asarray(distance_matrix, dtype=float)
    sample_count = distances.shape[0]
    if sample_count < 2:
        return np.zeros((sample_count, 2), dtype=float)

    squared = distances**2
    centering = np.eye(sample_count) - np.ones((sample_count, sample_count)) / sample_count
    gram = -0.5 * centering @ squared @ centering
    eigenvalues, eigenvectors = np.linalg.eigh(gram)
    order = np.argsort(eigenvalues)[::-1]
    dimensions = order[:2]
    values = np.maximum(eigenvalues[dimensions], 0.0)
    projection = eigenvectors[:, dimensions] * np.sqrt(values)
    if projection.shape[1] < 2:
        projection = np.pad(projection, ((0, 0), (0, 2 - projection.shape[1])))
    return projection[:, :2]


def plot_projection_pca(
    path: pathlib.Path,
    features: np.ndarray | None,
    labels: np.ndarray,
    cluster_name_by_id: dict[int, str],
    distance_matrix: np.ndarray | None = None,
) -> None:
    try:
        plt = _load_pyplot()
        if features is None and distance_matrix is None:
            write_placeholder_png(path)
            return

        projection_title = "PCA Projection"
        if features is not None:
            if len(features) < 2:
                write_placeholder_png(path)
                return
            try:
                from sklearn.decomposition import PCA

                projection = PCA(n_components=2, random_state=0).fit_transform(features)
            except Exception:
                # Fallback for environments without scikit-learn.
                centered = np.asarray(features, dtype=float) - np.asarray(features, dtype=float).mean(axis=0, keepdims=True)
                _, _, vt = np.linalg.svd(centered, full_matrices=False)
                basis = vt[:2].T if vt.shape[0] >= 2 else np.pad(vt.T, ((0, 0), (0, 2 - vt.shape[0])))
                projection = centered @ basis
        else:
            if distance_matrix is None or len(distance_matrix) < 2:
                write_placeholder_png(path)
                return
            projection = _classical_mds_projection(distance_matrix)
            projection_title = "Distance Projection"

        unique_ids, color_by_id, name_by_id = _cluster_color_map(labels, cluster_name_by_id)
        fig, ax = plt.subplots(figsize=(24, 8))
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
        ax.set_title(projection_title)
        ax.set_xlabel("Component 1")
        ax.set_ylabel("Component 2")
        ax.legend(
            title="Cluster names",
            loc="upper left",
            bbox_to_anchor=(1.02, 1.0),
            borderaxespad=0.0,
            fontsize=8,
        )
        fig.tight_layout(rect=(0.0, 0.0, 0.58, 1.0))
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=160, bbox_inches="tight", pad_inches=0.3)
        plt.close(fig)
    except Exception as error:
        LOGGER.warning("Could not generate PCA projection: %s", error)
        write_placeholder_png(path)


def _dendrogram_figure_height(course_count: int) -> float:
    return max(10.0, min(120.0, course_count * 0.24))


def _synthetic_cluster_linkage(courses: Sequence[CourseRecord], labels: np.ndarray) -> np.ndarray:
    if len(courses) < 2:
        return np.zeros((0, 4), dtype=float)

    ordered_indices = sorted(range(len(courses)), key=lambda index: (int(labels[index]), courses[index].course_id))
    rows: list[list[float]] = []
    next_node_id = len(courses)
    cluster_roots: list[tuple[int, int]] = []

    for cluster_id in sorted(int(label) for label in np.unique(labels)):
        indices = [index for index in ordered_indices if int(labels[index]) == cluster_id]
        if len(indices) == 1:
            cluster_roots.append((indices[0], 1))
            continue

        current_node = indices[0]
        current_count = 1
        for index in indices[1:]:
            current_count += 1
            rows.append([float(current_node), float(index), 0.5, float(current_count)])
            current_node = next_node_id
            next_node_id += 1
        cluster_roots.append((current_node, current_count))

    current_node, current_count = cluster_roots[0]
    for root_node, root_count in cluster_roots[1:]:
        current_count += root_count
        rows.append([float(current_node), float(root_node), 1.5, float(current_count)])
        current_node = next_node_id
        next_node_id += 1

    return np.asarray(rows, dtype=float)


def _agglomerative_linkage_matrix(
    distance_matrix: np.ndarray,
    features: np.ndarray | None,
    linkage_method: str,
) -> np.ndarray:
    from scipy.cluster.hierarchy import linkage
    from scipy.spatial.distance import squareform

    if linkage_method == "ward":
        if features is None:
            raise ValueError("Ward linkage requires feature vectors")
        return linkage(features, method="ward")

    condensed = squareform(distance_matrix, checks=False)
    return linkage(condensed, method=linkage_method)


def _annotate_dendrogram_clusters(
    ax,
    dendrogram_data: dict[str, object],
    labels: np.ndarray,
    cluster_name_by_id: dict[int, str],
) -> None:
    leaves = dendrogram_data.get("leaves", [])
    if not isinstance(leaves, list):
        return

    _, color_by_id, name_by_id = _cluster_color_map(labels, cluster_name_by_id)
    y_by_leaf = {int(leaf): 5 + 10 * position for position, leaf in enumerate(leaves)}
    x_limits = ax.get_xlim()
    x_min = min(x_limits)
    x_max = max(x_limits)
    x_span = x_max - x_min or 1.0
    label_x = x_max + x_span * 0.04
    ax.set_xlim(x_min, x_max + x_span * 0.35)

    for cluster_id in sorted(int(label) for label in np.unique(labels)):
        y_values = [
            y_by_leaf[index]
            for index, label in enumerate(labels)
            if int(label) == cluster_id and index in y_by_leaf
        ]
        if not y_values:
            continue
        y_min = min(y_values) - 5
        y_max = max(y_values) + 5
        ax.axhspan(y_min, y_max, color=color_by_id[cluster_id], alpha=0.06, zorder=0)
        ax.text(
            label_x,
            (y_min + y_max) / 2,
            name_by_id[cluster_id],
            va="center",
            ha="left",
            fontsize=8,
            clip_on=False,
        )


def plot_dendrogram(
    path: pathlib.Path,
    courses: Sequence[CourseRecord],
    distance_matrix: np.ndarray,
    features: np.ndarray | None,
    linkage_method: str,
    labels: np.ndarray,
    cluster_name_by_id: dict[int, str],
    *,
    algorithm: str,
) -> None:
    try:
        plt = _load_pyplot()
        if len(courses) < 2:
            write_placeholder_png(path)
            return

        from scipy.cluster.hierarchy import dendrogram

        if algorithm == "agglomerative":
            try:
                linkage_matrix = _agglomerative_linkage_matrix(distance_matrix, features, linkage_method)
            except Exception as error:
                LOGGER.warning("Could not build agglomerative dendrogram linkage, using cluster tree: %s", error)
                linkage_matrix = _synthetic_cluster_linkage(courses, labels)
        else:
            linkage_matrix = _synthetic_cluster_linkage(courses, labels)

        course_labels = [course_short_label(course) for course in courses]
        fig, ax = plt.subplots(figsize=(18, _dendrogram_figure_height(len(courses))))
        dendrogram_data = dendrogram(
            linkage_matrix,
            labels=course_labels,
            orientation="right",
            leaf_font_size=6,
            count_sort=False,
            distance_sort=False,
            ax=ax,
        )
        _annotate_dendrogram_clusters(ax, dendrogram_data, labels, cluster_name_by_id)
        ax.set_title(f"{algorithm.title()} Dendrogram")
        ax.set_xlabel("Distance")
        fig.subplots_adjust(left=0.42, right=0.74, top=0.97, bottom=0.04)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=160, bbox_inches="tight", pad_inches=0.3)
        plt.close(fig)
    except Exception as error:
        LOGGER.warning("Could not generate dendrogram: %s", error)
        write_placeholder_png(path)


def generate_charts(
    run_dir: pathlib.Path,
    courses: Sequence[CourseRecord],
    similarity_matrix: np.ndarray,
    distance_matrix: np.ndarray,
    features: np.ndarray | None,
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
    plot_projection_pca(charts_dir / "projection_pca.png", features, labels, cluster_name_by_id, distance_matrix)
    plot_dendrogram(
        charts_dir / "dendrogram.png",
        courses,
        distance_matrix,
        features,
        agglomerative_linkage,
        labels,
        cluster_name_by_id,
        algorithm=algorithm,
    )

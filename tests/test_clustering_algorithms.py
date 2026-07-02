from __future__ import annotations

import argparse
import pathlib
import unittest

import numpy as np

from clustering.algorithms import build_feature_matrix, run_clustering
from clustering.course_io import CourseRecord
from clustering.sections import SECTION_COURSE_CONTENTS, SECTION_LEARNING_OUTCOMES, SECTION_READINGS, SECTION_TITLE
from clustering.similarities import DEFAULT_WEIGHTS, build_similarity_matrices, normalize_weights


try:
    import sklearn  # noqa: F401

    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


def make_args(algorithm: str) -> argparse.Namespace:
    return argparse.Namespace(
        algorithm=algorithm,
        n_clusters=2,
        cluster_count_method="silhouette",
        k_min=2,
        k_max=4,
        agglomerative_linkage="average",
        distance_threshold=None,
        hdbscan_min_cluster_size=2,
        hdbscan_min_samples=1,
        hdbscan_cluster_selection_epsilon=0.0,
        hdbscan_cluster_selection_method="eom",
        spectral_assign_labels="kmeans",
        spectral_n_init=5,
        kmeans_n_init="auto",
        kmeans_max_iter=100,
        random_state=42,
    )


def synthetic_courses():
    courses = []
    embeddings = {}
    section_names = [SECTION_TITLE, SECTION_LEARNING_OUTCOMES, SECTION_COURSE_CONTENTS, SECTION_READINGS]
    vectors = [
        np.array([1.0, 0.0]),
        np.array([0.95, 0.05]),
        np.array([0.90, 0.10]),
        np.array([0.0, 1.0]),
        np.array([0.05, 0.95]),
        np.array([0.10, 0.90]),
    ]
    for index, vector in enumerate(vectors):
        sha = f"{index:064x}"[-64:]
        course = CourseRecord(
            course_id=f"C{index}",
            path=pathlib.Path(f"course-C{index}.yml"),
            sha256=sha,
            title=f"Course {index}",
            sections={},
            section_languages={},
            raw={},
        )
        courses.append(course)
        embeddings[sha] = {section_name: vector for section_name in section_names}
    return courses, embeddings


@unittest.skipUnless(HAS_SKLEARN, "scikit-learn is required for clustering algorithm smoke tests")
class TestClusteringAlgorithms(unittest.TestCase):
    def test_synthetic_smoke_for_supported_algorithms(self):
        courses, embeddings = synthetic_courses()
        weights = normalize_weights(DEFAULT_WEIGHTS)
        similarity_matrix, distance_matrix, _ = build_similarity_matrices(
            courses,
            embeddings,
            weights,
            "model",
            year_dir=pathlib.Path("."),
            no_cache=True,
        )
        features = build_feature_matrix(courses, embeddings, weights, embedding_dimension=2)

        for algorithm in ("agglomerative", "spectral", "kmeans", "hdbscan"):
            with self.subTest(algorithm=algorithm):
                if algorithm == "hdbscan":
                    try:
                        from sklearn.cluster import HDBSCAN  # noqa: F401
                    except ImportError:
                        self.skipTest("sklearn.cluster.HDBSCAN is unavailable")
                result = run_clustering(features, similarity_matrix, distance_matrix, make_args(algorithm))
                self.assertEqual(len(result.labels), len(courses))


if __name__ == "__main__":
    unittest.main()

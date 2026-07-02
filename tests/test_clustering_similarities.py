from __future__ import annotations

import pathlib
import unittest

import numpy as np

from clustering.course_io import CourseRecord
from clustering.sections import SECTION_COURSE_CONTENTS, SECTION_LEARNING_OUTCOMES, SECTION_READINGS, SECTION_TITLE
from clustering.similarities import compute_pair_similarity, normalize_weights


def course(course_id: str, sha: str) -> CourseRecord:
    return CourseRecord(
        course_id=course_id,
        path=pathlib.Path(f"course-{course_id}.yml"),
        sha256=sha,
        title=f"Course {course_id}",
        sections={},
        section_languages={},
        raw={},
    )


class TestClusteringSimilarities(unittest.TestCase):
    def test_weight_validation_and_normalization(self):
        weights = normalize_weights(
            {
                SECTION_TITLE: 1,
                SECTION_LEARNING_OUTCOMES: 1,
                SECTION_COURSE_CONTENTS: 2,
                SECTION_READINGS: 0,
            }
        )

        self.assertAlmostEqual(sum(weights.values()), 1.0)
        self.assertAlmostEqual(weights[SECTION_COURSE_CONTENTS], 0.5)

        with self.assertRaises(ValueError):
            normalize_weights({SECTION_TITLE: -1})

    def test_pairwise_similarity_is_symmetric_and_ignores_missing_sections(self):
        course_a = course("A", "a" * 64)
        course_b = course("B", "b" * 64)
        weights = normalize_weights(
            {
                SECTION_TITLE: 1,
                SECTION_LEARNING_OUTCOMES: 1,
                SECTION_COURSE_CONTENTS: 1,
                SECTION_READINGS: 1,
            }
        )
        embeddings = {
            course_a.sha256: {
                SECTION_TITLE: np.array([1.0, 0.0]),
                SECTION_LEARNING_OUTCOMES: None,
                SECTION_COURSE_CONTENTS: np.array([0.0, 1.0]),
                SECTION_READINGS: None,
            },
            course_b.sha256: {
                SECTION_TITLE: np.array([1.0, 0.0]),
                SECTION_LEARNING_OUTCOMES: np.array([1.0, 0.0]),
                SECTION_COURSE_CONTENTS: np.array([0.0, 1.0]),
                SECTION_READINGS: None,
            },
        }

        ab = compute_pair_similarity(course_a, course_b, embeddings, weights, "model")
        ba = compute_pair_similarity(course_b, course_a, embeddings, weights, "model")

        self.assertAlmostEqual(ab["weighted_similarity"], 1.0)
        self.assertAlmostEqual(ba["weighted_similarity"], 1.0)
        self.assertAlmostEqual(ab["distance"], 0.0)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import pathlib
import tempfile
import unittest

import numpy as np
import yaml

from clustering.course_io import CourseRecord
from clustering.reports import course_short_label, write_cluster_courses_short_yaml


def course(course_id: str, title: str, teachers: list[dict[str, str]] | None = None) -> CourseRecord:
    return CourseRecord(
        course_id=course_id,
        path=pathlib.Path(f"course-{course_id}.yml"),
        sha256=course_id * 64,
        title=title,
        sections={},
        section_languages={},
        raw={"teachers": teachers or []},
    )


class TestClusteringReports(unittest.TestCase):
    def test_course_short_label_uses_course_id_and_title(self):
        self.assertEqual(course_short_label(course("00269", "Electronics")), "00269 – Electronics")

    def test_short_cluster_courses_uses_email_handles_sorted(self):
        courses = [
            course(
                "00269",
                "Electronics",
                [
                    {"email": "zeta.teacher@unibo.it"},
                    {"email": "alpha.teacher@unibo.it"},
                    {"email": "alpha.teacher@unibo.it"},
                ],
            ),
            course("00819-A", "Programming", [{"email": "beta.teacher@unibo.it"}]),
        ]
        labels = np.asarray([3, 3], dtype=int)
        summary = {3: {"cluster_name": "Electronics Programming (3)"}}

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = pathlib.Path(tmp_dir) / "cluster_courses.short.yml"
            write_cluster_courses_short_yaml(path, courses, labels, summary)
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))

        self.assertEqual(
            payload,
            {
                "Electronics Programming (3)": {
                    "size": 2,
                    "courses": {
                        "00269": "Electronics | alpha.teacher, zeta.teacher",
                        "00819-A": "Programming | beta.teacher",
                    },
                }
            },
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import hashlib
import pathlib
import tempfile
import unittest

import yaml

from clustering.cache import file_sha256
from clustering.course_io import load_course, parse_course_id_from_filename


class TestClusteringCourseIo(unittest.TestCase):
    def test_parses_course_id_from_filename(self):
        self.assertEqual(parse_course_id_from_filename("course-00819-B.yml"), "00819-B")
        self.assertEqual(parse_course_id_from_filename(pathlib.Path("course-C7674.yml")), "C7674")

    def test_computes_file_sha256(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = pathlib.Path(tmp_dir) / "sample.yml"
            path.write_text("hello\n", encoding="utf-8")

            self.assertEqual(file_sha256(path), hashlib.sha256(b"hello\n").hexdigest())

    def test_load_course_uses_filename_id_when_yaml_id_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = pathlib.Path(tmp_dir) / "course-00819-B.yml"
            path.write_text(
                yaml.safe_dump({"course_title": {"name": "Programming"}}, sort_keys=False),
                encoding="utf-8",
            )

            course = load_course(path)

        self.assertEqual(course.course_id, "00819-B")
        self.assertEqual(course.title, "Programming")

    def test_load_course_uses_filename_id_even_when_yaml_id_differs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = pathlib.Path(tmp_dir) / "course-00819-B.yml"
            path.write_text(
                yaml.safe_dump({"course_title": {"id": "00819", "name": "Programming"}}, sort_keys=False),
                encoding="utf-8",
            )

            course = load_course(path)

        self.assertEqual(course.course_id, "00819-B")


if __name__ == "__main__":
    unittest.main()

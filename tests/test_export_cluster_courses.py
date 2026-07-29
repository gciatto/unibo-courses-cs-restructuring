from __future__ import annotations

import csv
import pathlib
import tempfile
import unittest

import yaml

from clustering.export_cluster_courses import (
    FIELDNAMES,
    _preferred_course_url,
    course_scope,
    export_cluster_courses,
)


def teacher(affiliation: str, url: str = "", italian_url: str = "") -> dict:
    urls = {key: value for key, value in (("en", url), ("it", italian_url)) if value}
    return {
        "affiliation": affiliation,
        "ssd": {"name": "INFO-01/A"},
        "role": ["associate professor"],
        "modules": [{"syllabus_urls": urls}],
    }


def programme(department: str, code: str = "0001", duration: int = 3) -> dict:
    return {
        "code": code,
        "name": {"en": f"Programme {code}"},
        "duration": duration,
        "department": department,
    }


class TestExportClusterCourses(unittest.TestCase):
    def test_course_scope_covers_all_categories(self):
        self.assertEqual(course_scope([teacher("disi")], [programme("disi")]), "internal")
        self.assertEqual(
            course_scope([teacher("disi"), teacher("difa")], [programme("disi"), programme("difa")]),
            "weak_internal",
        )
        self.assertEqual(course_scope([teacher("DISI")], [programme("difa")]), "service")
        self.assertEqual(course_scope([teacher("difa")], [programme(" DiSi ")]), "borrow")
        self.assertEqual(course_scope([teacher("difa")], [programme("difa")]), "external")

    def test_preferred_url_uses_frequency_tie_break_and_italian_fallback(self):
        teachers = [
            teacher("disi", "https://example.test/b"),
            teacher("disi", "https://example.test/a"),
            teacher("disi", "https://example.test/b"),
        ]
        self.assertEqual(_preferred_course_url(teachers), "https://example.test/b")
        tied = [teacher("disi", "https://example.test/b"), teacher("disi", "https://example.test/a")]
        self.assertEqual(_preferred_course_url(tied), "https://example.test/a")
        italian = [teacher("disi", italian_url="https://example.test/it")]
        self.assertEqual(_preferred_course_url(italian), "https://example.test/it")

    def test_exports_excel_ready_csv_with_expected_values(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            course_path = root / "course-001.yml"
            course_path.write_text(
                yaml.safe_dump(
                    {
                        "year": 2025,
                        "credits": [12, 6, 12, 1.5],
                        "ssds": ["INF/01", "INF/01"],
                        "languages": ["Italian", "English"],
                        "campi": ["Cesena", "Bologna"],
                        "teachers": [
                            teacher("disi", "https://example.test/en"),
                            {
                                **teacher("DISI", "https://example.test/en"),
                                "role": ["full professor", "associate professor"],
                            },
                        ],
                        "course_title": {"id": "001", "name": 'Course "One"'},
                        "programmes": [
                            programme("disi", "0002", 2),
                            programme("disi", "0001", 3),
                            programme("disi", "0003", 5),
                        ],
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            input_path = root / "clusters.yml"
            input_path.write_text(
                yaml.safe_dump(
                    {
                        "Cluster Name (7)": {
                            "index": 7,
                            "courses": [{
                                "path": str(course_path),
                                "id": "001",
                                "name": "Fallback",
                                "teachers": ["z.teacher", "a.teacher", "a.teacher"],
                            }],
                        }
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            output_path = export_cluster_courses(input_path)

            self.assertEqual(output_path, input_path.resolve().with_suffix(".csv"))
            raw = output_path.read_bytes()
            self.assertTrue(raw.startswith(b"\xef\xbb\xbf"))
            text = raw.decode("utf-8-sig").splitlines()
            self.assertTrue(text[0].startswith('"cluster_id";"cluster_name";'))
            self.assertIn('"Course ""One"""', text[1])
            with output_path.open(newline="", encoding="utf-8-sig") as stream:
                rows = list(csv.DictReader(stream, delimiter=";", quotechar='"'))

        self.assertEqual(list(rows[0]), FIELDNAMES)
        self.assertEqual(rows[0]["teachers"], "a.teacher, z.teacher")
        self.assertEqual(rows[0]["campus"], "Bologna, Cesena")
        self.assertEqual(rows[0]["credits"], "1,5, 6, 12")
        self.assertEqual(rows[0]["programmes_types"], "LM, LMCU, LT")
        self.assertEqual(rows[0]["course_scope"], "internal")
        self.assertEqual(rows[0]["course_url"], "https://example.test/en")

    def test_explicit_output_path_is_used(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            course_path = root / "course.yml"
            course_path.write_text("course_title: {name: Test}\n", encoding="utf-8")
            input_path = root / "clusters.yml"
            input_path.write_text(
                yaml.safe_dump({"Cluster": {"index": 1, "courses": [{"path": str(course_path), "id": "1"}]}}),
                encoding="utf-8",
            )
            destination = root / "custom.csv"
            self.assertEqual(export_cluster_courses(input_path, destination), destination)
            self.assertTrue(destination.exists())

    def test_rejects_cluster_without_index(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = pathlib.Path(tmp_dir) / "clusters.yml"
            input_path.write_text("Cluster:\n  courses: []\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing 'index'"):
                export_cluster_courses(input_path)


class TestExportClusterCoursesIntegration(unittest.TestCase):
    def test_repository_sample_exports_293_courses(self):
        input_path = pathlib.Path(
            "data/clusters/runs/2025-20260715-120359-spectral/cluster_courses.yml"
        )
        if not input_path.exists():
            self.skipTest("Repository sample data is not available")
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = pathlib.Path(tmp_dir) / "courses.csv"
            export_cluster_courses(input_path, output_path)
            with output_path.open(newline="", encoding="utf-8-sig") as stream:
                rows = list(csv.DictReader(stream, delimiter=";"))
        self.assertEqual(len(rows), 293)
        self.assertTrue(all(row["cluster_id"] and row["course_id"] for row in rows))


if __name__ == "__main__":
    unittest.main()

import pathlib
import tempfile
import unittest

import yaml

from merge_teachings import merge_courses_tree


COMMON_SYLLABUS = {
    "en": {
        "url": "https://example.invalid/en",
        "title": "Example English syllabus",
        "contents": {"Learning outcomes": "Example"},
    },
}


def write_yaml(path: pathlib.Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")


class TestMergeTeachings(unittest.TestCase):
    def test_merges_course_files_and_creates_symlinks(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            courses_dir = pathlib.Path(tmp_dir) / "courses"

            write_yaml(
                courses_dir / "a.teacher" / "2025" / "teaching-111.yml",
                {
                    "year": 2025,
                    "url": "https://example.invalid/course/1",
                    "credits": 6,
                    "ssd": "INF/01",
                    "language": "English",
                    "teaching_mode": "In-person",
                    "teacher": {
                        "id": "1",
                        "name": "Alice Teacher",
                        "email": "a.teacher@unibo.it",
                        "website": "https://example.invalid/a.teacher",
                        "role": "associate professor",
                        "affiliation": "dit",
                        "ssd": {"name": "INFO-01/A", "description": "Informatica"},
                    },
                    "course_title": {
                        "id": "91258",
                        "name": "NATURAL LANGUAGE PROCESSING",
                        "details": ["Module A", "6 cfu"],
                    },
                    "integrated_course": "Integrated course",
                    "campus": "Bologna",
                    "programme": "LM Example",
                    "syllabus": COMMON_SYLLABUS,
                },
            )
            write_yaml(
                courses_dir / "b.teacher" / "2025" / "teaching-222.yml",
                {
                    "year": 2025,
                    "url": "https://example.invalid/course/1",
                    "credits": 6,
                    "ssd": "INF/01",
                    "language": "English",
                    "teaching_mode": "In-person",
                    "teacher": {
                        "id": "2",
                        "name": "Bob Teacher",
                        "email": "b.teacher@unibo.it",
                        "website": "https://example.invalid/b.teacher",
                        "role": "assistant professor",
                        "affiliation": "dit",
                        "ssd": {"name": "INFO-01/A", "description": "Informatica"},
                    },
                    "course_title": {
                        "id": "91258",
                        "name": "NATURAL LANGUAGE PROCESSING",
                        "details": ["Module B"],
                    },
                    "integrated_course": "Integrated course",
                    "campus": "Bologna",
                    "programme": "LM Example",
                    "syllabus": COMMON_SYLLABUS,
                },
            )

            merged_count, symlink_count = merge_courses_tree(courses_dir)

            self.assertEqual(merged_count, 1)
            self.assertEqual(symlink_count, 2)

            merged_path = courses_dir / ".files" / "2025" / "course-91258.yml"
            self.assertTrue(merged_path.exists())

            merged_payload = yaml.safe_load(merged_path.read_text(encoding="utf-8"))
            self.assertEqual(
                merged_payload["course_title"],
                {"id": "91258", "name": "NATURAL LANGUAGE PROCESSING"},
            )
            self.assertEqual(len(merged_payload["teachers"]), 2)

            # top-level url must not appear in merged file
            self.assertNotIn("url", merged_payload)

            # syllabus pages must not contain url
            for page in merged_payload["syllabus"].values():
                self.assertNotIn("url", page)

            # Check new list fields
            self.assertEqual(merged_payload["credits"], [6])
            self.assertEqual(merged_payload["ssds"], ["INF/01"])
            self.assertEqual(merged_payload["languages"], ["English"])
            self.assertEqual(merged_payload["teaching_modes"], ["In-person"])
            self.assertEqual(merged_payload["campi"], ["Bologna"])
            self.assertEqual(merged_payload["programmes"], ["LM Example"])
            self.assertEqual(merged_payload["schedules"], [])

            modules_by_email = {
                teacher["email"]: teacher["module"]
                for teacher in merged_payload["teachers"]
            }
            self.assertEqual(
                modules_by_email["a.teacher@unibo.it"],
                {
                    "teaching_id": "111",
                    "url": "https://example.invalid/course/1",
                    "syllabus_urls": {"en": "https://example.invalid/en"},
                    "details": ["Module A", "6 cfu"],
                    "credits": 6,
                    "campus": "Bologna",
                    "programme": "LM Example",
                    "ssd": "INF/01",
                    "language": "English",
                    "teaching_mode": "In-person",
                },
            )
            self.assertEqual(
                modules_by_email["b.teacher@unibo.it"],
                {
                    "teaching_id": "222",
                    "url": "https://example.invalid/course/1",
                    "syllabus_urls": {"en": "https://example.invalid/en"},
                    "details": ["Module B"],
                    "credits": 6,
                    "campus": "Bologna",
                    "programme": "LM Example",
                    "ssd": "INF/01",
                    "language": "English",
                    "teaching_mode": "In-person",
                },
            )

            first_link = courses_dir / "a.teacher" / "2025" / "course-91258.yml"
            second_link = courses_dir / "b.teacher" / "2025" / "course-91258.yml"
            self.assertTrue(first_link.is_symlink())
            self.assertTrue(second_link.is_symlink())
            self.assertEqual(first_link.resolve(), merged_path.resolve())
            self.assertEqual(second_link.resolve(), merged_path.resolve())


if __name__ == "__main__":
    unittest.main()

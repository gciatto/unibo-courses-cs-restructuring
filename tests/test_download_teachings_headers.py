import csv
import io
import pathlib
import tempfile
import unittest
from unittest.mock import patch

import yaml

from _utils import build_programme_lookup
from _utils import ProgrammeLookup
from download_teachings import CourseDetails, SyllabusPage, build_metadata, ensure_expected_columns, process_row


OLD_HEADER = (
    "contact_uid,contact_name,contact_email,sito_web,didattica_url,course_title,"
    "course_url,integrated_course,campus,degree_course,lesson_period,schedule_url,virtuale_url\n"
)

NEW_HEADER = (
    "contact_uid,contact_name,contact_email,teacher_website,teachings_url,course_title,"
    "course_url,module_of,campus,degree_programme,lesson_period,schedule_url,virtuale_url\n"
)


class TestDownloadTeachingsHeaders(unittest.TestCase):
    def test_accepts_old_and_new_course_header_schemas(self):
        for header in (OLD_HEADER, NEW_HEADER):
            with self.subTest(header=header):
                reader = csv.DictReader(io.StringIO(header))
                ensure_expected_columns(reader)

    def test_skips_titled_rows_without_course_url(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            status, message = process_row(
                row_index=2,
                row={
                    "contact_uid": "1",
                    "contact_name": "Example Teacher",
                    "contact_email": "example.teacher@unibo.it",
                    "teacher_website": "https://www.unibo.it/sitoweb/example.teacher",
                    "teachings_url": "https://www.unibo.it/sitoweb/example.teacher/teachings/2025",
                    "course_title": "12345 - Non-clickable Teaching Header",
                    "course_url": "",
                    "module_of": "",
                    "campus": "Bologna",
                    "degree_programme": "Second cycle degree programme (LM) in Example",
                    "lesson_period": "Lesson period: from January 1, 2026 to January 31, 2026",
                    "schedule_url": "",
                    "virtuale_url": "",
                },
                output_dir=pathlib.Path(tmp_dir),
                whitelist=[],
                blacklist=[],
                timeout=1.0,
                max_retries=0,
                initial_backoff=0.0,
                backoff_multiplier=1.0,
                max_backoff=0.0,
                programme_lookup=ProgrammeLookup(),
            )

        self.assertEqual(status, "skipped")
        self.assertIn("non-downloadable teaching header", message)

    def test_build_metadata_uses_new_header_aliases(self):
        metadata = build_metadata(
            row={
                "contact_uid": "1",
                "contact_name": "Example Teacher",
                "contact_email": "example.teacher@unibo.it",
                "teacher_website": "https://www.unibo.it/sitoweb/example.teacher",
                "course_title": "12345 - Example Course - 6 cfu",
                "module_of": "Module of Example Integrated Course",
                "campus": "Bologna",
            },
            year=2025,
            url="https://www.unibo.it/en/study/course-unit-catalogue/course-unit/2025/123456",
            details=CourseDetails(),
            syllabus={},
            teacher_role=[],
            teacher_affiliation="",
            teacher_ssd=None,
            programmes=[],
        )

        self.assertEqual(metadata.teacher.teacher_website, "https://www.unibo.it/sitoweb/example.teacher")
        self.assertEqual(metadata.integrated_course, "Module of Example Integrated Course")
        self.assertEqual(metadata.course_title.name, "Example Course")

    def test_process_row_resolves_programmes_from_degree_programme_column(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            programmes_dir = root / "programmes" / "2025" / "disi"
            programmes_dir.mkdir(parents=True, exist_ok=True)
            (programmes_dir / "programme-6699.yml").write_text(
                yaml.safe_dump(
                    {
                        "code": "6699",
                        "year": 2025,
                        "name": {
                            "it": "Ingegneria e scienze informatiche",
                            "en": "Computer Science and Engineering",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            programme_lookup = build_programme_lookup(root / "programmes")
            row = {
                "contact_uid": "1",
                "contact_name": "Example Teacher",
                "contact_email": "example.teacher@unibo.it",
                "teacher_website": "",
                "teachings_url": "https://www.unibo.it/sitoweb/example.teacher/teachings/2025",
                "course_title": "12345 - Example Course - 6 cfu",
                "course_url": "https://www.unibo.it/en/study/course-unit-catalogue/course-unit/2025/123456",
                "module_of": "",
                "campus": "Bologna",
                "degree_programme": "Second cycle degree programme (LM) in Computer Science and Engineering (cod. 6699)",
                "lesson_period": "",
                "schedule_url": "",
                "virtuale_url": "",
            }

            with patch(
                "download_teachings.discover_language_urls",
                return_value={"it": "https://example.invalid/it", "en": "https://example.invalid/en"},
            ), patch(
                "download_teachings.parse_syllabus_page",
                return_value=(
                    SyllabusPage(
                        url="https://example.invalid/it",
                        title="Example",
                        contents={"Conoscenze e abilita da conseguire": "..."},
                    ),
                    "# Syllabus\nText without programme mentions",
                    CourseDetails(),
                ),
            ):
                status, _ = process_row(
                    row_index=2,
                    row=row,
                    output_dir=root / "courses",
                    whitelist=[],
                    blacklist=[],
                    timeout=1.0,
                    max_retries=0,
                    initial_backoff=0.0,
                    backoff_multiplier=1.0,
                    max_backoff=0.0,
                    programme_lookup=programme_lookup,
                )

            self.assertEqual(status, "ok")
            written_file = root / "courses" / "example.teacher" / "2025" / "teaching-123456.yml"
            self.assertTrue(written_file.exists())
            payload = yaml.safe_load(written_file.read_text(encoding="utf-8")) or {}

            programmes = payload.get("programmes")
            self.assertIsInstance(programmes, list)
            self.assertTrue(programmes)
            self.assertEqual(programmes[0].get("code"), "6699")


if __name__ == "__main__":
    unittest.main()

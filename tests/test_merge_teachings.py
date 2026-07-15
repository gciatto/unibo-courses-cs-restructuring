import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

from scraping.merge_teachings import (
    DEFAULT_LEARNING_OUTCOMES_EMBEDDING_MODEL,
    DEFAULT_LEARNING_OUTCOMES_SIMILARITY_BACKEND,
    DEFAULT_LEARNING_OUTCOMES_SIMILARITY_THRESHOLD,
    merge_courses_tree,
    parse_args,
)


def write_yaml(path: pathlib.Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")


def make_syllabus(
    *,
    en_learning_outcomes: str,
    it_learning_outcomes: str,
    en_title: str = "2025/2026 English syllabus",
    it_title: str = "2025/2026 Italian syllabus",
    en_course_contents: str = "Shared English contents",
    it_course_contents: str = "Contenuti condivisi",
) -> dict:
    return {
        "en": {
            "url": "https://example.invalid/en",
            "title": en_title,
            "contents": {
                "Learning outcomes": en_learning_outcomes,
                "Course contents": en_course_contents,
            },
        },
        "it": {
            "url": "https://example.invalid/it",
            "title": it_title,
            "contents": {
                "Conoscenze e abilità da conseguire": it_learning_outcomes,
                "Contenuti": it_course_contents,
            },
        },
    }


def make_teaching_payload(
    *,
    year: int,
    course_id: str,
    course_name: str,
    teaching_id: str,
    teacher_email: str,
    teacher_name: str,
    teacher_slug: str,
    programmes: list[dict],
    syllabus: dict,
    details: list[str] | None = None,
    course_url: str | None = None,
    campus: str = "Bologna",
    ssd: str = "INF/01",
    teaching_mode: str = "In-person",
) -> dict:
    return {
        "year": year,
        "url": course_url or f"https://example.invalid/course/{teaching_id}",
        "credits": 6,
        "ssd": ssd,
        "language": "English",
        "teaching_mode": teaching_mode,
        "teacher": {
            "id": teaching_id,
            "name": teacher_name,
            "email": teacher_email,
            "website": f"https://example.invalid/{teacher_slug}",
            "role": "associate professor",
            "affiliation": "dit",
            "ssd": {"name": "INFO-01/A", "description": "Informatica"},
        },
        "course_title": {
            "id": course_id,
            "name": course_name,
            "details": details or [f"Module {teaching_id}"],
        },
        "integrated_course": "",
        "campus": campus,
        "programmes": programmes,
        "syllabus": syllabus,
    }


def load_merged_payload(courses_dir: pathlib.Path, year: int, course_id: str, suffix: str = "") -> dict:
    path = courses_dir / ".files" / str(year) / f"course-{course_id}{suffix}.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class TestMergeTeachings(unittest.TestCase):
    def test_parse_args_defaults(self):
        with patch.object(sys, "argv", ["merge_teachings.py"]):
            args = parse_args()

        self.assertEqual(args.learning_outcomes_similarity_backend, DEFAULT_LEARNING_OUTCOMES_SIMILARITY_BACKEND)
        self.assertEqual(args.learning_outcomes_similarity_threshold, DEFAULT_LEARNING_OUTCOMES_SIMILARITY_THRESHOLD)
        self.assertEqual(args.learning_outcomes_embedding_model, DEFAULT_LEARNING_OUTCOMES_EMBEDDING_MODEL)

    def test_rejects_different_course_ids(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            courses_dir = pathlib.Path(tmp_dir) / "courses"

            write_yaml(
                courses_dir / "andrea.omicini" / "2025" / "teaching-526532.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="87474",
                    course_name="Distributed Systems",
                    teaching_id="526532",
                    teacher_email="andrea.omicini@unibo.it",
                    teacher_name="Omicini, Andrea",
                    teacher_slug="andrea.omicini",
                    programmes=[{"code": "6673"}, {"code": "5898"}],
                    syllabus=make_syllabus(
                        en_learning_outcomes="Distributed systems outcomes A.",
                        it_learning_outcomes="Esiti di apprendimento A.",
                    ),
                    campus="Cesena",
                ),
            )
            write_yaml(
                courses_dir / "giovanni.ciatto" / "2025" / "teaching-479053.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="93468",
                    course_name="Distributed Software Systems",
                    teaching_id="479053",
                    teacher_email="giovanni.ciatto@unibo.it",
                    teacher_name="Ciatto, Giovanni",
                    teacher_slug="giovanni.ciatto",
                    programmes=[{"code": "5898"}, {"code": "6699"}],
                    syllabus=make_syllabus(
                        en_learning_outcomes="Distributed software systems outcomes B.",
                        it_learning_outcomes="Esiti di apprendimento B.",
                    ),
                    campus="Bologna",
                    teaching_mode="E-learning",
                ),
            )

            merged_count, symlink_count = merge_courses_tree(courses_dir)

            self.assertEqual(merged_count, 2)
            self.assertEqual(symlink_count, 2)
            self.assertTrue((courses_dir / ".files" / "2025" / "course-87474.yml").exists())
            self.assertTrue((courses_dir / ".files" / "2025" / "course-93468.yml").exists())

    def test_merges_same_teaching_id_within_year(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            courses_dir = pathlib.Path(tmp_dir) / "courses"

            write_yaml(
                courses_dir / "a.teacher" / "2025" / "teaching-111.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="91258",
                    course_name="NATURAL LANGUAGE PROCESSING",
                    teaching_id="111",
                    teacher_email="a.teacher@unibo.it",
                    teacher_name="Alice Teacher",
                    teacher_slug="a.teacher",
                    programmes=[{"code": "8888"}],
                    syllabus=make_syllabus(
                        en_learning_outcomes="Learning outcomes A.",
                        it_learning_outcomes="Esiti A.",
                    ),
                ),
            )
            write_yaml(
                courses_dir / "b.teacher" / "2025" / "teaching-111.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="91258",
                    course_name="NATURAL LANGUAGE PROCESSING",
                    teaching_id="111",
                    teacher_email="b.teacher@unibo.it",
                    teacher_name="Bob Teacher",
                    teacher_slug="b.teacher",
                    programmes=[{"code": "9999"}],
                    syllabus=make_syllabus(
                        en_learning_outcomes="Learning outcomes B.",
                        it_learning_outcomes="Esiti B.",
                    ),
                ),
            )

            merged_count, symlink_count = merge_courses_tree(courses_dir)

            self.assertEqual(merged_count, 1)
            self.assertEqual(symlink_count, 2)

            merged_payload = load_merged_payload(courses_dir, 2025, "91258")
            module_ids = [module["teaching_id"] for teacher in merged_payload["teachers"] for module in teacher["modules"]]
            self.assertEqual(module_ids, ["111", "111"])

    def test_merges_same_programme_codes(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            courses_dir = pathlib.Path(tmp_dir) / "courses"

            write_yaml(
                courses_dir / "a.teacher" / "2025" / "teaching-111.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="91258",
                    course_name="NATURAL LANGUAGE PROCESSING",
                    teaching_id="111",
                    teacher_email="a.teacher@unibo.it",
                    teacher_name="Alice Teacher",
                    teacher_slug="a.teacher",
                    programmes=[{"code": "8888"}, {"code": "9999"}],
                    syllabus=make_syllabus(
                        en_learning_outcomes="Learning outcomes A.",
                        it_learning_outcomes="Esiti A.",
                    ),
                ),
            )
            write_yaml(
                courses_dir / "b.teacher" / "2025" / "teaching-222.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="91258",
                    course_name="NATURAL LANGUAGE PROCESSING",
                    teaching_id="222",
                    teacher_email="b.teacher@unibo.it",
                    teacher_name="Bob Teacher",
                    teacher_slug="b.teacher",
                    programmes=[{"code": "9999"}, {"code": "8888"}],
                    syllabus=make_syllabus(
                        en_learning_outcomes="Learning outcomes B.",
                        it_learning_outcomes="Esiti B.",
                    ),
                ),
            )

            merged_count, symlink_count = merge_courses_tree(courses_dir)

            self.assertEqual(merged_count, 1)
            self.assertEqual(symlink_count, 2)

            merged_payload = load_merged_payload(courses_dir, 2025, "91258")
            self.assertEqual([programme["code"] for programme in merged_payload["programmes"]], ["8888", "9999"])

    def test_merges_exact_syllabus_except_learning_outcomes_and_lists_values(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            courses_dir = pathlib.Path(tmp_dir) / "courses"

            write_yaml(
                courses_dir / "a.teacher" / "2025" / "teaching-111.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="91258",
                    course_name="NATURAL LANGUAGE PROCESSING",
                    teaching_id="111",
                    teacher_email="a.teacher@unibo.it",
                    teacher_name="Alice Teacher",
                    teacher_slug="a.teacher",
                    programmes=[{"code": "8888"}],
                    syllabus=make_syllabus(
                        en_learning_outcomes="English outcomes A.",
                        it_learning_outcomes="Esiti italiani A.",
                    ),
                ),
            )
            write_yaml(
                courses_dir / "b.teacher" / "2025" / "teaching-222.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="91258",
                    course_name="NATURAL LANGUAGE PROCESSING",
                    teaching_id="222",
                    teacher_email="b.teacher@unibo.it",
                    teacher_name="Bob Teacher",
                    teacher_slug="b.teacher",
                    programmes=[{"code": "9999"}],
                    syllabus=make_syllabus(
                        en_learning_outcomes="English outcomes B.",
                        it_learning_outcomes="Esiti italiani B.",
                    ),
                ),
            )

            merged_count, symlink_count = merge_courses_tree(courses_dir)

            self.assertEqual(merged_count, 1)
            self.assertEqual(symlink_count, 2)

            merged_payload = load_merged_payload(courses_dir, 2025, "91258")
            self.assertEqual(
                merged_payload["syllabus"]["en"]["contents"]["Learning outcomes"],
                ["English outcomes A.", "English outcomes B."],
            )
            self.assertEqual(
                merged_payload["syllabus"]["it"]["contents"]["Conoscenze e abilità da conseguire"],
                ["Esiti italiani A.", "Esiti italiani B."],
            )

    def test_merges_similar_learning_outcomes_with_threshold_override(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            courses_dir = pathlib.Path(tmp_dir) / "courses"

            write_yaml(
                courses_dir / "a.teacher" / "2025" / "teaching-111.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="91258",
                    course_name="NATURAL LANGUAGE PROCESSING",
                    teaching_id="111",
                    teacher_email="a.teacher@unibo.it",
                    teacher_name="Alice Teacher",
                    teacher_slug="a.teacher",
                    programmes=[{"code": "8888"}],
                    syllabus=make_syllabus(
                        en_title="Syllabus A",
                        it_title="Programma A",
                        en_course_contents="Different English contents A.",
                        it_course_contents="Contenuti diversi A.",
                        en_learning_outcomes="Students learn distributed systems and middleware platforms.",
                        it_learning_outcomes="Gli studenti imparano i sistemi distribuiti e le piattaforme middleware.",
                    ),
                ),
            )
            write_yaml(
                courses_dir / "b.teacher" / "2025" / "teaching-222.yml",
                make_teaching_payload(
                    year=2025,
                    course_id="91258",
                    course_name="NATURAL LANGUAGE PROCESSING",
                    teaching_id="222",
                    teacher_email="b.teacher@unibo.it",
                    teacher_name="Bob Teacher",
                    teacher_slug="b.teacher",
                    programmes=[{"code": "9999"}],
                    syllabus=make_syllabus(
                        en_title="Syllabus B",
                        it_title="Programma B",
                        en_course_contents="Different English contents B.",
                        it_course_contents="Contenuti diversi B.",
                        en_learning_outcomes="Students learn distributed software systems and middleware platforms.",
                        it_learning_outcomes="Gli studenti imparano i sistemi software distribuiti e le piattaforme middleware.",
                    ),
                ),
            )

            merged_count, symlink_count = merge_courses_tree(
                courses_dir,
                learning_outcomes_similarity_backend="text",
                learning_outcomes_similarity_threshold=80.0,
            )

            self.assertEqual(merged_count, 1)
            self.assertEqual(symlink_count, 2)

            merged_payload = load_merged_payload(courses_dir, 2025, "91258")
            self.assertEqual(
                merged_payload["syllabus"]["en"]["contents"]["Learning outcomes"],
                [
                    "Students learn distributed systems and middleware platforms.",
                    "Students learn distributed software systems and middleware platforms.",
                ],
            )
            self.assertEqual(
                merged_payload["syllabus"]["it"]["contents"]["Conoscenze e abilità da conseguire"],
                [
                    "Gli studenti imparano i sistemi distribuiti e le piattaforme middleware.",
                    "Gli studenti imparano i sistemi software distribuiti e le piattaforme middleware.",
                ],
            )


if __name__ == "__main__":
    unittest.main()

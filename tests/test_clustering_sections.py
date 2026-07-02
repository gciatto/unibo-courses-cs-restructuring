from __future__ import annotations

import unittest

from clustering.sections import (
    SECTION_COURSE_CONTENTS,
    SECTION_LEARNING_OUTCOMES,
    SECTION_READINGS,
    SECTION_TITLE,
    extract_similarity_sections,
)


class TestClusteringSections(unittest.TestCase):
    def test_prefers_english_sections_individually(self):
        sections, languages = extract_similarity_sections(
            {
                "course_title": {"name": "Algorithms"},
                "syllabus": {
                    "en": {
                        "contents": {
                            "Learning outcomes": "English outcomes",
                            "Course contents": "English contents",
                        }
                    },
                    "it": {
                        "contents": {
                            "Conoscenze e abilità da conseguire": "Italian outcomes",
                            "Contenuti": "Italian contents",
                            "Testi/Bibliografia": "Italian readings",
                        }
                    },
                },
            }
        )

        self.assertEqual(sections[SECTION_TITLE], "Algorithms")
        self.assertEqual(sections[SECTION_LEARNING_OUTCOMES], "English outcomes")
        self.assertEqual(sections[SECTION_COURSE_CONTENTS], "English contents")
        self.assertEqual(sections[SECTION_READINGS], "Italian readings")
        self.assertEqual(languages[SECTION_READINGS], "it")

    def test_missing_section_is_explicit_none(self):
        sections, languages = extract_similarity_sections({"course_title": {"name": ""}, "syllabus": {}})

        self.assertIsNone(sections[SECTION_TITLE])
        self.assertIsNone(sections[SECTION_LEARNING_OUTCOMES])
        self.assertIsNone(languages[SECTION_LEARNING_OUTCOMES])


if __name__ == "__main__":
    unittest.main()

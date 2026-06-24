import pathlib
import tempfile
import unittest

import yaml

from _utils import build_programme_lookup
from download_teachings import ProgrammeMention, extract_programme_mentions, resolve_programmes


class TestProgrammeMentions(unittest.TestCase):
    def test_extract_programme_mentions_from_same_line(self):
        markdown = (
            "- Corso: Second cycle degree programme (LM) in Digital Transformation Management (cod. 5815)"
            "Also valid for Second cycle degree programme (LM) in "
            "[Computer Science and Engineering (cod. 6699)](https://example.invalid/path)"
        )

        mentions = extract_programme_mentions(markdown)

        self.assertEqual(
            mentions,
            [
                ProgrammeMention(title="Digital Transformation Management", code="5815"),
                ProgrammeMention(title="Computer Science and Engineering", code="6699"),
            ],
        )

    def test_extract_programme_mentions_with_l_and_without_lm_in_it(self):
        markdown = (
            "- Corso: First cycle degree programme (L) in Computer Engineering (cod. 6668)\n"
            "Also valid for First cycle degree programme (L) in "
            "[Computer Engineering (cod. 6668)](https://example.invalid/path)\n"
            "- Corso: Laurea Magistrale in Ingegneria elettronica (cod. 6716)\n"
            "Valido anche per Laurea Magistrale in [Automation Engineering (cod. 8891)](https://example.invalid/path)"
        )

        mentions = extract_programme_mentions(markdown)
        mention_payloads = [item.model_dump() for item in mentions]

        self.assertIn(
            {"title": "Computer Engineering", "code": "6668"},
            mention_payloads,
        )
        self.assertIn(
            {"title": "Ingegneria elettronica", "code": "6716"},
            mention_payloads,
        )
        self.assertIn(
            {"title": "Automation Engineering", "code": "8891"},
            mention_payloads,
        )


class TestProgrammeResolution(unittest.TestCase):
    def test_resolve_programmes_from_lookup(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            programmes_dir = root / "programmes" / "2025" / "disi"
            programmes_dir.mkdir(parents=True, exist_ok=True)

            first_path = programmes_dir / "programme-5815.yml"
            first_path.write_text(
                yaml.safe_dump(
                    {
                        "code": "5815",
                        "year": 2025,
                        "name": {
                            "it": "Gestione della trasformazione digitale",
                            "en": "Digital Transformation Management",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            second_path = programmes_dir / "programme-6699.yml"
            second_path.write_text(
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

            lookup = build_programme_lookup(root / "programmes")
            programmes = resolve_programmes(
                [
                    ProgrammeMention(title="Digital Transformation Management", code="5815"),
                    ProgrammeMention(title="Computer Science and Engineering", code="6699"),
                ],
                year=2025,
                programme_lookup=lookup,
            )

            self.assertEqual(len(programmes), 2)
            self.assertEqual(programmes[0].title, "Digital Transformation Management")
            self.assertEqual(programmes[0].details.get("code"), "5815")
            self.assertEqual(programmes[1].title, "Computer Science and Engineering")
            self.assertEqual(programmes[1].details.get("code"), "6699")

    def test_does_not_fallback_to_title_with_mismatched_code(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            programmes_dir = root / "programmes" / "2025" / "disi"
            programmes_dir.mkdir(parents=True, exist_ok=True)

            only_path = programmes_dir / "programme-6671.yml"
            only_path.write_text(
                yaml.safe_dump(
                    {
                        "code": "6671",
                        "year": 2025,
                        "name": {
                            "it": "Ingegneria dell'automazione",
                            "en": "Automation Engineering",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            lookup = build_programme_lookup(root / "programmes")
            programmes = resolve_programmes(
                [ProgrammeMention(title="Automation Engineering", code="8891")],
                year=2025,
                programme_lookup=lookup,
            )

            self.assertEqual(programmes, [])

    def test_multiple_matches_by_title_are_all_included(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            programmes_dir = root / "programmes" / "2025" / "dei"
            programmes_dir.mkdir(parents=True, exist_ok=True)

            first_path = programmes_dir / "programme-6671.yml"
            first_path.write_text(
                yaml.safe_dump(
                    {
                        "code": "6671",
                        "year": 2025,
                        "name": {
                            "it": "Ingegneria dell'automazione",
                            "en": "Automation Engineering",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            second_path = programmes_dir / "programme-9217.yml"
            second_path.write_text(
                yaml.safe_dump(
                    {
                        "code": "9217",
                        "year": 2025,
                        "name": {
                            "it": "Ingegneria dell'automazione",
                            "en": "Automation Engineering",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            lookup = build_programme_lookup(root / "programmes")
            programmes = resolve_programmes(
                [ProgrammeMention(title="Automation Engineering", code="")],
                year=2025,
                programme_lookup=lookup,
            )

            self.assertEqual(len(programmes), 2)
            self.assertEqual({item.details.get("code") for item in programmes}, {"6671", "9217"})


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import re
import unicodedata
from typing import Any


SECTION_TITLE = "course title.name"
SECTION_LEARNING_OUTCOMES = "Learning outcomes"
SECTION_COURSE_CONTENTS = "Course contents"
SECTION_READINGS = "Readings/Bibliography"

SECTION_NAMES = [
    SECTION_TITLE,
    SECTION_LEARNING_OUTCOMES,
    SECTION_COURSE_CONTENTS,
    SECTION_READINGS,
]

ENGLISH_LABELS = {
    SECTION_LEARNING_OUTCOMES: ("Learning outcomes",),
    SECTION_COURSE_CONTENTS: ("Course contents",),
    SECTION_READINGS: ("Readings/Bibliography",),
}

ITALIAN_LABELS = {
    SECTION_LEARNING_OUTCOMES: ("Conoscenze e abilita da conseguire", "Conoscenze e abilità da conseguire"),
    SECTION_COURSE_CONTENTS: ("Contenuti",),
    SECTION_READINGS: ("Testi/Bibliografia",),
}


def normalize_label(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.replace("\xa0", " "))
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = re.sub(r"\s+", " ", normalized).strip().strip(":").lower()
    return normalized


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    value = value.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    lines = [line.rstrip() for line in value.splitlines()]
    return "\n".join(lines).strip()


def is_missing_text(value: Any) -> bool:
    return normalize_text(value) == ""


def _contents_for_language(payload: dict[str, Any], language: str) -> dict[str, Any]:
    syllabus = payload.get("syllabus")
    if not isinstance(syllabus, dict):
        return {}
    page = syllabus.get(language)
    if not isinstance(page, dict):
        return {}
    contents = page.get("contents")
    return contents if isinstance(contents, dict) else {}


def _content_by_label(contents: dict[str, Any], aliases: tuple[str, ...]) -> str | None:
    if not contents:
        return None

    normalized_aliases = {normalize_label(alias) for alias in aliases}
    for label, value in contents.items():
        if normalize_label(str(label)) in normalized_aliases:
            text = normalize_text(value)
            return text if text else None
    return None


def extract_similarity_sections(
    payload: dict[str, Any],
) -> tuple[dict[str, str | None], dict[str, str | None]]:
    """Extract title and selected syllabus sections with English-first fallback."""
    course_title = payload.get("course_title")
    if isinstance(course_title, dict):
        title = normalize_text(course_title.get("name"))
    else:
        title = ""

    sections: dict[str, str | None] = {
        SECTION_TITLE: title or None,
    }
    section_languages: dict[str, str | None] = {
        SECTION_TITLE: "metadata" if title else None,
    }

    english_contents = _contents_for_language(payload, "en")
    italian_contents = _contents_for_language(payload, "it")

    for section_name in SECTION_NAMES[1:]:
        english_text = _content_by_label(english_contents, ENGLISH_LABELS[section_name])
        if english_text:
            sections[section_name] = english_text
            section_languages[section_name] = "en"
            continue

        italian_text = _content_by_label(italian_contents, ITALIAN_LABELS[section_name])
        sections[section_name] = italian_text
        section_languages[section_name] = "it" if italian_text else None

    return sections, section_languages

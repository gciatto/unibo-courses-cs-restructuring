from __future__ import annotations

import argparse
import csv
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from _utils import DIR_DATA, DEFAULT_HEADERS


DEFAULT_INPUT_CSV = DIR_DATA / "contacts.csv"
DEFAULT_OUTPUT_CSV = DIR_DATA / "course_headers.csv"


@dataclass(frozen=True)
class TeachingCourse:
    contact_uid: str
    contact_name: str
    contact_email: str
    sito_web: str
    didattica_url: str
    course_title: str
    course_url: str
    integrated_course: str
    campus: str
    degree_course: str
    lesson_period: str
    schedule_url: str
    virtuale_url: str


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def first_text(node: Tag, selector: str) -> str:
    element = node.select_one(selector)
    if element is None:
        return ""
    return clean_text(element.get_text(" ", strip=True))


def read_contact_rows(input_csv: Path, limit: int) -> list[dict[str, str]]:
    with input_csv.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
    return rows[:limit] if limit > 0 else rows


def build_didattica_url(sito_web: str) -> str:
    return sito_web.rstrip("/") + "/didattica"


def fetch_page(session: requests.Session, url: str, timeout: int) -> requests.Response | None:
    response = session.get(url, timeout=timeout)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response


def extract_table_value(block: Tag, header_label: str) -> str:
    for row in block.select("table tr"):
        header = first_text(row, "th").rstrip(":")
        if header == header_label:
            return first_text(row, "td")
    return ""


def extract_link_wrapper_url(block: Tag, label_fragment: str, base_url: str) -> str:
    for anchor in block.select(".link-wrapper a"):
        text = clean_text(anchor.get_text(" ", strip=True))
        if label_fragment.lower() in text.lower():
            href = anchor.get("href", "").strip()
            return urljoin(base_url, href) if href else ""
    return ""


def parse_course_block(block: Tag, contact_row: dict[str, str], didattica_url: str) -> TeachingCourse:
    course_anchor = block.select_one("h4 a")
    course_title = clean_text(course_anchor.get_text(" ", strip=True)) if course_anchor else ""
    course_href = course_anchor.get("href", "").strip() if course_anchor else ""
    course_url = urljoin(didattica_url, course_href) if course_href else ""

    integrated_course = ""
    lesson_period = ""
    for child in block.find_all(recursive=False):
        if child.name == "p":
            text = clean_text(child.get_text(" ", strip=True))
        elif child.name == "div":
            text = first_text(child, "p")
        else:
            continue

        if not text:
            continue
        if text.startswith("Componente del corso integrato"):
            integrated_course = text
        elif text.startswith("Periodo delle lezioni:"):
            lesson_period = text

    return TeachingCourse(
        contact_uid=contact_row.get("uid", ""),
        contact_name=contact_row.get("nome", ""),
        contact_email=contact_row.get("email", ""),
        sito_web=contact_row.get("sito_web", ""),
        didattica_url=didattica_url,
        course_title=course_title,
        course_url=course_url,
        integrated_course=integrated_course,
        campus=extract_table_value(block, "Campus"),
        degree_course=extract_table_value(block, "Corso"),
        lesson_period=lesson_period,
        schedule_url=extract_link_wrapper_url(block, "Orario delle lezioni", didattica_url),
        virtuale_url=extract_link_wrapper_url(block, "Risorse didattiche su Virtuale", didattica_url),
    )


def parse_teaching_courses(html: str, contact_row: dict[str, str], didattica_url: str) -> list[TeachingCourse]:
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select("div.linked-data-list")
    return [parse_course_block(block, contact_row, didattica_url) for block in blocks]


def save_courses_csv(courses: Iterable[TeachingCourse], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    rows = list(courses)

    with destination.open("w", newline="", encoding="utf-8") as csv_file:
        fieldnames = list(TeachingCourse.__dataclass_fields__.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for course in rows:
            writer.writerow(asdict(course))


def initialize_courses_csv(destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(TeachingCourse.__dataclass_fields__.keys())
    with destination.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()


def append_courses_csv(courses: Iterable[TeachingCourse], destination: Path) -> None:
    fieldnames = list(TeachingCourse.__dataclass_fields__.keys())
    with destination.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        for course in courses:
            writer.writerow(asdict(course))
        csv_file.flush()


def print_courses(courses: list[TeachingCourse]) -> None:
    for index, course in enumerate(courses, start=1):
        print(f"[{index}] {course.course_title}")
        print(f"  docente_uid: {course.contact_uid}")
        print(f"  docente_nome: {course.contact_name}")
        print(f"  docente_email: {course.contact_email}")
        print(f"  sito_web: {course.sito_web}")
        print(f"  didattica_url: {course.didattica_url}")
        print(f"  integrated_course: {course.integrated_course}")
        print(f"  campus: {course.campus}")
        print(f"  degree_course: {course.degree_course}")
        print(f"  lesson_period: {course.lesson_period}")
        print(f"  course_url: {course.course_url}")
        print(f"  schedule_url: {course.schedule_url}")
        print(f"  virtuale_url: {course.virtuale_url}")
        print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estrae i corsi dalle pagine /didattica dei siti docente Unibo."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_CSV,
        help="Percorso del CSV dei contatti.",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_CSV,
        help="Percorso del CSV dei corsi.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Numero massimo di righe del CSV contatti da processare. Usa 0 per tutte.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Pausa opzionale tra richieste HTTP.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Timeout della richiesta HTTP in secondi.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    contact_rows = read_contact_rows(input_path, args.limit)
    session = build_session()
    courses: list[TeachingCourse] = []
    initialize_courses_csv(output_path)

    for index, contact_row in enumerate(contact_rows, start=1):
        sito_web = contact_row.get("sito_web", "").strip()
        contact_name = contact_row.get("nome", "")

        if not sito_web:
            print(f"[{index}] {contact_name}: sito_web assente, salto")
            continue

        didattica_url = build_didattica_url(sito_web)
        time.sleep(max(args.delay, 0))

        try:
            response = fetch_page(session, didattica_url, args.timeout)
        except requests.RequestException as exc:
            print(f"[{index}] {contact_name}: errore su {didattica_url} -> {exc}")
            continue

        if response is None:
            print(f"[{index}] {contact_name}: {didattica_url} restituisce 404, salto")
            continue

        parsed_courses = parse_teaching_courses(response.text, contact_row, didattica_url)
        if not parsed_courses:
            print(f"[{index}] {contact_name}: nessun corso trovato in {didattica_url}")
            continue

        print(f"[{index}] {contact_name}: trovati {len(parsed_courses)} corsi")
        courses.extend(parsed_courses)
        append_courses_csv(parsed_courses, output_path)

    print()
    print(f"Trovati {len(courses)} corsi in {len(contact_rows)} righe del CSV contatti")
    print(f"CSV salvato in: {output_path}")
    print()
    print_courses(courses)


if __name__ == "__main__":
    main()

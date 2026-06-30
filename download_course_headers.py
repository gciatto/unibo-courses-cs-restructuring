from __future__ import annotations

import argparse
import csv
import datetime
import logging
import pathlib
import shlex
import sys
import time
from dataclasses import asdict, dataclass
from typing import Iterable
from urllib.parse import unquote, urljoin, urlsplit

from bs4 import BeautifulSoup
from bs4.element import Tag

from _utils import (
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_CACHE_DIR,
    DEFAULT_DOWNLOAD_TIMEOUT,
    DEFAULT_INITIAL_BACKOFF,
    DEFAULT_MAX_BACKOFF,
    DEFAULT_MAX_RETRIES,
    DIR_DATA,
    configure_logging,
    download_html_page,
)


DEFAULT_INPUT_CSV = DIR_DATA / "contacts.csv"
DEFAULT_OUTPUT_CSV = DIR_DATA / "course_headers.csv"
DEFAULT_YEAR = datetime.date.today().year - 1
TEACHINGS_URL_TEMPLATE = "https://www.unibo.it/sitoweb/{teacher}/teachings/{year}"

LOGGER = logging.getLogger(pathlib.Path(__file__).stem)


@dataclass(frozen=True)
class TeachingCourse:
    contact_uid: str
    contact_name: str
    contact_email: str
    teacher_website: str
    teachings_url: str
    course_title: str
    course_url: str
    module_of: str
    campus: str
    degree_programme: str
    lesson_period: str
    schedule_url: str
    virtuale_url: str


def clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split()).strip()


def first_text(node: Tag, selector: str) -> str:
    element = node.select_one(selector)
    if element is None:
        return ""
    return clean_text(element.get_text(" ", strip=True))


def read_contact_rows(input_csv: pathlib.Path, limit: int) -> list[dict[str, str]]:
    with input_csv.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
    return rows[:limit] if limit > 0 else rows


def extract_teacher_slug(teacher_website: str) -> str:
    parsed = urlsplit(teacher_website.strip())
    parts = [unquote(part) for part in parsed.path.split("/") if part]

    if "sitoweb" in parts:
        sitoweb_index = parts.index("sitoweb")
        if sitoweb_index + 1 < len(parts):
            return parts[sitoweb_index + 1]

    return parts[-1] if parts else ""


def build_teachings_url(teacher_website: str, year: int) -> str:
    teacher = extract_teacher_slug(teacher_website)
    if not teacher:
        return ""
    return TEACHINGS_URL_TEMPLATE.format(teacher=teacher, year=year)


def extract_table_value(block: Tag, header_label: str) -> str:
    expected = header_label.rstrip(":").lower()
    for row in block.select("table tr"):
        header = first_text(row, "th").rstrip(":").lower()
        if header == expected:
            return first_text(row, "td")
    return ""


def extract_link_wrapper_url(block: Tag, label_fragment: str, base_url: str) -> str:
    expected = label_fragment.lower()
    for anchor in block.select(".link-wrapper a"):
        text = clean_text(anchor.get_text(" ", strip=True))
        if expected in text.lower():
            href = anchor.get("href", "").strip()
            return urljoin(base_url, href) if href else ""
    return ""


def direct_child_texts(block: Tag) -> Iterable[str]:
    for child in block.find_all(recursive=False):
        if child.name == "p":
            text = clean_text(child.get_text(" ", strip=True))
        elif child.name == "div":
            text = first_text(child, "p")
        else:
            continue

        if text:
            yield text


def parse_course_block(
    block: Tag,
    contact_row: dict[str, str],
    teachings_url: str,
) -> TeachingCourse | None:
    heading = block.select_one("h4")
    course_title = clean_text(heading.get_text(" ", strip=True)) if heading else ""
    if not course_title:
        return None

    course_anchor = heading.select_one("a")
    course_href = course_anchor.get("href", "").strip() if course_anchor else ""
    course_url = urljoin(teachings_url, course_href) if course_href else ""

    module_of = ""
    lesson_period = ""
    for text in direct_child_texts(block):
        if text.startswith("Module of "):
            module_of = text
        elif text.startswith("Lesson period:"):
            lesson_period = text

    return TeachingCourse(
        contact_uid=contact_row.get("uid", ""),
        contact_name=contact_row.get("name", ""),
        contact_email=contact_row.get("email", ""),
        teacher_website=contact_row.get("website", ""),
        teachings_url=teachings_url,
        course_title=course_title,
        course_url=course_url,
        module_of=module_of,
        campus=extract_table_value(block, "Campus"),
        degree_programme=extract_table_value(block, "Degree programme"),
        lesson_period=lesson_period,
        schedule_url=extract_link_wrapper_url(block, "Course timetable", teachings_url),
        virtuale_url=extract_link_wrapper_url(block, "Teaching resources on Virtuale", teachings_url),
    )


def parse_teaching_courses(
    html: str,
    contact_row: dict[str, str],
    teachings_url: str,
) -> list[TeachingCourse]:
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select("div.linked-data-list")
    courses: list[TeachingCourse] = []

    for block in blocks:
        course = parse_course_block(block, contact_row, teachings_url)
        if course is not None:
            courses.append(course)

    return courses


def fieldnames() -> list[str]:
    return list(TeachingCourse.__dataclass_fields__.keys())


def save_courses_csv(courses: Iterable[TeachingCourse], destination: pathlib.Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    rows = list(courses)

    with destination.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames())
        writer.writeheader()
        for course in rows:
            writer.writerow(asdict(course))


def initialize_courses_csv(destination: pathlib.Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames())
        writer.writeheader()


def append_courses_csv(courses: Iterable[TeachingCourse], destination: pathlib.Path) -> None:
    with destination.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames())
        for course in courses:
            writer.writerow(asdict(course))
        csv_file.flush()


def log_courses(courses: list[TeachingCourse]) -> None:
    for index, course in enumerate(courses, start=1):
        LOGGER.info(
            "[%s] %s\n"
            "  contact_uid: %s\n"
            "  contact_name: %s\n"
            "  contact_email: %s\n"
            "  teacher_website: %s\n"
            "  teachings_url: %s\n"
            "  module_of: %s\n"
            "  campus: %s\n"
            "  degree_programme: %s\n"
            "  lesson_period: %s\n"
            "  course_url: %s\n"
            "  schedule_url: %s\n"
            "  virtuale_url: %s",
            index,
            course.course_title,
            course.contact_uid,
            course.contact_name,
            course.contact_email,
            course.teacher_website,
            course.teachings_url,
            course.module_of,
            course.campus,
            course.degree_programme,
            course.lesson_period,
            course.course_url,
            course.schedule_url,
            course.virtuale_url,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl UniBo English teacher teaching pages into a course header CSV.",
    )
    parser.add_argument(
        "--input",
        type=pathlib.Path,
        default=DEFAULT_INPUT_CSV,
        help=f"Input contacts CSV path (default: {DEFAULT_INPUT_CSV}).",
    )
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output course header CSV path (default: {DEFAULT_OUTPUT_CSV}).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=DEFAULT_YEAR,
        help=f"Academic year in teaching page URLs (default: {DEFAULT_YEAR}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of contact rows to process. Use 0 for all rows.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Optional pause before each HTTP request in seconds.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_DOWNLOAD_TIMEOUT,
        help=f"HTTP timeout in seconds (default: {DEFAULT_DOWNLOAD_TIMEOUT}).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Maximum number of retries (default: {DEFAULT_MAX_RETRIES}).",
    )
    parser.add_argument(
        "--initial-backoff",
        type=float,
        default=DEFAULT_INITIAL_BACKOFF,
        help=f"Initial retry backoff in seconds (default: {DEFAULT_INITIAL_BACKOFF}).",
    )
    parser.add_argument(
        "--backoff-multiplier",
        type=float,
        default=DEFAULT_BACKOFF_MULTIPLIER,
        help=f"Retry backoff multiplier (default: {DEFAULT_BACKOFF_MULTIPLIER}).",
    )
    parser.add_argument(
        "--max-backoff",
        type=float,
        default=DEFAULT_MAX_BACKOFF,
        help=f"Maximum retry backoff in seconds (default: {DEFAULT_MAX_BACKOFF}).",
    )
    parser.add_argument(
        "--cache-dir",
        type=pathlib.Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Directory used for HTML cache (default: {DEFAULT_CACHE_DIR}).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable cache reads and writes.",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Ignore cached pages and re-download HTML.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> bool:
    if args.year <= 0:
        LOGGER.error("--year must be > 0")
        return False
    if args.limit < 0:
        LOGGER.error("--limit must be >= 0")
        return False
    if args.delay < 0:
        LOGGER.error("--delay must be >= 0")
        return False
    if args.timeout <= 0:
        LOGGER.error("--timeout must be > 0")
        return False
    if args.max_retries < 0:
        LOGGER.error("--max-retries must be >= 0")
        return False
    if args.initial_backoff < 0:
        LOGGER.error("--initial-backoff must be >= 0")
        return False
    if args.backoff_multiplier < 1:
        LOGGER.error("--backoff-multiplier must be >= 1")
        return False
    if args.max_backoff < 0:
        LOGGER.error("--max-backoff must be >= 0")
        return False
    return True


def main() -> int:
    configure_logging()
    LOGGER.info("Command line: %s", shlex.join(sys.argv))

    args = parse_args()
    if not validate_args(args):
        return 2

    contact_rows = read_contact_rows(args.input, args.limit)
    courses: list[TeachingCourse] = []
    initialize_courses_csv(args.output)

    for index, contact_row in enumerate(contact_rows, start=1):
        teacher_website = (contact_row.get("website") or "").strip()
        contact_name = (contact_row.get("name") or "").strip() or "<missing contact name>"

        if not teacher_website:
            LOGGER.info("[%s] %s: missing website, skipped", index, contact_name)
            continue

        teachings_url = build_teachings_url(teacher_website, args.year)
        if not teachings_url:
            LOGGER.warning("[%s] %s: could not extract teacher slug from %s", index, contact_name, teacher_website)
            continue

        time.sleep(args.delay)

        try:
            html = download_html_page(
                teachings_url,
                timeout=args.timeout,
                max_retries=args.max_retries,
                initial_backoff=args.initial_backoff,
                backoff_multiplier=args.backoff_multiplier,
                max_backoff=args.max_backoff,
                cache_dir=args.cache_dir,
                use_cache=not args.no_cache,
                refresh_cache=args.refresh_cache,
            )
        except Exception as error:
            LOGGER.warning("[%s] %s: error on %s -> %s", index, contact_name, teachings_url, error)
            continue

        parsed_courses = parse_teaching_courses(html, contact_row, teachings_url)
        if not parsed_courses:
            LOGGER.info("[%s] %s: no courses found in %s", index, contact_name, teachings_url)
            continue

        LOGGER.info("[%s] %s: found %s courses", index, contact_name, len(parsed_courses))
        courses.extend(parsed_courses)
        append_courses_csv(parsed_courses, args.output)

    LOGGER.info("Found %s courses in %s contact rows", len(courses), len(contact_rows))
    LOGGER.info("CSV saved to: %s", args.output)
    log_courses(courses)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

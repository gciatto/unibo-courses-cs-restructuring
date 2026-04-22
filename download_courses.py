import pathlib
import argparse
import csv
import datetime
import logging
import re
import shlex
import sys
from typing import Any, Iterable

from html_to_md import convert_html_to_markdown
import marko
import yaml
from pydantic import BaseModel, Field

from _utils import *


DEFAULT_INPUT = DATA_DIR / "course_headers.csv"
DEFAULT_OUTPUT = DATA_DIR / "courses"

EXPECTED_COLUMNS = [
    "contact_uid",
    "contact_name",
    "contact_email",
    "sito_web",
    "didattica_url",
    "course_title",
    "course_url",
    "integrated_course",
    "campus",
    "degree_course",
    "lesson_period",
    "schedule_url",
    "virtuale_url",
]

PATTERN_SKIP_BEFORE = re.compile(r"^#\s+.*")
PATTERN_SKIP_SINCE = re.compile(r"^(Seguici su:|Follow us on:)\s*$", re.IGNORECASE)
PATTERN_LANGUAGE_URL = re.compile(r"https?://.*?\.unibo\.it/.*?/(it|en)\?post_path=.*?/(\d+/\d+)$")
PATTERN_ACADEMIC_YEAR_SECTION = re.compile(r"^(?:Academic Year|Anno Accademico)\s+\d{4}/\d{4}$")
PATTERN_CREDITS = re.compile(r"(?:^|\n)-\s+(?:Credits|Crediti formativi):\s*(\d+)\b", re.IGNORECASE)
PATTERN_SSD = re.compile(r"(?:^|\n)-\s+SSD:\s*([^\n]+)", re.IGNORECASE)
PATTERN_LANGUAGE = re.compile(r"(?:^|\n)-\s+(?:Language|Lingua di insegnamento):\s*([^\n]+)", re.IGNORECASE)
PATTERN_TEACHING_MODE = re.compile(r"(?:^|\n)-\s+(?:Teaching Mode|Modalità didattica):\s*([^\n]+)", re.IGNORECASE)
PATTERN_TIMETABLE = re.compile(
    r"(?:dal\s+(\d{2}/\d{2}/\d{4})\s+al\s+(\d{2}/\d{2}/\d{4}))|"
    r"(?:from\s+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s+to\s+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}))",
    re.IGNORECASE,
)
PATTERN_DETAIL_AMONG_PARENTHESES = re.compile(r"\(([^)]+)\)")
PATTERN_DETAIL_CFU = re.compile(r"\s*[-–—]\s*(\s*\d+\s*CFU\s*)\s*", re.IGNORECASE)

LOGGER = logging.getLogger(pathlib.Path(__file__).stem)


class Teacher(BaseModel):
    teacher_id: str = Field(default="", serialization_alias="id")
    teacher_name: str = Field(default="", serialization_alias="name")
    teacher_email: str = Field(default="", serialization_alias="email")
    teacher_website: str = Field(default="", serialization_alias="website")


class CourseTitle(BaseModel):
    id: str = Field(default="")
    name: str = Field(default="")
    details: list[str] = Field(default_factory=list)


class CourseMetadata(BaseModel):
    year: int
    url: str
    credits: int | None = None
    ssd: str = Field(default="")
    language: str = Field(default="")
    teaching_mode: str = Field(default="")
    schedule: "CourseSchedule | None" = None
    teacher: Teacher
    course_title: CourseTitle
    integrated_course: str = Field(default="")
    campus: str = Field(default="")
    programme: str = Field(default="")
    syllabus: dict[str, "SyllabusPage"] = Field(default_factory=dict)


class SyllabusPage(BaseModel):
    url: str = Field(default="")
    title: str = Field(default="")
    contents: dict[str, str] = Field(default_factory=dict)


class CourseSchedule(BaseModel):
    schedule_from: datetime.date = Field(serialization_alias="from")
    schedule_to: datetime.date = Field(serialization_alias="to")


class CourseDetails(BaseModel):
    credits: int | None = None
    ssd: str = Field(default="")
    language: str = Field(default="")
    teaching_mode: str = Field(default="")
    schedule: CourseSchedule | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl course pages into YAML files.",
    )
    parser.add_argument(
        "--input",
        "-i",
        type=pathlib.Path,
        default=DEFAULT_INPUT,
        help=f"Input CSV file path (default: {DEFAULT_INPUT}).",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        default=None,
        help="Maximum number of web pages to download. Default: no limit.",
    )
    parser.add_argument(
        "--whitelist",
        "-w",
        nargs="*",
        default=[],
        help="Only include courses when any keyword matches title or page content (case-insensitive).",
    )
    parser.add_argument(
        "--blacklist",
        "-b",
        nargs="*",
        default=[],
        help="Exclude courses when any keyword matches title or page content (case-insensitive).",
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=float,
        default=DEFAULT_DOWNLOAD_TIMEOUT,
        help=f"HTTP timeout in seconds (default: {DEFAULT_DOWNLOAD_TIMEOUT}).",
    )
    parser.add_argument(
        "--max-retries",
        "-r",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Maximum number of download retries after the first attempt (default: {DEFAULT_MAX_RETRIES}).",
    )
    parser.add_argument(
        "--initial-backoff",
        "-ib",
        type=float,
        default=DEFAULT_INITIAL_BACKOFF,
        help=f"Initial exponential backoff delay in seconds (default: {DEFAULT_INITIAL_BACKOFF}).",
    )
    parser.add_argument(
        "--backoff-multiplier",
        "-bm",
        type=float,
        default=DEFAULT_BACKOFF_MULTIPLIER,
        help=f"Multiplier applied to backoff between retries (default: {DEFAULT_BACKOFF_MULTIPLIER}).",
    )
    parser.add_argument(
        "--max-backoff",
        "-mb",
        type=float,
        default=DEFAULT_MAX_BACKOFF,
        help=f"Maximum backoff delay in seconds (default: {DEFAULT_MAX_BACKOFF}).",
    )
    return parser.parse_args()


def normalize_keywords(keywords: Iterable[str]) -> list[str]:
    return [k.strip().lower() for k in keywords if k and k.strip()]


def matching_keywords(text: str, keywords: list[str]) -> list[str]:
    lowered = text.lower()
    return [keyword for keyword in keywords if keyword in lowered]


def contains_any(text: str, keywords: list[str]) -> bool:
    return bool(matching_keywords(text, keywords))


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt=r"%Y-%m-%d %H:%M:%S")


def format_course_context(row_index: int, row: dict[str, str]) -> str:
    course_title = (row.get("course_title") or "").strip() or "<missing title>"
    course_url = (row.get("course_url") or "").strip() or "<missing url>"
    return f"row={row_index} title={course_title!r} url={course_url}"


def extract_details_from_parentheses(text: str) -> tuple[str, list[str]]:
    details: list[str] = []
    cleaned_chars: list[str] = []
    current_detail: list[str] = []
    depth = 0

    for char in text:
        if char == "(":
            if depth == 0:
                current_detail = []
            else:
                current_detail.append(char)
            depth += 1
            continue

        if char == ")":
            if depth == 0:
                cleaned_chars.append(char)
                continue

            depth -= 1
            if depth == 0:
                content = "".join(current_detail).strip()
                if content and not content.startswith("-"):
                    details.append(content)
                current_detail = []
            else:
                current_detail.append(char)
            continue

        if depth == 0:
            cleaned_chars.append(char)
        else:
            current_detail.append(char)

    if depth > 0:
        cleaned_chars.append("(" + "".join(current_detail))

    return "".join(cleaned_chars).strip(), details


@auto_logged(LOGGER)
def split_course_title(raw_title: str) -> CourseTitle:
    raw_title = raw_title.strip()
    if "-" not in raw_title:
        return CourseTitle(name=raw_title)
    id_part, name_part = [part.strip() for part in raw_title.split("-", 1)]
    if "-" not in name_part and "(" not in name_part:
        return CourseTitle(id=id_part, name=name_part)
    name_part, some_details = extract_details_from_parentheses(name_part)
    m = PATTERN_DETAIL_CFU.search(name_part)
    if m:
        some_details.append(m.group(1))
        name_part = PATTERN_DETAIL_CFU.sub("", name_part, count=1).strip()
    return CourseTitle(id=id_part, name=name_part, details=some_details)


def parse_year_and_course_id(url: str) -> tuple[int, str]:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.rstrip("/")

    match = re.search(r"/(\d{4})/([^/]+)$", path)
    if not match:
        raise ValueError(f"Could not extract year and course id from URL: {url}")

    year_raw, course_id = match.groups()
    return int(year_raw), course_id


def build_metadata(
    row: dict[str, str],
    year: int,
    url: str,
    details: CourseDetails,
    syllabus: dict[str, SyllabusPage],
) -> CourseMetadata:
    return CourseMetadata(
        year=year,
        url=url,
        credits=details.credits,
        ssd=details.ssd,
        language=details.language,
        teaching_mode=details.teaching_mode,
        schedule=details.schedule,
        teacher=Teacher(
            teacher_id=(row.get("contact_uid") or "").strip(),
            teacher_name=(row.get("contact_name") or "").strip(),
            teacher_email=(row.get("contact_email") or "").strip(),
            teacher_website=(row.get("sito_web") or "").strip(),
        ),
        course_title=split_course_title((row.get("course_title") or "").strip()),
        integrated_course=(row.get("integrated_course") or "").strip(),
        campus=(row.get("campus") or "").strip(),
        programme=(row.get("degree_course") or "").strip(),
        syllabus=syllabus,
    )


def extract_markdown_urls(markdown: str) -> list[str]:
    return [match.group(0).rstrip(".,)") for match in re.finditer(r"https?://[^\s)>]+", markdown)]


def extract_language_urls(markdown: str) -> dict[str, str]:
    language_urls: dict[str, str] = {}

    for url in extract_markdown_urls(markdown):
        match = PATTERN_LANGUAGE_URL.fullmatch(url)
        if match is None:
            continue
        language = match.group(1)
        language_urls.setdefault(language, url)

    if "it" not in language_urls or "en" not in language_urls:
        raise ValueError(
            "Could not extract both italian and english language URLs from the page markdown.",
        )

    return language_urls


def parse_front_matter_dumbly(text: str) -> dict[str, str]:
    result = {}
    for line in text.splitlines():
        if ": " not in line:
            LOGGER.warning("Skipping unrecognized header line: %r", line)
            continue
        key, value = line.split(":", 1)
        result[key.strip()] = value.strip()
    return result


def split_front_matter(markdown: str) -> tuple[dict[str, str], str]:
    lines = markdown.splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError("Markdown front matter not found.")

    closing_index = None
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            closing_index = index
            break

    if closing_index is None:
        raise ValueError("Markdown front matter is not terminated.")

    front_matter_raw = "\n".join(lines[1:closing_index])
    try:
        front_matter = yaml.safe_load(front_matter_raw) or {}
    except yaml.YAMLError:
        front_matter = parse_front_matter_dumbly(front_matter_raw)
    if not isinstance(front_matter, dict):
        raise ValueError("Markdown front matter is not a YAML mapping.")

    metadata = {
        key: str(front_matter.get(key) or "").strip()
        for key in ("base", "canonical", "title")
    }
    body = "\n".join(lines[closing_index + 1 :]).lstrip("\n")
    return metadata, body


def clean_markdown(markdown: str) -> str:
    lines = markdown.splitlines()
    start_index = None

    for index, line in enumerate(lines):
        if PATTERN_SKIP_BEFORE.match(line):
            start_index = index
            break

    if start_index is None:
        raise ValueError("Could not find the first syllabus heading in markdown content.")

    cleaned_lines: list[str] = []
    empty_line_count = 0

    for line in lines[start_index:]:
        if PATTERN_SKIP_SINCE.match(line):
            break

        if line.strip() == "":
            empty_line_count += 1
        else:
            empty_line_count = 0

        if empty_line_count <= 1:
            cleaned_lines.append(line.rstrip())

    cleaned_markdown = "\n".join(cleaned_lines).strip()
    if not cleaned_markdown:
        raise ValueError("Markdown content is empty after cleanup.")

    return cleaned_markdown


def stringify_inline(node: Any) -> str:
    if isinstance(node, str):
        return node

    children = getattr(node, "children", None)
    node_type = type(node).__name__

    if node_type == "RawText":
        if isinstance(children, str):
            return children
        if isinstance(children, tuple):
            return "".join(part if isinstance(part, str) else stringify_inline(part) for part in children)
        return ""

    if node_type == "LineBreak":
        return "\n"

    if node_type == "CodeSpan":
        return stringify_children(children)

    if node_type == "Link":
        label = stringify_children(children).strip()
        destination = str(getattr(node, "dest", "") or "").strip()
        if label and destination:
            return f"{label} ({destination})"
        return label or destination

    if node_type == "Image":
        return stringify_children(children).strip()

    return stringify_children(children)


def stringify_children(children: Any) -> str:
    if children is None:
        return ""
    if isinstance(children, str):
        return children
    if isinstance(children, tuple):
        return "".join(part if isinstance(part, str) else stringify_inline(part) for part in children)
    if isinstance(children, list):
        return "".join(stringify_inline(child) for child in children)
    return stringify_inline(children)


def normalize_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
    compact_lines: list[str] = []
    previous_empty = False

    for line in lines:
        if line == "":
            if previous_empty:
                continue
            previous_empty = True
        else:
            previous_empty = False
        compact_lines.append(line)

    return "\n".join(compact_lines).strip()


def find_academic_year_section(sections: dict[str, str]) -> tuple[str, str] | None:
    for title, content in sections.items():
        if PATTERN_ACADEMIC_YEAR_SECTION.fullmatch(title.strip()):
            return title, content
    return None


def normalize_detail_value(value: str) -> str:
    value = normalize_text(value)
    value = re.sub(r"\s*\((?:Modulo|Module)\s+[^)]*\)\s*;?\s*$", "", value, flags=re.IGNORECASE)
    return value.strip(" ;")


def parse_date_value(value: str) -> datetime.date:
    normalized = normalize_text(value)

    for date_format in ("%d/%m/%Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.datetime.strptime(normalized, date_format).date()
        except ValueError:
            continue

    raise ValueError(f"Unsupported date format: {value}")


def parse_timetile(section_content: str) -> CourseSchedule | None:
    ranges: list[tuple[datetime.date, datetime.date]] = []

    for match in PATTERN_TIMETABLE.finditer(section_content):
        if match.group(1) and match.group(2):
            start_date = parse_date_value(match.group(1))
            end_date = parse_date_value(match.group(2))
        elif match.group(3) and match.group(4):
            start_date = parse_date_value(match.group(3))
            end_date = parse_date_value(match.group(4))
        else:
            continue

        ranges.append((start_date, end_date))

    if not ranges:
        return None

    start_date = min(start for start, _ in ranges)
    end_date = max(end for _, end in ranges)
    return CourseSchedule(schedule_from=start_date, schedule_to=end_date)


def extract_unique_values(pattern: re.Pattern[str], text: str) -> list[str]:
    values: list[str] = []
    for match in pattern.finditer(text):
        value = normalize_detail_value(match.group(1))
        if value and value not in values:
            values.append(value)
    return values


def extract_course_details(section_content: str) -> CourseDetails:
    credits_match = PATTERN_CREDITS.search(section_content)
    ssd_values = extract_unique_values(PATTERN_SSD, section_content)
    language_values = extract_unique_values(PATTERN_LANGUAGE, section_content)
    teaching_mode_values = extract_unique_values(PATTERN_TEACHING_MODE, section_content)

    return CourseDetails(
        credits=int(credits_match.group(1)) if credits_match else None,
        ssd="; ".join(ssd_values),
        language="; ".join(language_values),
        teaching_mode="; ".join(teaching_mode_values),
        schedule=parse_timetile(section_content),
    )


def merge_course_details(*details_list: CourseDetails) -> CourseDetails:
    merged = CourseDetails()
    for details in details_list:
        if merged.credits is None and details.credits is not None:
            merged.credits = details.credits
        if not merged.ssd and details.ssd:
            merged.ssd = details.ssd
        if not merged.language and details.language:
            merged.language = details.language
        if not merged.teaching_mode and details.teaching_mode:
            merged.teaching_mode = details.teaching_mode
        if merged.schedule is None and details.schedule is not None:
            merged.schedule = details.schedule
    return merged


def stringify_block(node: Any) -> str:
    node_type = type(node).__name__

    if node_type in {"Paragraph", "Heading", "SetextHeading"}:
        return normalize_text(stringify_children(getattr(node, "children", None)))

    if node_type == "BlankLine":
        return ""

    if node_type == "List":
        items: list[str] = []
        ordered = bool(getattr(node, "ordered", False))
        for index, item in enumerate(getattr(node, "children", []), start=1):
            item_text = stringify_block(item)
            if not item_text:
                continue
            prefix = f"{index}. " if ordered else "- "
            item_lines = item_text.splitlines() or [""]
            formatted = [prefix + item_lines[0]]
            formatted.extend(f"  {line}" for line in item_lines[1:])
            items.append("\n".join(formatted))
        return "\n".join(items)

    if node_type == "ListItem":
        parts = [stringify_block(child) for child in getattr(node, "children", [])]
        return join_text_parts(parts)

    if node_type == "Quote":
        text = join_text_parts(stringify_block(child) for child in getattr(node, "children", []))
        if not text:
            return ""
        return "\n".join(f"> {line}" if line else ">" for line in text.splitlines())

    if node_type in {"FencedCode", "CodeBlock"}:
        language = str(getattr(node, "lang", "") or "").strip()
        code = normalize_text(getattr(node, "children", "") or "")
        if language:
            return f"```{language}\n{code}\n```"
        return f"```\n{code}\n```"

    if node_type == "HTMLBlock":
        return normalize_text(getattr(node, "body", "") or "")

    if hasattr(node, "children"):
        return normalize_text(stringify_children(getattr(node, "children", None)))

    return ""


def join_text_parts(parts: Iterable[str]) -> str:
    joined_parts: list[str] = []
    for part in parts:
        normalized = part.strip()
        if not normalized:
            continue
        joined_parts.append(normalized)
    return "\n\n".join(joined_parts)


def extract_sections(markdown: str) -> dict[str, str]:
    document = marko.parse(markdown)
    sections: dict[str, str] = {}
    current_title = ""
    current_blocks: list[Any] = []

    for child in getattr(document, "children", []):
        node_type = type(child).__name__
        if node_type in {"Heading", "SetextHeading"}:
            level = int(getattr(child, "level", 0) or 0)
            title = normalize_text(stringify_children(getattr(child, "children", None)))

            if level <= 1:
                continue

            if current_title:
                rendered = join_text_parts(stringify_block(block) for block in current_blocks)
                if rendered:
                    if current_title in sections:
                        sections[current_title] = f"{sections[current_title]}\n\n{rendered}".strip()
                    else:
                        sections[current_title] = rendered

            current_title = title
            current_blocks = []
            continue

        if current_title:
            current_blocks.append(child)

    if current_title:
        rendered = join_text_parts(stringify_block(block) for block in current_blocks)
        if rendered:
            if current_title in sections:
                sections[current_title] = f"{sections[current_title]}\n\n{rendered}".strip()
            else:
                sections[current_title] = rendered

    if not sections:
        raise ValueError("Could not extract any syllabus sections from markdown.")

    return sections


def split_syllabus_sections(sections: dict[str, str]) -> tuple[dict[str, str], CourseDetails]:
    section_match = find_academic_year_section(sections)
    if section_match is None:
        return sections, CourseDetails()

    title, content = section_match
    filtered_sections = dict(sections)
    filtered_sections.pop(title, None)
    return filtered_sections, extract_course_details(content)


def parse_syllabus_page(
    url: str,
    timeout: float,
    max_retries: int,
    initial_backoff: float,
    backoff_multiplier: float,
    max_backoff: float,
) -> tuple[SyllabusPage, str, CourseDetails]:
    html_content = download_html_page(
        url,
        timeout=timeout,
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        backoff_multiplier=backoff_multiplier,
        max_backoff=max_backoff,
    )
    markdown = convert_html_to_markdown(html_content)
    front_matter, markdown_body = split_front_matter(markdown)
    cleaned_markdown = clean_markdown(markdown_body)
    sections = extract_sections(cleaned_markdown)
    filtered_sections, details = split_syllabus_sections(sections)

    syllabus_page = SyllabusPage(
        url=front_matter["canonical"] or front_matter["base"] or url,
        title=front_matter["title"],
        contents=filtered_sections,
    )
    return syllabus_page, cleaned_markdown, details


def discover_language_urls(
    course_url: str,
    timeout: float,
    max_retries: int,
    initial_backoff: float,
    backoff_multiplier: float,
    max_backoff: float,
) -> dict[str, str]:
    html_content = download_html_page(
        course_url,
        timeout=timeout,
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        backoff_multiplier=backoff_multiplier,
        max_backoff=max_backoff,
    )
    markdown = convert_html_to_markdown(html_content)
    return extract_language_urls(markdown)


def process_row(
    row_index: int,
    row: dict[str, str],
    output_dir: pathlib.Path,
    whitelist: list[str],
    blacklist: list[str],
    timeout: float,
    max_retries: int,
    initial_backoff: float,
    backoff_multiplier: float,
    max_backoff: float,
) -> tuple[str, str]:
    course_context = format_course_context(row_index, row)
    course_url = (row.get("course_url") or "").strip()
    if not course_url:
        return "skipped", f"empty course_url ({course_context})"

    teacher_email = (row.get("contact_email") or "").strip()
    if not teacher_email:
        return "skipped", f"empty contact_email ({course_context})"

    try:
        year, course_id = parse_year_and_course_id(course_url)
    except ValueError as error:
        return "skipped", f"{error} ({course_context})"

    language_urls = discover_language_urls(
        course_url,
        timeout=timeout,
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        backoff_multiplier=backoff_multiplier,
        max_backoff=max_backoff,
    )

    syllabus: dict[str, SyllabusPage] = {}
    details_by_language: dict[str, CourseDetails] = {}
    searchable_fragments: list[str] = [(row.get("course_title") or "").strip()]
    page_errors: list[str] = []

    for language in ("it", "en"):
        language_url = language_urls.get(language)
        if not language_url:
            page_errors.append(f"missing {language} language URL")
            continue

        try:
            syllabus_page, cleaned_markdown, page_details = parse_syllabus_page(
                language_url,
                timeout=timeout,
                max_retries=max_retries,
                initial_backoff=initial_backoff,
                backoff_multiplier=backoff_multiplier,
                max_backoff=max_backoff,
            )
        except Exception as error:
            page_errors.append(f"{language_url} ({error})")
            continue

        syllabus[language] = syllabus_page
        details_by_language[language] = page_details
        searchable_fragments.append(cleaned_markdown)

    searchable_text = "\n".join(searchable_fragments)
    whitelist_matches = matching_keywords(searchable_text, whitelist)
    blacklist_matches = matching_keywords(searchable_text, blacklist)

    if whitelist and not whitelist_matches:
        missing_terms = ", ".join(whitelist)
        return (
            "skipped",
            f"whitelist did not match; missing keywords: {missing_terms} ({course_context})",
        )

    if blacklist_matches:
        present_terms = ", ".join(blacklist_matches)
        return (
            "skipped",
            f"blacklist matched; present keywords: {present_terms} ({course_context})",
        )

    if not syllabus:
        details = "; ".join(page_errors) if page_errors else "no language pages available"
        raise ValueError(f"No syllabus pages were parsed successfully: {details}")

    teacher_id = teacher_email.split("@")[0]
    course_dir = output_dir / teacher_id / str(year)
    course_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = course_dir / f"course-{course_id}.yml"
    metadata = build_metadata(
        row=row,
        year=year,
        url=course_url,
        details=merge_course_details(
            details_by_language.get("en", CourseDetails()),
            details_by_language.get("it", CourseDetails()),
        ),
        syllabus=syllabus,
    )
    metadata_path.write_text(
        yaml.safe_dump(
            metadata.model_dump(by_alias=True, exclude_none=True),
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    if page_errors:
        return (
            "ok",
            f"wrote {metadata_path} with warnings: {'; '.join(page_errors)} ({course_context})",
        )
    return "ok", f"wrote {metadata_path} ({course_context})"


def ensure_expected_columns(reader: csv.DictReader) -> None:
    actual = reader.fieldnames or []
    missing = [name for name in EXPECTED_COLUMNS if name not in actual]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"Input CSV is missing expected columns: {missing_text}")


def main() -> int:
    configure_logging()
    LOGGER.info("Command line: %s", shlex.join(sys.argv))
    args = parse_args()
    whitelist = normalize_keywords(args.whitelist)
    blacklist = normalize_keywords(args.blacklist)

    if args.limit is not None and args.limit < 0:
        LOGGER.error("--limit must be >= 0")
        return 2

    if args.max_retries < 0:
        LOGGER.error("--max-retries must be >= 0")
        return 2

    if args.initial_backoff < 0:
        LOGGER.error("--initial-backoff must be >= 0")
        return 2

    if args.backoff_multiplier < 1:
        LOGGER.error("--backoff-multiplier must be >= 1")
        return 2

    if args.max_backoff < 0:
        LOGGER.error("--max-backoff must be >= 0")
        return 2

    if not args.input.exists():
        LOGGER.error("input file does not exist: %s", args.input)
        return 2

    args.output_dir.mkdir(parents=True, exist_ok=True)

    downloaded_count = 0
    skipped_count = 0
    failed_count = 0

    with args.input.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        ensure_expected_columns(reader)

        for row_index, row in enumerate(reader, start=2):
            if args.limit is not None and downloaded_count >= args.limit:
                break

            try:
                status, message = process_row(
                    row_index=row_index,
                    row=row,
                    output_dir=args.output_dir,
                    whitelist=whitelist,
                    blacklist=blacklist,
                    timeout=args.timeout,
                    max_retries=args.max_retries,
                    initial_backoff=args.initial_backoff,
                    backoff_multiplier=args.backoff_multiplier,
                    max_backoff=args.max_backoff,
                )
            except (urllib.error.URLError, TimeoutError) as error:
                failed_count += 1
                LOGGER.error("%s (%s)", format_course_context(row_index, row), error)
                continue
            except Exception:
                failed_count += 1
                LOGGER.exception(
                    "unexpected error while processing %s",
                    format_course_context(row_index, row),
                )
                continue

            if status == "ok":
                downloaded_count += 1
                LOGGER.info(message)
            elif status == "skipped":
                skipped_count += 1
                LOGGER.warning(message)
            else:
                failed_count += 1
                LOGGER.error(message)

    LOGGER.info(
        "Done. downloaded=%s skipped=%s failed=%s",
        downloaded_count,
        skipped_count,
        failed_count,
    )
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

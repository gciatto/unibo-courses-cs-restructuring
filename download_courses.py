import pathlib
import argparse
import csv
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable

import yaml
from pydantic import BaseModel
from pydantic import Field


DATA_DIR = pathlib.Path("data")
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


class Teacher(BaseModel):
    teacher_id: str = Field(default="")
    teacher_name: str = Field(default="")
    teacher_email: str = Field(default="")


class CourseTitle(BaseModel):
    id: str = Field(default="")
    name: str = Field(default="")
    details: str = Field(default="")


class CourseMetadata(BaseModel):
    year: int
    url: str
    teacher: Teacher
    course_title: CourseTitle
    integrated_course: str = Field(default="")
    campus: str = Field(default="")
    programme: str = Field(default="")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download course pages and write HTML + YAML metadata files.",
    )
    parser.add_argument(
        "--input",
        type=pathlib.Path,
        default=DEFAULT_INPUT,
        help=f"Input CSV file path (default: {DEFAULT_INPUT}).",
    )
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of web pages to download. Default: no limit.",
    )
    parser.add_argument(
        "--whitelist",
        nargs="*",
        default=[],
        help="Only include courses when any keyword matches title or page content (case-insensitive).",
    )
    parser.add_argument(
        "--blacklist",
        nargs="*",
        default=[],
        help="Exclude courses when any keyword matches title or page content (case-insensitive).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds (default: 30).",
    )
    return parser.parse_args()


def normalize_keywords(keywords: Iterable[str]) -> list[str]:
    return [k.strip().lower() for k in keywords if k and k.strip()]


def contains_any(text: str, keywords: list[str]) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def split_course_title(raw_title: str) -> CourseTitle:
    parts = [part.strip() for part in raw_title.split("-")]
    parts = [part for part in parts if part]

    if not parts:
        return CourseTitle()
    if len(parts) == 1:
        return CourseTitle(name=parts[0])
    if len(parts) == 2:
        return CourseTitle(id=parts[0], name=parts[1])
    return CourseTitle(id=parts[0], name=parts[1], details=" - ".join(parts[2:]))


def parse_year_and_course_id(url: str) -> tuple[int, str]:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.rstrip("/")

    match = re.search(r"/(\d{4})/([^/]+)$", path)
    if not match:
        raise ValueError(f"Could not extract year and course id from URL: {url}")

    year_raw, course_id = match.groups()
    return int(year_raw), course_id


def download_html(url: str, timeout: float) -> str:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; unibo-courses-downloader/1.0)"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def build_metadata(row: dict[str, str], year: int, url: str) -> CourseMetadata:
    return CourseMetadata(
        year=year,
        url=url,
        teacher=Teacher(
            teacher_id=(row.get("contact_uid") or "").strip(),
            teacher_name=(row.get("contact_name") or "").strip(),
            teacher_email=(row.get("contact_email") or "").strip(),
        ),
        course_title=split_course_title((row.get("course_title") or "").strip()),
        integrated_course=(row.get("integrated_course") or "").strip(),
        campus=(row.get("campus") or "").strip(),
        programme=(row.get("degree_course") or "").strip(),
    )


def ensure_expected_columns(reader: csv.DictReader) -> None:
    actual = reader.fieldnames or []
    missing = [name for name in EXPECTED_COLUMNS if name not in actual]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"Input CSV is missing expected columns: {missing_text}")


def main() -> int:
    args = parse_args()
    whitelist = normalize_keywords(args.whitelist)
    blacklist = normalize_keywords(args.blacklist)

    if args.limit is not None and args.limit < 0:
        print("ERROR: --limit must be >= 0", file=sys.stderr)
        return 2

    if not args.input.exists():
        print(f"ERROR: input file does not exist: {args.input}", file=sys.stderr)
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

            course_url = (row.get("course_url") or "").strip()
            if not course_url:
                skipped_count += 1
                print(f"SKIP row {row_index}: empty course_url", file=sys.stderr)
                continue

            teacher_email = (row.get("contact_email") or "").strip()
            if not teacher_email:
                skipped_count += 1
                print(f"SKIP row {row_index}: empty contact_email", file=sys.stderr)
                continue

            try:
                year, course_id = parse_year_and_course_id(course_url)
            except ValueError as error:
                skipped_count += 1
                print(f"SKIP row {row_index}: {error}", file=sys.stderr)
                continue

            try:
                html_content = download_html(course_url, timeout=args.timeout)
            except (urllib.error.URLError, TimeoutError, ValueError) as error:
                failed_count += 1
                print(f"FAIL row {row_index}: {course_url} ({error})", file=sys.stderr)
                continue

            course_title = (row.get("course_title") or "").strip()
            searchable_text = f"{course_title}\n{html_content}"

            if whitelist and not contains_any(searchable_text, whitelist):
                skipped_count += 1
                print(f"SKIP row {row_index}: whitelist did not match", file=sys.stderr)
                continue

            if blacklist and contains_any(searchable_text, blacklist):
                skipped_count += 1
                print(f"SKIP row {row_index}: blacklist matched", file=sys.stderr)
                continue

            course_dir = args.output_dir / teacher_email / str(year)
            course_dir.mkdir(parents=True, exist_ok=True)

            html_path = course_dir / f"course-{course_id}-page.html"
            metadata_path = course_dir / f"course-{course_id}-metadata.yml"

            metadata = build_metadata(row=row, year=year, url=course_url)

            html_path.write_text(html_content, encoding="utf-8")
            metadata_path.write_text(
                yaml.safe_dump(
                    metadata.model_dump(exclude_none=True),
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )

            downloaded_count += 1
            print(f"OK row {row_index}: wrote {html_path} and {metadata_path}")

    print(
        f"Done. downloaded={downloaded_count} skipped={skipped_count} failed={failed_count}",
        file=sys.stderr,
    )
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

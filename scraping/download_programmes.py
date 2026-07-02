import argparse
import datetime
import logging
import pathlib
import re
import shlex
import sys
from typing import Annotated, Any
from urllib.parse import parse_qs, parse_qsl, urlencode, urlsplit, urlunsplit

import yaml
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, StringConstraints, field_validator

from scraping._utils import (
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_CACHE_DIR,
    DEFAULT_DOWNLOAD_TIMEOUT,
    DEFAULT_INITIAL_BACKOFF,
    DEFAULT_MAX_BACKOFF,
    DEFAULT_MAX_RETRIES,
    configure_logging,
    download_html_page,
)
from data import DIR_DATA
from resources import classify_dept


LOGGER = logging.getLogger(pathlib.Path(__file__).stem)

CATALOG_BASE_URLS = {
    "first-single-cycle": {
        "it": "https://www.unibo.it/it/studiare/lauree-e-lauree-magistrali-a-ciclo-unico",
        "en": "https://www.unibo.it/en/study/first-and-single-cycle-degree",
        "default_duration": None,
    },
    "second-cycle": {
        "it": "https://www.unibo.it/it/studiare/lauree-magistrali",
        "en": "https://www.unibo.it/en/study/second-cycle-degree",
        # Second-cycle cards do not display a duration field; Italian law fixes it at 2 years.
        "default_duration": 2,
    },
}

CATALOG_ALL = "all"
DEFAULT_CATALOG = CATALOG_ALL

DEFAULT_OUTPUT_DIR = DIR_DATA / "programmes"

LABELS_CAMPUS = {"sede didattica", "place of teaching"}
LABELS_LANGUAGE = {"lingua", "language"}
LABELS_DURATION = {"durata", "duration"}
LABELS_ACCESS = {"tipo di accesso", "type of access"}

LANGUAGE_TRANSLATIONS = {
    "italiano": "italian",
    "italian": "italian",
    "inglese": "english",
    "english": "english",
    "francese": "french",
    "french": "french",
    "tedesco": "german",
    "german": "german",
    "spagnolo": "spanish",
    "spanish": "spanish",
    "portoghese": "portuguese",
    "portuguese": "portuguese",
    "russo": "russian",
    "russian": "russian",
    "cinese": "chinese",
    "chinese": "chinese",
    "arabo": "arabic",
    "arabic": "arabic",
}

LanguageName = Annotated[str, StringConstraints(pattern=r"^[a-z]+(?:[ -][a-z]+)*$")]


class ProgrammeName(BaseModel):
    it: str = Field(default="")
    en: str = Field(default="")


class ProgrammeUrls(BaseModel):
    it: str = Field(default="")
    en: str = Field(default="")


class ProgrammeYaml(BaseModel):
    code: str
    campus: str
    languages: list[LanguageName]
    duration: int
    access: str
    name: ProgrammeName
    year: int
    department: str
    urls: ProgrammeUrls

    @field_validator("languages")
    @classmethod
    def validate_languages(cls, value: list[str]) -> list[str]:
        normalized = [normalize_spaces(item).lower() for item in value if normalize_spaces(item)]
        if not normalized:
            raise ValueError("languages must contain at least one lower-case language in English")
        for item in normalized:
            if item != item.lower():
                raise ValueError(f"Invalid language {item!r}: must be lower-case")
        # Preserve order while deduplicating.
        return list(dict.fromkeys(normalized))


class CrawledProgrammeCard(BaseModel):
    code: str
    name: str
    campus: str
    languages: list[LanguageName]
    duration: int
    access: str
    url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Crawl UniBo degree programme catalogues (first/single-cycle or second-cycle) "
            "into YAML files."
        ),
    )
    parser.add_argument(
        "--catalog",
        choices=[CATALOG_ALL, *sorted(CATALOG_BASE_URLS.keys())],
        default=DEFAULT_CATALOG,
        help=(
            "Programme catalogue to crawl "
            f"(default: {DEFAULT_CATALOG})."
        ),
    )
    parser.add_argument(
        "--italian-base-url",
        default=None,
        help=(
            "Optional override for the Italian listing base URL; if omitted, the value "
            "for the selected --catalog is used."
        ),
    )
    parser.add_argument(
        "--english-base-url",
        default=None,
        help=(
            "Optional override for the English listing base URL; if omitted, the value "
            "for the selected --catalog is used."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        default=datetime.date.today().year - 1,
        help="Academic year (default: current year - 1).",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output base directory (default: {DEFAULT_OUTPUT_DIR}).",
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


def normalize_spaces(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split()).strip()


def normalize_label(value: str) -> str:
    return normalize_spaces(value).strip(":").lower()


def value_for_label(card, labels: set[str]) -> str:
    for paragraph in card.select(".text-wrapper p"):
        label_node = paragraph.select_one("span")
        if label_node is None:
            continue
        label = normalize_label(label_node.get_text(" ", strip=True))
        if label not in labels:
            continue
        label_text = normalize_spaces(label_node.get_text(" ", strip=True))
        whole_text = normalize_spaces(paragraph.get_text(" ", strip=True))
        if whole_text.lower().startswith(label_text.lower()):
            return normalize_spaces(whole_text[len(label_text):].lstrip(":"))
    return ""


def parse_duration_years(raw_duration: str, code: str, lang: str) -> int:
    match = re.search(r"(\d+)", raw_duration)
    if match is None:
        raise ValueError(f"Could not parse duration for programme {code} ({lang}): {raw_duration!r}")
    return int(match.group(1))


def normalize_programme_languages(raw_language: str, code: str, lang: str) -> list[str]:
    raw_tokens = [normalize_spaces(part) for part in re.split(r"\s*,\s*|\s*;\s*", raw_language) if normalize_spaces(part)]
    languages: list[str] = []

    for token in raw_tokens:
        token_key = token.lower()
        if token_key in LANGUAGE_TRANSLATIONS:
            languages.append(LANGUAGE_TRANSLATIONS[token_key])
            continue

        fallback = re.sub(r"[^a-z -]", "", token_key).strip()
        if fallback:
            LOGGER.warning(
                "Programme %s (%s) has unmapped language token %r; using fallback %r",
                code,
                lang,
                token,
                fallback,
            )
            languages.append(fallback)

    if not languages:
        LOGGER.warning(
            "Programme %s (%s) has unknown language label %r; defaulting to ['italian']",
            code,
            lang,
            raw_language,
        )
        languages = ["italian"]

    # Keep deterministic order and remove duplicates.
    return sorted(set(languages))


def parse_dept_slug(data_params: str) -> str | None:
    query_map = parse_qs(data_params, keep_blank_values=True)
    values = query_map.get("schede")
    if not values:
        return None
    return values[0].strip() or None


def parse_department_buttons(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    departments: dict[str, str] = {}

    for button in soup.select("button[data-params*='schede=']"):
        data_params = button.get("data-params", "")
        slug = parse_dept_slug(data_params)
        if not slug:
            continue
        title_node = button.select_one(".title")
        if title_node is None:
            continue
        dept_name = normalize_spaces(title_node.get_text(" ", strip=True))
        if dept_name:
            departments[slug] = dept_name

    if not departments:
        raise RuntimeError("Could not find department buttons in listing page")
    return departments


def parse_programme_cards(
    html: str,
    lang: str,
    *,
    default_duration: int | None = None,
) -> dict[str, CrawledProgrammeCard]:
    soup = BeautifulSoup(html, "html.parser")
    cards: dict[str, CrawledProgrammeCard] = {}

    for item in soup.select(".card-list-abstract > .item"):
        button = item.select_one("button.add-favourites")
        code = normalize_spaces(button.get("data-codice", "") if button else "")
        if not code:
            heading = item.select_one(".title h3[id]")
            code = normalize_spaces(heading.get("id", "") if heading else "")
        if not code:
            LOGGER.warning("Skipping card with missing programme code (%s)", lang)
            continue

        name = normalize_spaces(item.select_one(".title h3").get_text(" ", strip=True) if item.select_one(".title h3") else "")
        campus = value_for_label(item, LABELS_CAMPUS)
        raw_language = value_for_label(item, LABELS_LANGUAGE)
        raw_duration = value_for_label(item, LABELS_DURATION)
        access = value_for_label(item, LABELS_ACCESS)

        anchor = item.select_one(".card-actions a[href]")
        url = normalize_spaces(anchor.get("href", "") if anchor else "")

        if raw_duration:
            try:
                duration = parse_duration_years(raw_duration, code, lang)
            except ValueError as error:
                LOGGER.warning("%s", error)
                continue
        elif default_duration is not None:
            duration = default_duration
        else:
            LOGGER.warning(
                "Could not parse duration for programme %s (%s): %r",
                code,
                lang,
                raw_duration,
            )
            continue

        cards[code] = CrawledProgrammeCard(
            code=code,
            name=name,
            campus=campus,
            languages=normalize_programme_languages(raw_language, code, lang),
            duration=duration,
            access=access,
            url=url,
        )

    return cards


def fetch_html(
    url: str,
    *,
    timeout: float,
    max_retries: int,
    initial_backoff: float,
    backoff_multiplier: float,
    max_backoff: float | None,
    cache_dir: pathlib.Path,
    use_cache: bool,
    refresh_cache: bool,
) -> str:
    return download_html_page(
        url,
        timeout=timeout,
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        backoff_multiplier=backoff_multiplier,
        max_backoff=max_backoff,
        cache_dir=cache_dir,
        use_cache=use_cache,
        refresh_cache=refresh_cache,
    )


def with_query_params(url: str, params: dict[str, Any]) -> str:
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({key: str(value) for key, value in params.items()})
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query),
            parsed.fragment,
        )
    )


def build_listing_url(base_url: str, year: int) -> str:
    return with_query_params(
        base_url,
        {
            "orderby": "department",
            "annoAccademico": year,
            "corsiper": "dipartimento",
        },
    )


def build_department_cards_url(base_url: str, year: int, dept_slug: str) -> str:
    parsed = urlsplit(base_url)
    cards_path = f"{parsed.path.rstrip('/')}/elenco"
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            cards_path,
            urlencode(
                {
                    "orderby": "department",
                    "annoAccademico": year,
                    "corsiper": "dipartimento",
                    "schede": dept_slug,
                }
            ),
            parsed.fragment,
        )
    )


def warn_if_mismatch(code: str, field_name: str, it_value: Any, en_value: Any) -> None:
    if it_value != en_value:
        LOGGER.warning(
            "Mismatch for %s field=%s between IT and EN pages: it=%r en=%r",
            code,
            field_name,
            it_value,
            en_value,
        )


def merge_programme(
    *,
    code: str,
    year: int,
    dept_acronym: str,
    it_card: CrawledProgrammeCard | None,
    en_card: CrawledProgrammeCard | None,
) -> ProgrammeYaml | None:
    if it_card is None and en_card is None:
        return None

    if it_card is not None and en_card is not None:
        warn_if_mismatch(code, "campus", it_card.campus, en_card.campus)
        warn_if_mismatch(code, "duration", it_card.duration, en_card.duration)
        warn_if_mismatch(code, "languages", it_card.languages, en_card.languages)

    chosen_campus = (it_card.campus if it_card else "") or (en_card.campus if en_card else "")
    chosen_duration = (it_card.duration if it_card else None) or (en_card.duration if en_card else None)
    chosen_languages = sorted(
        set((it_card.languages if it_card else []) + (en_card.languages if en_card else []))
    )
    access_en = (en_card.access if en_card else "")

    if not access_en and it_card is not None and it_card.access:
        LOGGER.warning(
            "Programme %s is missing EN access text; using IT value as fallback",
            code,
        )
        access_en = it_card.access

    if chosen_duration is None:
        LOGGER.warning("Skipping programme %s because duration is unavailable", code)
        return None

    result = ProgrammeYaml(
        code=code,
        campus=chosen_campus,
        languages=chosen_languages or ["italian"],
        duration=chosen_duration,
        access=access_en,
        name=ProgrammeName(
            it=it_card.name if it_card else "",
            en=en_card.name if en_card else "",
        ),
        year=year,
        department=dept_acronym,
        urls=ProgrammeUrls(
            it=it_card.url if it_card else "",
            en=en_card.url if en_card else "",
        ),
    )
    LOGGER.info("Parsed %s programme %s: '%s' from %s", result.department, code, result.name.en, result.urls.en)
    return result


def crawl_and_write(args: argparse.Namespace) -> tuple[int, int]:
    written_count = 0
    skipped_count = 0

    selected_catalogs = (
        sorted(CATALOG_BASE_URLS)
        if args.catalog == CATALOG_ALL
        else [args.catalog]
    )

    for catalog_name in selected_catalogs:
        catalog_urls = CATALOG_BASE_URLS[catalog_name]
        italian_base_url = args.italian_base_url or catalog_urls["it"]
        english_base_url = args.english_base_url or catalog_urls["en"]
        default_duration: int | None = catalog_urls.get("default_duration")

        listing_url_it = build_listing_url(italian_base_url, args.year)
        listing_url_en = build_listing_url(english_base_url, args.year)

        html_it = fetch_html(
            listing_url_it,
            timeout=args.timeout,
            max_retries=args.max_retries,
            initial_backoff=args.initial_backoff,
            backoff_multiplier=args.backoff_multiplier,
            max_backoff=args.max_backoff,
            cache_dir=args.cache_dir,
            use_cache=not args.no_cache,
            refresh_cache=args.refresh_cache,
        )
        html_en = fetch_html(
            listing_url_en,
            timeout=args.timeout,
            max_retries=args.max_retries,
            initial_backoff=args.initial_backoff,
            backoff_multiplier=args.backoff_multiplier,
            max_backoff=args.max_backoff,
            cache_dir=args.cache_dir,
            use_cache=not args.no_cache,
            refresh_cache=args.refresh_cache,
        )

        departments_it = parse_department_buttons(html_it)
        departments_en = parse_department_buttons(html_en)
        dept_slugs = sorted(set(departments_it) | set(departments_en))

        for dept_slug in dept_slugs:
            dept_name_it = departments_it.get(dept_slug, "")
            dept_name_en = departments_en.get(dept_slug, "")
            dept_name_for_classification = dept_name_it or dept_name_en

            dept_acronym = classify_dept(dept_name_for_classification)
            if dept_acronym is None:
                LOGGER.warning(
                    "Could not classify department slug=%s name=%r; skipping its programmes",
                    dept_slug,
                    dept_name_for_classification,
                )
                skipped_count += 1
                continue

            cards_url_it = build_department_cards_url(italian_base_url, args.year, dept_slug)
            cards_url_en = build_department_cards_url(english_base_url, args.year, dept_slug)

            cards_html_it = fetch_html(
                cards_url_it,
                timeout=args.timeout,
                max_retries=args.max_retries,
                initial_backoff=args.initial_backoff,
                backoff_multiplier=args.backoff_multiplier,
                max_backoff=args.max_backoff,
                cache_dir=args.cache_dir,
                use_cache=not args.no_cache,
                refresh_cache=args.refresh_cache,
            )
            cards_html_en = fetch_html(
                cards_url_en,
                timeout=args.timeout,
                max_retries=args.max_retries,
                initial_backoff=args.initial_backoff,
                backoff_multiplier=args.backoff_multiplier,
                max_backoff=args.max_backoff,
                cache_dir=args.cache_dir,
                use_cache=not args.no_cache,
                refresh_cache=args.refresh_cache,
            )

            cards_it = parse_programme_cards(cards_html_it, "it", default_duration=default_duration)
            cards_en = parse_programme_cards(cards_html_en, "en", default_duration=default_duration)

            all_codes = sorted(set(cards_it) | set(cards_en))
            if not all_codes:
                LOGGER.warning("No programmes found for department slug=%s", dept_slug)
                continue

            for code in all_codes:
                merged = merge_programme(
                    code=code,
                    year=args.year,
                    dept_acronym=dept_acronym,
                    it_card=cards_it.get(code),
                    en_card=cards_en.get(code),
                )
                if merged is None:
                    skipped_count += 1
                    continue

                out_dir = args.output_dir / str(args.year) / dept_acronym
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"programme-{code}.yml"
                out_file.write_text(
                    yaml.safe_dump(
                        merged.model_dump(exclude_none=True),
                        sort_keys=False,
                        allow_unicode=True,
                    ),
                    encoding="utf-8",
                )
                written_count += 1

    return written_count, skipped_count


def main() -> int:
    configure_logging()
    LOGGER.info("Command line: %s", shlex.join(sys.argv))

    args = parse_args()
    LOGGER.info(
        "Starting crawl for catalog=%s year=%s",
        args.catalog,
        args.year,
    )

    written_count, skipped_count = crawl_and_write(args)
    LOGGER.info("Done. written=%s skipped=%s", written_count, skipped_count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import csv
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from string import ascii_uppercase
from urllib.parse import parse_qsl, quote_plus, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup
from bs4.element import Tag

from _utils import BASE_URL, DIR_DATA, download_html_page, configure_logging
from resources import classify_dept, classify_role
import pathlib


DEFAULT_URL = "https://www.unibo.it/uniboweb/unibosearch/rubrica.aspx"
DEFAULT_OUTPUT = DIR_DATA / "contacts.csv"
LOGGER = logging.getLogger(pathlib.Path(__file__).stem)


@dataclass(frozen=True)
class Contact:
    uid: str
    name: str
    role: str
    department: str
    address: str
    email: str
    website: str
    vcard: str
    ssd: str


SSD_PREFIXES = (
    "Settore scientifico disciplinare: ",
    "Academic discipline: ",
)


def clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def first_text(node: Tag, selector: str) -> str:
    element = node.select_one(selector)
    if element is None:
        return ""
    return clean_text(element.get_text(" ", strip=True))


def first_link(node: Tag, selector: str) -> str:
    element = node.select_one(selector)
    if element is None:
        return ""
    href = element.get("href", "").strip()
    if not href:
        return ""
    return urljoin(BASE_URL, href)


def extract_ssd(node: Tag) -> str:
    raw_ssd = first_text(node, "p.ssd")
    if not raw_ssd:
        return ""

    for prefix in SSD_PREFIXES:
        if raw_ssd.startswith(prefix):
            return raw_ssd[len(prefix):].strip()

    return raw_ssd.strip()


def parse_contact(table: Tag) -> Contact:
    uid=first_text(table, "th.uid")
    name=first_text(table, "td.fn.name")
    role=first_text(table, "tr.role td")
    department=first_text(table, "tr.org td")
    address=first_text(table, "tr.adr td")
    email = first_text(table, "a.email")
    website = first_link(table, "a.url")
    vcard = first_link(table, "tr td a[title*='Vcard']")
    ssd = extract_ssd(table)

    if (r := classify_role(role)) is not None:
        role = r
    else:
        LOGGER.warning(f"Unrecognized role '{role}' for contact '{name}' (uid: {uid})")

    if (d := classify_dept(department)) is not None:
        department = d
    else:
        LOGGER.warning(f"Unrecognized department '{department}' for contact '{name}' (uid: {uid})")

    return Contact(
        uid=uid,
        name=name,
        role=role,
        department=department,
        address=address,
        email=email,
        website=website,
        vcard=vcard,
        ssd=ssd,
    )


def parse_contacts(html: str) -> list[Contact]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.select("table.contact.vcard")
    return [parse_contact(table) for table in tables]


def parse_total_pages(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    total_pages = soup.select_one("#pages_before .totalPageNumber")
    if total_pages is None:
        return 1

    text = clean_text(total_pages.get_text(" ", strip=True))
    page_number = text.split()[-1] if text else ""
    return int(page_number) if page_number.isdigit() else 1


def build_page_url(url: str, page_number: int) -> str:
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page_number)
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment)
    )


def build_letter_url(base_url: str, letter: str) -> str:
    parsed = urlsplit(base_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["tab"] = "PersonePanel"
    query["mode"] = "advanced"
    query["query"] = f"+inizialecognome:{letter.upper()}"
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query, quote_via=quote_plus), parsed.fragment)
    )


def print_contacts(contacts: list[Contact]) -> None:
    for index, contact in enumerate(contacts, start=1):
        print(f"[{index}] {contact.name}")
        print(f"  uid: {contact.uid}")
        print(f"  role: {contact.role}")
        print(f"  department: {contact.department}")
        print(f"  address: {contact.address}")
        print(f"  email: {contact.email}")
        print(f"  website: {contact.website}")
        print(f"  vcard: {contact.vcard}")
        print(f"  ssd: {contact.ssd}")
        print()


def save_contacts_csv(contacts: list[Contact], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.info(f"Saving {len(contacts)} contacts to {destination}")

    with destination.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ["uid", "name", "role", "department", "address", "email", "website", "vcard", "ssd"]
        )
        for contact in contacts:
            writer.writerow(
                [
                    contact.uid,
                    contact.name,
                    contact.role,
                    contact.department,
                    contact.address,
                    contact.email,
                    contact.website,
                    contact.vcard,
                    contact.ssd,
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract and print contacts from the Unibo people directory."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="Base URL of the Unibo directory.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Optional pause before each HTTP request.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="HTTP request timeout in seconds.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output CSV file path.",
    )
    return parser.parse_args()


def scrape_letter(
    base_url: str, letter: str, delay: float, timeout: int
) -> tuple[list[Contact], int]:
    LOGGER.info(f"Scraping contacts for letter '{letter}'")
    letter_url = build_letter_url(base_url, letter)
    first_page_url = build_page_url(letter_url, 1)

    time.sleep(max(delay, 0))
    first_html = download_html_page(first_page_url, timeout=timeout)
    total_pages = parse_total_pages(first_html)
    contacts = parse_contacts(first_html)
    LOGGER.debug(f"Letter '{letter}': found {len(contacts)} contacts on page 1 of {total_pages}")

    for page_number in range(2, total_pages + 1):
        time.sleep(max(delay, 0))
        page_url = build_page_url(letter_url, page_number)
        html = download_html_page(page_url, timeout=timeout)
        new_contacts = parse_contacts(html)
        contacts.extend(new_contacts)
        LOGGER.debug(f"Letter '{letter}': found {len(new_contacts)} contacts on page {page_number} of {total_pages}")

    LOGGER.info(f"Letter '{letter}': scraped {len(contacts)} total contacts across {total_pages} pages")
    return contacts, total_pages


def main() -> None:
    configure_logging()
    LOGGER.info("Starting contact scraper")
    
    args = parse_args()
    LOGGER.debug(f"Arguments: url={args.url}, delay={args.delay}, timeout={args.timeout}, output={args.output}")
    
    contacts: list[Contact] = []
    total_pages = 0

    for letter in ascii_uppercase:
        letter_contacts, letter_pages = scrape_letter(
            base_url=args.url,
            letter=letter,
            delay=args.delay,
            timeout=args.timeout,
        )
        total_pages += letter_pages
        contacts.extend(letter_contacts)

    output_path = Path(args.output)
    save_contacts_csv(contacts, output_path)
    LOGGER.info(f"Success: Found {len(contacts)} contacts across {total_pages} total pages")
    LOGGER.info(f"CSV saved to: {output_path}")
    
    print_contacts(contacts)


if __name__ == "__main__":
    main()
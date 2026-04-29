from __future__ import annotations

import argparse
import csv
import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
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


def parse_contact(table: Tag) -> Contact:
    uid=first_text(table, "th.uid")
    name=first_text(table, "td.fn.name")
    role=first_text(table, "tr.role td")
    department=first_text(table, "tr.org td")
    address=first_text(table, "tr.adr td").replace("[ Vai alla mappa ]", "").strip()
    email = first_text(table, "a.email")
    website = first_link(table, "a.url")
    vcard = first_link(table, "tr td a[title*='Vcard']")

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


def print_contacts(index: int, contact: Contact) -> None:
    message = f"Extract contact #{index}: {contact.name}"
    message += f"\n  uid: {contact.uid}"
    message += f"\n  role: {contact.role}"
    message += f"\n  department: {contact.department}"
    message += f"\n  address: {contact.address}"
    message += f"\n  email: {contact.email}"
    message += f"\n  website: {contact.website}"
    message += f"\n  vcard: {contact.vcard}"
    LOGGER.info(message)

def contact_row(contact: Contact) -> list[str]:
    return [
        contact.uid,
        contact.name,
        contact.role,
        contact.department,
        contact.address,
        contact.email,
        contact.website,
        contact.vcard,
    ]


@contextmanager
def open_contacts_csv_writer(destination: Path) -> Iterator[Callable[[Contact], None]]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.info(f"Streaming contacts to {destination}")

    with destination.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ["uid", "name", "role", "department", "address", "email", "website", "vcard"]
        )
        csv_file.flush()

        def write_contact(contact: Contact) -> None:
            writer.writerow(contact_row(contact))
            csv_file.flush()

        yield write_contact


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
    base_url: str,
    letter: str,
    delay: float,
    timeout: int,
    on_contact: Callable[[Contact], None],
) -> tuple[int, int]:
    LOGGER.info(f"Scraping contacts for letter '{letter}'")
    letter_url = build_letter_url(base_url, letter)
    first_page_url = build_page_url(letter_url, 1)

    time.sleep(max(delay, 0))
    first_html = download_html_page(first_page_url, timeout=timeout)
    total_pages = parse_total_pages(first_html)
    contacts_count = 0

    first_page_contacts = parse_contacts(first_html)
    for contact in first_page_contacts:
        on_contact(contact)
    contacts_count += len(first_page_contacts)
    LOGGER.info(f"Letter '{letter}': found {len(first_page_contacts)} contacts on page 1 of {total_pages}")

    for page_number in range(2, total_pages + 1):
        time.sleep(max(delay, 0))
        page_url = build_page_url(letter_url, page_number)
        html = download_html_page(page_url, timeout=timeout)
        new_contacts = parse_contacts(html)
        for contact in new_contacts:
            on_contact(contact)
        contacts_count += len(new_contacts)
        LOGGER.info(f"Letter '{letter}': found {len(new_contacts)} contacts on page {page_number} of {total_pages}")

    LOGGER.info(f"Letter '{letter}': scraped {contacts_count} total contacts across {total_pages} pages")
    return contacts_count, total_pages


def main() -> None:
    configure_logging()
    LOGGER.info("Starting contact scraper")

    args = parse_args()
    LOGGER.info(f"Arguments: url={args.url}, delay={args.delay}, timeout={args.timeout}, output={args.output}")

    contacts_count = 0
    total_pages = 0
    printed_contacts = 0

    output_path = Path(args.output)
    with open_contacts_csv_writer(output_path) as write_contact:
        def write_and_print_contact(contact: Contact) -> None:
            nonlocal printed_contacts
            write_contact(contact)
            printed_contacts += 1
            print_contacts(printed_contacts, contact)

        for letter in ascii_uppercase:
            letter_contacts, letter_pages = scrape_letter(
                base_url=args.url,
                letter=letter,
                delay=args.delay,
                timeout=args.timeout,
                on_contact=write_and_print_contact,
            )
            total_pages += letter_pages
            contacts_count += letter_contacts

    LOGGER.info(f"Success: Found {contacts_count} contacts across {total_pages} total pages")
    LOGGER.info(f"CSV saved to: {output_path}")


if __name__ == "__main__":
    main()
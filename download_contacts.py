from __future__ import annotations

import argparse
import csv
import time
from string import ascii_uppercase
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, quote_plus, urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from _utils import BASE_URL, DATA_DIR, DEFAULT_HEADERS


DEFAULT_URL = "https://www.unibo.it/uniboweb/unibosearch/rubrica.aspx"
DEFAULT_OUTPUT = DATA_DIR / "contacts.csv"


@dataclass(frozen=True)
class Contact:
    uid: str
    nome: str
    ruolo: str
    dipartimento: str
    indirizzo: str
    email: str
    sito_web: str
    vcard: str


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def fetch_html(session: requests.Session, url: str, timeout_seconds: int) -> str:
    response = session.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.text


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
    email = first_text(table, "a.email")
    sito_web = first_link(table, "a.url")
    vcard = first_link(table, "tr td a[title='scarica la Vcard']")

    return Contact(
        uid=first_text(table, "th.uid"),
        nome=first_text(table, "td.fn.name"),
        ruolo=first_text(table, "tr.role td"),
        dipartimento=first_text(table, "tr.org td"),
        indirizzo=first_text(table, "tr.adr td"),
        email=email,
        sito_web=sito_web,
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


def print_contacts(contacts: list[Contact]) -> None:
    for index, contact in enumerate(contacts, start=1):
        print(f"[{index}] {contact.nome}")
        print(f"  uid: {contact.uid}")
        print(f"  ruolo: {contact.ruolo}")
        print(f"  dipartimento: {contact.dipartimento}")
        print(f"  indirizzo: {contact.indirizzo}")
        print(f"  email: {contact.email}")
        print(f"  sito_web: {contact.sito_web}")
        print(f"  vcard: {contact.vcard}")
        print()


def save_contacts_csv(contacts: list[Contact], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ["uid", "nome", "ruolo", "dipartimento", "indirizzo", "email", "sito_web", "vcard"]
        )
        for contact in contacts:
            writer.writerow(
                [
                    contact.uid,
                    contact.nome,
                    contact.ruolo,
                    contact.dipartimento,
                    contact.indirizzo,
                    contact.email,
                    contact.sito_web,
                    contact.vcard,
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estrae e stampa i contatti dalla rubrica persone di Unibo."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="URL base della rubrica Unibo.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Pausa opzionale prima della richiesta HTTP.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Timeout della richiesta HTTP in secondi.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Percorso del file CSV di output.",
    )
    return parser.parse_args()


def scrape_letter(
    session: requests.Session, base_url: str, letter: str, delay: float, timeout: int
) -> tuple[list[Contact], int]:
    letter_url = build_letter_url(base_url, letter)
    first_page_url = build_page_url(letter_url, 1)

    time.sleep(max(delay, 0))
    first_html = fetch_html(session, first_page_url, timeout)
    total_pages = parse_total_pages(first_html)
    contacts = parse_contacts(first_html)

    for page_number in range(2, total_pages + 1):
        time.sleep(max(delay, 0))
        page_url = build_page_url(letter_url, page_number)
        html = fetch_html(session, page_url, timeout)
        contacts.extend(parse_contacts(html))

    return contacts, total_pages


def main() -> None:
    args = parse_args()
    session = build_session()
    contacts: list[Contact] = []
    total_pages = 0

    for letter in ascii_uppercase:
        letter_contacts, letter_pages = scrape_letter(
            session=session,
            base_url=args.url,
            letter=letter,
            delay=args.delay,
            timeout=args.timeout,
        )
        total_pages += letter_pages
        contacts.extend(letter_contacts)

    output_path = Path(args.output)
    save_contacts_csv(contacts, output_path)

    print(f"Trovati {len(contacts)} contatti su {total_pages} pagine totali")
    print(f"CSV salvato in: {output_path}")
    print()
    print_contacts(contacts)


if __name__ == "__main__":
    main()
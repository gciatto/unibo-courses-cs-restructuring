from __future__ import annotations

import argparse
import pathlib
import sys
import re
from typing import TextIO

from html_to_markdown import convert


STDIN_MARKER = "-"


def convert_html_to_markdown(
    source: str | TextIO,
    destination: TextIO | None = None,
    ignore_before: None | re.Pattern[str] | str = None,
    ignore_since: None | re.Pattern[str] | str = None,
) -> str | TextIO:
    if hasattr(source, "read"):
        html = source.read()
    else:
        html = source
    if isinstance(ignore_before, str):
        ignore_before = re.compile(ignore_before)
    if isinstance(ignore_since, str):
        ignore_since = re.compile(ignore_since)

    markdown = convert(html).content

    if destination is None:
        destination = ""

    def append_to_destination(line: str) -> None:
        nonlocal destination
        if isinstance(destination, str):
            destination += line + "\n"
        else:
            destination.write(line + "\n")

    outputting = ignore_before is None
    empty_line_count = 0
    for line in markdown.splitlines():
        if line.strip() == "":
            empty_line_count += 1
        else:
            empty_line_count = 0
        if not outputting and ignore_before is not None and ignore_before.match(line):
            outputting = True
        if outputting and ignore_since is not None and ignore_since.match(line):
            break
        if outputting and empty_line_count <= 1:
            append_to_destination(line)

    return destination


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert HTML input to Markdown using html_to_markdown.",
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=STDIN_MARKER,
        help=(
            "HTML input as a file path, raw string, or '-' to read from stdin "
            "(default: stdin)."
        ),
    )
    parser.add_argument(
        "-o",
        "--output-file",
        action="store_true",
        help="Write output to a .md file next to the input file instead of stdout.",
    )
    parser.add_argument(
        "--ignore-before",
        "-b",
        type=re.compile,
        default=None,
        help="Start output only from the first Markdown line matching this regex.",
    )
    parser.add_argument(
        "--ignore-since",
        "-s",
        type=re.compile,
        default=None,
        help="Stop output after the first Markdown line matching this regex.",
    )
    return parser.parse_args()


def resolve_input(value: str) -> tuple[str | TextIO, pathlib.Path | None]:
    if value == STDIN_MARKER:
        return sys.stdin, None

    input_path = pathlib.Path(value)
    if input_path.exists() and input_path.is_file():
        return input_path.read_text(encoding="utf-8"), input_path

    return value, None


def build_output_path(input_path: pathlib.Path) -> pathlib.Path:
    return input_path.with_suffix(".md")


def main() -> int:
    args = parse_args()
    source, input_path = resolve_input(args.input)

    if args.output_file:
        if input_path is None:
            print(
                "ERROR: --output-file requires the input to be an existing file path.",
                file=sys.stderr,
            )
            return 2

        output_path = build_output_path(input_path)
        markdown = convert_html_to_markdown(
            source,
            ignore_before=args.ignore_before,
            ignore_since=args.ignore_since,
        )
        output_path.write_text(markdown, encoding="utf-8")
        return 0

    convert_html_to_markdown(
        source,
        sys.stdout,
        ignore_before=args.ignore_before,
        ignore_since=args.ignore_since,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



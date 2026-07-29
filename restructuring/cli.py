from __future__ import annotations

import argparse
import logging
import os
import pathlib

from restructuring.models import ModelConfig, RetryConfig
from restructuring.io import DEFAULT_SYLLABUS_SECTION_KEYS
from restructuring.workflow import TOPIC_CONVERSATION_MODES, run_restructuring


DEFAULT_ENDPOINT = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-5.6-luna"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract syllabus-grounded topics and propose course restructurings with an LLM."
    )
    parser.add_argument("input", type=pathlib.Path, help="Path to cluster_courses.yml")
    parser.add_argument("--endpoint", default=os.environ.get("OPENAI_BASE_URL", DEFAULT_ENDPOINT))
    parser.add_argument("--model", default=os.environ.get("OPENAI_MODEL", DEFAULT_MODEL))
    parser.add_argument(
        "--temperature",
        type=float,
        default=os.environ.get("RESTRUCTURING_TEMPERATURE"),
        help="Sampling temperature; omitted from API requests by default",
    )
    parser.add_argument(
        "--max-completion-tokens",
        type=int,
        default=os.environ.get("RESTRUCTURING_MAX_COMPLETION_TOKENS", "8192"),
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=os.environ.get("RESTRUCTURING_REQUEST_TIMEOUT", "120"),
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=os.environ.get("RESTRUCTURING_MAX_RETRIES", "6"),
    )
    parser.add_argument(
        "--initial-backoff",
        type=float,
        default=os.environ.get("RESTRUCTURING_INITIAL_BACKOFF", "1"),
    )
    parser.add_argument(
        "--max-backoff",
        type=float,
        default=os.environ.get("RESTRUCTURING_MAX_BACKOFF", "60"),
    )
    parser.add_argument("--cluster-id", type=int, action="append", default=[])
    parser.add_argument("--cluster-name-regex", action="append", default=[])
    parser.add_argument(
        "--syllabus-sections",
        nargs="+",
        default=list(DEFAULT_SYLLABUS_SECTION_KEYS),
        choices=("title", "outcomes", "contents", "bib", "teaching_methods", "assessment", "teaching_tools", "office_hours"),
        metavar="SECTION",
        help=(
            "Syllabus sections to include in the reconstructed markdown. "
            "Keywords: title, outcomes, contents, bib, teaching_methods, assessment, teaching_tools, office_hours."
        ),
    )
    parser.add_argument(
        "--topic-conversation-mode",
        choices=TOPIC_CONVERSATION_MODES,
        default="stateless",
        help=(
            "Context retained across topic-extraction calls: 'stateless' sends only "
            "the current ontology and syllabus (default), while 'full' also sends "
            "all preceding course turns."
        ),
    )
    parser.add_argument("--refresh-cache", action="store_true")
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if not str(args.endpoint).strip():
        raise ValueError("--endpoint must not be empty")
    if not str(args.model).strip():
        raise ValueError("--model must not be empty")
    if args.max_completion_tokens <= 0:
        raise ValueError("--max-completion-tokens must be > 0")
    if args.request_timeout <= 0:
        raise ValueError("--request-timeout must be > 0")
    if args.max_retries < 0:
        raise ValueError("--max-retries must be >= 0")
    if args.initial_backoff < 0:
        raise ValueError("--initial-backoff must be >= 0")
    if args.max_backoff < 0:
        raise ValueError("--max-backoff must be >= 0")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = build_parser()
    try:
        args = parser.parse_args()
        _validate_args(args)
        output_dir = run_restructuring(
            args.input,
            ModelConfig(
                endpoint=args.endpoint,
                model=args.model,
                temperature=args.temperature,
                max_completion_tokens=args.max_completion_tokens,
            ),
            RetryConfig(
                max_retries=args.max_retries,
                initial_backoff=args.initial_backoff,
                max_backoff=args.max_backoff,
            ),
            syllabus_section_keys=tuple(args.syllabus_sections),
            topic_conversation_mode=args.topic_conversation_mode,
            cluster_ids=tuple(args.cluster_id),
            cluster_name_regexes=tuple(args.cluster_name_regex),
            refresh_cache=args.refresh_cache,
            request_timeout=args.request_timeout,
        )
    except (RuntimeError, ValueError) as error:
        parser.error(str(error))
    print(output_dir)


if __name__ == "__main__":
    main()

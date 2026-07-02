from __future__ import annotations

import hashlib
import json
import pathlib
from typing import Any

import yaml


EMBEDDING_SCHEMA_VERSION = 1
SIMILARITY_SCHEMA_VERSION = 1


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_config_hash(payload: dict[str, Any], length: int = 16) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:length]


def embedding_cache_path(course_path: pathlib.Path, course_sha256: str, model_config_hash: str) -> pathlib.Path:
    return course_path.parent / f"embeddings-{course_sha256}-{model_config_hash}.yml"


def similarity_cache_dir(year_dir: pathlib.Path, model_config_hash: str) -> pathlib.Path:
    return year_dir / ".similarities" / model_config_hash


def sorted_pair_shas(sha_a: str, sha_b: str) -> tuple[str, str]:
    return tuple(sorted((sha_a, sha_b)))  # type: ignore[return-value]


def similarity_cache_path(year_dir: pathlib.Path, sha_a: str, sha_b: str, model_config_hash: str) -> pathlib.Path:
    first, second = sorted_pair_shas(sha_a, sha_b)
    return similarity_cache_dir(year_dir, model_config_hash) / f"similarity-{first}-and-{second}.yml"


def read_yaml_cache(path: pathlib.Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else None


def write_yaml_cache(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def prune_embedding_caches(year_dir: pathlib.Path, valid_course_shas: set[str], model_config_hash: str) -> int:
    removed = 0
    for path in year_dir.glob("embeddings-*-*.yml"):
        name = path.name
        prefix = "embeddings-"
        suffix = ".yml"
        stem = name[len(prefix) : -len(suffix)]
        course_sha, _, cache_hash = stem.partition("-")
        if course_sha not in valid_course_shas or cache_hash != model_config_hash:
            path.unlink()
            removed += 1
    return removed


def prune_similarity_caches(year_dir: pathlib.Path, valid_course_shas: set[str], model_config_hash: str) -> int:
    root = similarity_cache_dir(year_dir, model_config_hash)
    if not root.exists():
        return 0

    removed = 0
    for path in root.glob("similarity-*-and-*.yml"):
        stem = path.stem.removeprefix("similarity-")
        first, separator, second = stem.partition("-and-")
        if not separator or first not in valid_course_shas or second not in valid_course_shas:
            path.unlink()
            removed += 1
    return removed

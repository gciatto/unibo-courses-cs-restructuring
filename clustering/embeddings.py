from __future__ import annotations

import hashlib
import logging
import math
import re
from dataclasses import dataclass
from typing import Protocol, Sequence

import numpy as np

from clustering.cache import (
    EMBEDDING_SCHEMA_VERSION,
    embedding_cache_path,
    read_yaml_cache,
    stable_config_hash,
    write_yaml_cache,
)
from clustering.course_io import CourseRecord
from clustering.sections import SECTION_NAMES


LOGGER = logging.getLogger(__name__)

DEFAULT_EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"


class TextEmbedder(Protocol):
    dimension: int | None

    def encode_texts(self, texts: Sequence[str], batch_size: int) -> np.ndarray:
        ...


@dataclass(frozen=True)
class EmbeddingConfig:
    model_name: str = DEFAULT_EMBEDDING_MODEL
    chunk_token_limit: int = 384
    embedding_batch_size: int = 32
    device: str = "auto"
    normalize_embeddings: bool = True
    schema_version: int = EMBEDDING_SCHEMA_VERSION

    def model_config_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "embedding_model": self.model_name,
            "chunk_token_limit": self.chunk_token_limit,
            "normalize_chunks_before_average": self.normalize_embeddings,
            "normalize_section_vectors": self.normalize_embeddings,
            "section_names": SECTION_NAMES,
        }

    @property
    def model_config_hash(self) -> str:
        return stable_config_hash(self.model_config_payload())


def resolve_device(device: str) -> str:
    if device != "auto":
        return device

    try:
        import torch

        if torch.cuda.is_available():
            return "cuda"
        if getattr(torch.backends, "mps", None) is not None and torch.backends.mps.is_available():
            return "mps"
    except Exception:
        pass
    return "cpu"


class SentenceTransformerEmbedder:
    def __init__(self, model_name: str, device: str = "auto") -> None:
        self.model_name = model_name
        self.device = resolve_device(device)
        self._model = None
        self.dimension: int | None = None

    def _load(self):
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as error:
                raise RuntimeError(
                    "sentence-transformers is required for real embeddings. "
                    "Install requirements.txt or pass a test embedder programmatically."
                ) from error
            LOGGER.info("Loading embedding model %s on %s", self.model_name, self.device)
            self._model = SentenceTransformer(self.model_name, device=self.device)
            try:
                self.dimension = int(self._model.get_sentence_embedding_dimension())
            except Exception:
                self.dimension = None
        return self._model

    def encode_texts(self, texts: Sequence[str], batch_size: int) -> np.ndarray:
        model = self._load()
        encoded = model.encode(
            list(texts),
            batch_size=batch_size,
            normalize_embeddings=False,
            show_progress_bar=False,
        )
        array = np.asarray(encoded, dtype=float)
        if array.ndim == 1:
            array = array.reshape(1, -1)
        if self.dimension is None and array.size:
            self.dimension = int(array.shape[1])
        return array


class DeterministicHashEmbedder:
    """Small deterministic bag-of-words embedder for tests and smoke runs."""

    def __init__(self, dimension: int = 32) -> None:
        self.dimension = dimension

    def encode_texts(self, texts: Sequence[str], batch_size: int) -> np.ndarray:
        rows = [self._embed_one(text) for text in texts]
        return np.vstack(rows) if rows else np.zeros((0, self.dimension), dtype=float)

    def _embed_one(self, text: str) -> np.ndarray:
        vector = np.zeros(self.dimension, dtype=float)
        tokens = re.findall(r"[\w']+", text.casefold())
        if not tokens:
            return vector

        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:4], "big") % self.dimension
            sign = 1.0 if digest[4] % 2 == 0 else -1.0
            vector[index] += sign
        return normalize_vector(vector)


def normalize_vector(vector: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(vector))
    if not math.isfinite(norm) or norm == 0.0:
        return vector.astype(float, copy=True)
    return vector.astype(float, copy=True) / norm


def normalize_rows(matrix: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return matrix
    norms = np.linalg.norm(matrix, axis=1)
    result = matrix.astype(float, copy=True)
    nonzero = norms > 0
    result[nonzero] = result[nonzero] / norms[nonzero, None]
    return result


def chunk_text(text: str, token_limit: int) -> list[str]:
    if token_limit <= 0:
        raise ValueError("chunk token limit must be > 0")

    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", text) if part.strip()]
    chunks: list[str] = []

    for paragraph in paragraphs or [text.strip()]:
        words = paragraph.split()
        if not words:
            continue
        for start in range(0, len(words), token_limit):
            chunks.append(" ".join(words[start : start + token_limit]))

    return chunks


def embed_section(text: str | None, embedder: TextEmbedder, config: EmbeddingConfig) -> np.ndarray | None:
    if text is None or not text.strip():
        return None

    chunks = chunk_text(text, config.chunk_token_limit)
    if not chunks:
        return None

    vectors = embedder.encode_texts(chunks, batch_size=config.embedding_batch_size)
    if vectors.size == 0:
        return None
    if config.normalize_embeddings:
        vectors = normalize_rows(vectors)

    section_vector = vectors.mean(axis=0)
    if config.normalize_embeddings:
        section_vector = normalize_vector(section_vector)
    return section_vector


def embed_course_sections(
    course: CourseRecord,
    embedder: TextEmbedder,
    config: EmbeddingConfig,
) -> dict[str, np.ndarray | None]:
    return {
        section_name: embed_section(course.sections.get(section_name), embedder, config)
        for section_name in SECTION_NAMES
    }


def _embedding_cache_payload(
    course: CourseRecord,
    config: EmbeddingConfig,
    model_config_hash: str,
    embeddings: dict[str, np.ndarray | None],
) -> dict[str, object]:
    first_vector = next((vector for vector in embeddings.values() if vector is not None), None)
    dimension = int(first_vector.shape[0]) if first_vector is not None else 0
    return {
        "metadata": {
            "schema_version": EMBEDDING_SCHEMA_VERSION,
            "course_id": course.course_id,
            "course_path": str(course.path),
            "course_sha256": course.sha256,
            "embedding_model": config.model_name,
            "model_config_hash": model_config_hash,
            "embedding_dimension": dimension,
        },
        "embeddings": {
            section_name: (vector.astype(float).tolist() if vector is not None else None)
            for section_name, vector in embeddings.items()
        },
    }


def _read_embedding_cache(
    course: CourseRecord,
    config: EmbeddingConfig,
    model_config_hash: str,
) -> dict[str, np.ndarray | None] | None:
    path = embedding_cache_path(course.path, course.sha256, model_config_hash)
    payload = read_yaml_cache(path)
    if payload is None:
        return None
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return None
    if metadata.get("schema_version") != EMBEDDING_SCHEMA_VERSION:
        return None
    if metadata.get("course_sha256") != course.sha256:
        return None
    if metadata.get("embedding_model") != config.model_name:
        return None
    if metadata.get("model_config_hash") != model_config_hash:
        return None

    raw_embeddings = payload.get("embeddings")
    if not isinstance(raw_embeddings, dict):
        return None

    embeddings: dict[str, np.ndarray | None] = {}
    for section_name in SECTION_NAMES:
        value = raw_embeddings.get(section_name)
        embeddings[section_name] = None if value is None else np.asarray(value, dtype=float)
    return embeddings


def load_or_compute_course_embeddings(
    course: CourseRecord,
    embedder: TextEmbedder,
    config: EmbeddingConfig,
    *,
    no_cache: bool = False,
    refresh: bool = False,
) -> dict[str, np.ndarray | None]:
    model_config_hash = config.model_config_hash
    cache_path = embedding_cache_path(course.path, course.sha256, model_config_hash)

    if not no_cache and not refresh:
        cached = _read_embedding_cache(course, config, model_config_hash)
        if cached is not None:
            return cached

    embeddings = embed_course_sections(course, embedder, config)
    if not no_cache:
        write_yaml_cache(cache_path, _embedding_cache_payload(course, config, model_config_hash, embeddings))
    return embeddings


def load_or_compute_embeddings(
    courses: Sequence[CourseRecord],
    embedder: TextEmbedder,
    config: EmbeddingConfig,
    *,
    no_cache: bool = False,
    refresh: bool = False,
) -> tuple[dict[str, dict[str, np.ndarray | None]], int]:
    by_sha: dict[str, dict[str, np.ndarray | None]] = {}
    dimension = 0
    for index, course in enumerate(courses, start=1):
        LOGGER.info("Embedding %s/%s %s", index, len(courses), course.path)
        embeddings = load_or_compute_course_embeddings(
            course,
            embedder,
            config,
            no_cache=no_cache,
            refresh=refresh,
        )
        by_sha[course.sha256] = embeddings
        first_vector = next((vector for vector in embeddings.values() if vector is not None), None)
        if first_vector is not None:
            dimension = int(first_vector.shape[0])
    if dimension == 0 and embedder.dimension:
        dimension = int(embedder.dimension)
    return by_sha, dimension

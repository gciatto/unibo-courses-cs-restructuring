from __future__ import annotations

import pathlib
import tempfile
import unittest

from clustering.cache import embedding_cache_path, file_sha256, similarity_cache_path, stable_config_hash


class TestClusteringCache(unittest.TestCase):
    def test_embedding_cache_path_is_digest_based(self):
        path = pathlib.Path("data/courses/.files/2025/course-00819-B.yml")
        cache_path = embedding_cache_path(path, "a" * 64, "deadbeef")

        self.assertEqual(
            cache_path,
            pathlib.Path("data/courses/.files/2025/embeddings-" + "a" * 64 + "-deadbeef.yml"),
        )

    def test_similarity_cache_path_sorts_pair_digests(self):
        year_dir = pathlib.Path("data/courses/.files/2025")
        cache_path = similarity_cache_path(year_dir, "b" * 64, "a" * 64, "modelhash")

        self.assertEqual(
            cache_path,
            year_dir / ".similarities" / "modelhash" / ("similarity-" + "a" * 64 + "-and-" + "b" * 64 + ".yml"),
        )

    def test_cache_key_changes_when_file_digest_changes(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = pathlib.Path(tmp_dir) / "course-1.yml"
            path.write_text("title: one\n", encoding="utf-8")
            first_sha = file_sha256(path)
            path.write_text("title: two\n", encoding="utf-8")
            second_sha = file_sha256(path)

        self.assertNotEqual(first_sha, second_sha)
        self.assertNotEqual(
            embedding_cache_path(path, first_sha, "model"),
            embedding_cache_path(path, second_sha, "model"),
        )

    def test_model_config_hash_is_stable(self):
        self.assertEqual(
            stable_config_hash({"b": 2, "a": 1}),
            stable_config_hash({"a": 1, "b": 2}),
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import datetime as dt
import pathlib
import tempfile
import unittest

import yaml

from clustering.cli import build_parser, run
from clustering.embeddings import DeterministicHashEmbedder


try:
    import sklearn  # noqa: F401

    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


def write_course(path: pathlib.Path, course_id: str, title: str, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "year": 2025,
                "course_title": {"id": course_id, "name": title},
                "syllabus": {
                    "en": {
                        "contents": {
                            "Learning outcomes": text,
                            "Course contents": text,
                            "Readings/Bibliography": text,
                        }
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


class TestClusterCoursesCli(unittest.TestCase):
    def test_cli_defaults(self):
        args = build_parser().parse_args([])

        self.assertEqual(args.year, dt.date.today().year - 1)
        self.assertEqual(args.algorithm, "agglomerative")

    @unittest.skipUnless(HAS_SKLEARN, "scikit-learn is required for CLI smoke test")
    def test_output_files_are_created(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            courses_dir = root / "courses"
            year_dir = courses_dir / "2025"
            write_course(year_dir / "course-A.yml", "A", "Data Mining", "data mining database analytics")
            write_course(year_dir / "course-B.yml", "B", "Business Intelligence", "data mining database analytics")
            write_course(year_dir / "course-C.yml", "C", "Compilers", "compiler parsing code generation")
            write_course(year_dir / "course-D.yml", "D", "Programming Languages", "compiler parsing code generation")

            args = build_parser().parse_args(
                [
                    "--year",
                    "2025",
                    "--courses-dir",
                    str(courses_dir),
                    "--output-dir",
                    str(root / "runs"),
                    "--algorithm",
                    "agglomerative",
                    "--n-clusters",
                    "2",
                    "--no-cache",
                ]
            )
            run_dir = run(args, embedder=DeterministicHashEmbedder(dimension=16))

            for relative in (
                "run_config.yml",
                "courses.csv",
                "similarity_matrix.csv",
                "distance_matrix.csv",
                "clusters.csv",
                "cluster_summary.yml",
                "nearest_neighbors.csv",
                "top_pairs.csv",
                "charts/similarity_heatmap.png",
                "charts/cluster_sizes.png",
                "charts/projection_pca.png",
                "charts/dendrogram.png",
            ):
                self.assertTrue((run_dir / relative).exists(), relative)


if __name__ == "__main__":
    unittest.main()

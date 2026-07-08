from __future__ import annotations

import datetime as dt
import pathlib
import tempfile
import unittest

import yaml

from clustering.cli import build_parser, main, run
from clustering.embeddings import DeterministicHashEmbedder


try:
    import sklearn  # noqa: F401

    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


def write_course(
    path: pathlib.Path,
    course_id: str,
    title: str,
    text: str,
    teachers: list[dict[str, str]] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "year": 2025,
                "teachers": teachers or [],
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
        self.assertIsNone(args.regenerate_charts)

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
                "cluster_courses.yml",
                "cluster_courses.short.yml",
                "nearest_neighbors.csv",
                "top_pairs.csv",
                "charts/similarity_heatmap.png",
                "charts/cluster_sizes.png",
                "charts/projection_pca.png",
                "charts/dendrogram.png",
            ):
                self.assertTrue((run_dir / relative).exists(), relative)

            cluster_courses = yaml.safe_load((run_dir / "cluster_courses.yml").read_text(encoding="utf-8"))
            self.assertIsInstance(cluster_courses, dict)
            self.assertTrue(cluster_courses)
            first_cluster = next(iter(cluster_courses.values()))
            self.assertIn("index", first_cluster)
            self.assertIn("courses", first_cluster)
            self.assertTrue(first_cluster["courses"])

    @unittest.skipUnless(HAS_SKLEARN, "scikit-learn is required for CLI smoke test")
    def test_non_agglomerative_run_creates_dendrogram(self):
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
                    "kmeans",
                    "--n-clusters",
                    "2",
                    "--no-cache",
                ]
            )
            run_dir = run(args, embedder=DeterministicHashEmbedder(dimension=16))

            self.assertTrue((run_dir / "charts" / "dendrogram.png").exists())

    @unittest.skipUnless(HAS_SKLEARN, "scikit-learn is required for CLI smoke test")
    def test_regenerate_charts_from_existing_run(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            courses_dir = root / "courses"
            year_dir = courses_dir / "2025"
            write_course(
                year_dir / "course-A.yml",
                "A",
                "Data Mining",
                "data mining database analytics",
                teachers=[
                    {"email": "zeta.teacher@unibo.it"},
                    {"email": "alpha.teacher@unibo.it"},
                ],
            )
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

            (run_dir / "cluster_courses.short.yml").unlink()
            for chart in (run_dir / "charts").glob("*.png"):
                chart.unlink()

            exit_code = main(["--regenerate-charts", str(run_dir)])

            self.assertEqual(exit_code, 0)
            for relative in (
                "cluster_courses.short.yml",
                "charts/similarity_heatmap.png",
                "charts/cluster_sizes.png",
                "charts/projection_pca.png",
                "charts/dendrogram.png",
            ):
                self.assertTrue((run_dir / relative).exists(), relative)

            short_report = yaml.safe_load((run_dir / "cluster_courses.short.yml").read_text(encoding="utf-8"))
            course_values = [
                value
                for cluster_data in short_report.values()
                for value in cluster_data["courses"].values()
            ]
            self.assertIn("Data Mining | alpha.teacher, zeta.teacher", course_values)


if __name__ == "__main__":
    unittest.main()

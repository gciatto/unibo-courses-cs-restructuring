from __future__ import annotations

import pathlib
import tempfile
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from restructuring.cli import build_parser
from restructuring.io import (
    DEFAULT_SYLLABUS_SECTION_KEYS,
    conversation_cache_key,
    load_clusters,
    select_clusters,
)
from restructuring.models import (
    ClusterInput,
    CourseInput,
    CourseTopicMembership,
    CourseTopicsResponse,
    FinalClusterResponse,
    ModelConfig,
    RetryConfig,
    Topic,
)
from restructuring.workflow import (
    PROMPTS_DIR,
    call_with_backoff,
    process_cluster,
    render_plantuml_svg,
    run_restructuring,
)


def course(course_id: str, contents: str = "Alpha material") -> CourseInput:
    return CourseInput(
        course_id=course_id,
        title=f"Misleading title {course_id}",
        path=f"course-{course_id}.yml",
        course_contents=contents,
        course_contents_language="en",
        learning_outcomes="Apply the supplied material.",
        learning_outcomes_language="en",
        syllabus_sections=(
            ("Learning outcomes", "Apply the supplied material."),
            ("Course contents", contents),
        ),
    )


def cluster(cluster_id: int, name: str, *courses: CourseInput) -> ClusterInput:
    return ClusterInput(cluster_id=cluster_id, name=name, courses=tuple(courses))


class FakeCompletions:
    def __init__(self, responses: list[object]):
        self.responses = list(responses)
        self.calls: list[dict] = []

    def parse(self, **arguments):
        self.calls.append(arguments)
        if not self.responses:
            raise AssertionError("Unexpected API call")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        message = SimpleNamespace(parsed=response, content=response.model_dump_json(), refusal=None)
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])


class FakeClient:
    def __init__(self, responses: list[object]):
        self.completions = FakeCompletions(responses)
        self.chat = SimpleNamespace(completions=self.completions)


class FakePlantUMLRenderer:
    def __init__(self, failures: int = 0, svg: str = "<svg></svg>"):
        self.failures = failures
        self.svg = svg
        self.calls: list[tuple[str, str, str]] = []

    def dump(self, path: str, output_format: str, code: str) -> None:
        self.calls.append((path, output_format, code))
        if len(self.calls) <= self.failures:
            raise TimeoutError("temporary renderer timeout")
        pathlib.Path(path).write_text(self.svg, encoding="utf-8")


def response_for_course(*topics: Topic, covered: list[str]) -> CourseTopicsResponse:
    return CourseTopicsResponse(topics=list(topics), covered_topic_keys=covered)


def final_response() -> FinalClusterResponse:
    return FinalClusterResponse(
        topics=[
            Topic(key="alpha", description="Alpha from the supplied syllabus."),
            Topic(key="beta", description="Beta from the supplied syllabus."),
        ],
        course_topics=[
            CourseTopicMembership(course_id="A", topic_keys=["alpha"]),
            CourseTopicMembership(course_id="B", topic_keys=["beta"]),
        ],
        plantuml="""@startuml
class "A - Old A" as OLD_A #Transparent
class "B - Old B" as OLD_B #Transparent
class Foundations {
  alpha
}
class Advanced {
  beta
}
note right of Foundations
alpha: Alpha from the supplied syllabus.
end note
note right of Advanced
beta: Beta from the supplied syllabus.
end note
Foundations <|-- Advanced
OLD_A ..> Foundations : subsumed by
OLD_B ..> Advanced : subsumed by
@enduml""",
    )


class TestClusterSelection(unittest.TestCase):
    def setUp(self):
        self.clusters = [
            cluster(1, "Programming Foundations (1)"),
            cluster(2, "Advanced Databases (2)"),
            cluster(3, "Programming Languages (3)"),
        ]

    def test_defaults_to_all_clusters(self):
        self.assertEqual(select_clusters(self.clusters), self.clusters)

    def test_ids_and_regexes_are_combined_as_union(self):
        selected = select_clusters(
            self.clusters,
            cluster_ids=[2, 2],
            name_regexes=["programming"],
        )
        self.assertEqual([item.cluster_id for item in selected], [1, 2, 3])

    def test_regex_matching_is_case_insensitive_and_partial(self):
        selected = select_clusters(self.clusters, name_regexes=["DATA"])
        self.assertEqual([item.cluster_id for item in selected], [2])

    def test_rejects_unknown_ids_invalid_regex_and_empty_selection(self):
        with self.assertRaisesRegex(ValueError, "Unknown cluster IDs"):
            select_clusters(self.clusters, cluster_ids=[99])
        with self.assertRaisesRegex(ValueError, "Invalid cluster name regex"):
            select_clusters(self.clusters, name_regexes=["["])
        with self.assertRaisesRegex(ValueError, "matched no clusters"):
            select_clusters(self.clusters, name_regexes=["does-not-exist"])


class TestRestructuringCli(unittest.TestCase):
    def test_configuration_uses_environment_and_cli_precedence(self):
        environment = {
            "OPENAI_BASE_URL": "https://environment.test/v1",
            "OPENAI_MODEL": "environment-model",
            "RESTRUCTURING_TEMPERATURE": "0.25",
            "RESTRUCTURING_MAX_RETRIES": "9",
        }
        with patch.dict("os.environ", environment, clear=False):
            environmental = build_parser().parse_args(["clusters.yml"])
            overridden = build_parser().parse_args([
                "clusters.yml",
                "--endpoint",
                "https://cli.test/v1",
                "--model",
                "cli-model",
                "--max-retries",
                "2",
            ])
        self.assertEqual(environmental.endpoint, "https://environment.test/v1")
        self.assertEqual(environmental.model, "environment-model")
        self.assertEqual(environmental.temperature, 0.25)
        self.assertEqual(environmental.max_retries, 9)
        self.assertEqual(overridden.endpoint, "https://cli.test/v1")
        self.assertEqual(overridden.model, "cli-model")
        self.assertEqual(overridden.max_retries, 2)

    def test_syllabus_sections_can_be_selected(self):
        parsed = build_parser().parse_args([
            "clusters.yml",
            "--syllabus-sections",
            "title",
            "bib",
            "office_hours",
        ])
        self.assertEqual(parsed.syllabus_sections, ["title", "bib", "office_hours"])


class TestInputAndCache(unittest.TestCase):
    def test_extracts_english_first_with_per_section_italian_fallback(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            course_path = root / "course-A.yml"
            course_path.write_text(
                yaml.safe_dump(
                    {
                        "course_title": {"id": "A", "name": "Do not trust this"},
                        "syllabus": {
                            "en": {"contents": {"Course contents": "English contents"}},
                            "it": {
                                "contents": {
                                    "Contenuti": "Contenuti italiani",
                                    "Conoscenze e abilità da conseguire": "Esiti italiani",
                                }
                            },
                        },
                    },
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )
            input_path = root / "clusters.yml"
            input_path.write_text(
                yaml.safe_dump({
                    "Cluster (4)": {
                        "index": 4,
                        "courses": [{"id": "A", "name": "Fallback", "path": str(course_path)}],
                    }
                }),
                encoding="utf-8",
            )
            loaded = load_clusters(input_path)[0].courses[0]
        self.assertEqual(loaded.course_contents, "English contents")
        self.assertEqual(loaded.course_contents_language, "en")
        self.assertEqual(loaded.learning_outcomes, "Esiti italiani")
        self.assertEqual(loaded.learning_outcomes_language, "it")
        self.assertEqual(
            loaded.syllabus_sections,
            (("Conoscenze e abilità da conseguire", "Esiti italiani"), ("Course contents", "English contents")),
        )

    def test_loads_selected_sections_into_markdown_order(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            course_path = root / "course-A.yml"
            course_path.write_text(
                yaml.safe_dump(
                    {
                        "course_title": {"id": "A", "name": "Reusable title"},
                        "syllabus": {
                            "en": {
                                "contents": {
                                    "Learning outcomes": "English outcomes",
                                    "Course contents": "English contents",
                                    "Readings/Bibliography": "English bibliography",
                                }
                            },
                            "it": {
                                "contents": {
                                    "Orario di ricevimento": "Ricevimento italiano",
                                }
                            },
                        },
                    },
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )
            input_path = root / "clusters.yml"
            input_path.write_text(
                yaml.safe_dump(
                    {
                        "Cluster (4)": {
                            "index": 4,
                            "courses": [{"id": "A", "name": "Fallback", "path": str(course_path)}],
                        }
                    }
                ),
                encoding="utf-8",
            )
            loaded = load_clusters(
                input_path,
                syllabus_section_keys=("title", "outcomes", "contents", "bib", "office_hours"),
            )[0].courses[0]
        self.assertEqual(
            loaded.syllabus_sections,
            (
                ("Learning outcomes", "English outcomes"),
                ("Course contents", "English contents"),
                ("Readings/Bibliography", "English bibliography"),
                ("Orario di ricevimento", "Ricevimento italiano"),
            ),
        )

    def test_cache_key_is_stable_and_sensitive_to_configuration(self):
        item = cluster(7, "Cluster (7)", course("B"), course("A"))
        config = ModelConfig(endpoint="https://example.test/v1", model="model")
        first, metadata = conversation_cache_key(item, config)
        second, _ = conversation_cache_key(item, config)
        changed, _ = conversation_cache_key(
            item,
            ModelConfig(endpoint="https://other.test/v1", model="model"),
        )
        self.assertEqual(first, second)
        self.assertNotEqual(first, changed)
        self.assertEqual(metadata["course_ids"], ["A", "B"])
        self.assertEqual(metadata["syllabus_sections"], list(DEFAULT_SYLLABUS_SECTION_KEYS))
        self.assertEqual(metadata["prompt_version"], 2)


class TestWorkflow(unittest.TestCase):
    def setUp(self):
        self.cluster = cluster(5, "Example (5)", course("A"), course("B", "Beta material"))
        self.config = ModelConfig(endpoint="https://example.test/v1", model="test-model")
        self.retry = RetryConfig(max_retries=0)
        self.first = response_for_course(
            Topic(key="alpha", description="Alpha from evidence."),
            covered=["alpha"],
        )
        self.second = response_for_course(
            Topic(key="alpha", description="Alpha from evidence."),
            Topic(key="beta", description="Beta from evidence."),
            covered=["beta"],
        )

    def test_processes_sequentially_and_reuses_complete_cache(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_dir = pathlib.Path(tmp_dir)
            client = FakeClient([self.first, self.second, final_response()])
            result = process_cluster(self.cluster, client, self.config, self.retry, cache_dir)
            self.assertEqual(result.plantuml, final_response().plantuml)
            self.assertEqual(len(client.completions.calls), 3)
            second_prompt = client.completions.calls[1]["messages"][-1]["content"]
            self.assertIn("# Misleading title B", second_prompt)
            self.assertIn("## Learning outcomes", second_prompt)
            self.assertIn("## Course contents", second_prompt)
            self.assertIn("<syllabus_markdown>", second_prompt)
            cached_files = list(cache_dir.glob("*.yml"))
            self.assertEqual(len(cached_files), 1)
            payload = yaml.safe_load(cached_files[0].read_text(encoding="utf-8"))
            self.assertEqual(len(payload["messages"]), 7)

            no_calls = FakeClient([])
            cached_result = process_cluster(self.cluster, no_calls, self.config, self.retry, cache_dir)
            self.assertEqual(cached_result.plantuml, result.plantuml)
            self.assertEqual(no_calls.completions.calls, [])

            refreshed = FakeClient([self.first, self.second, final_response()])
            process_cluster(
                self.cluster,
                refreshed,
                self.config,
                self.retry,
                cache_dir,
                refresh_cache=True,
            )
            self.assertEqual(len(refreshed.completions.calls), 3)

    def test_prompts_are_loaded_from_external_text_files(self):
        self.assertEqual(
            {path.name for path in PROMPTS_DIR.glob("*.txt")},
            {"system.txt", "course.txt", "final.txt"},
        )
        self.assertIn(
            "Never infer a topic from a course title",
            (PROMPTS_DIR / "system.txt").read_text(encoding="utf-8"),
        )

    def test_changed_course_prompt_reuses_prefix_and_replaces_suffix(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_dir = pathlib.Path(tmp_dir)
            process_cluster(
                self.cluster,
                FakeClient([self.first, self.second, final_response()]),
                self.config,
                self.retry,
                cache_dir,
            )
            changed = cluster(5, "Example (5)", course("A"), course("B", "Changed beta evidence"))
            client = FakeClient([self.second, final_response()])
            process_cluster(changed, client, self.config, self.retry, cache_dir)
            self.assertEqual(len(client.completions.calls), 2)

    def test_writes_timestamped_yaml_and_plantuml_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            input_path = root / "clusters.yml"
            course_paths = []
            for item in self.cluster.courses:
                path = root / f"course-{item.course_id}.yml"
                path.write_text(
                    yaml.safe_dump({
                        "course_title": {"id": item.course_id, "name": item.title},
                        "syllabus": {
                            "en": {
                                "contents": {
                                    "Course contents": item.course_contents,
                                    "Learning outcomes": item.learning_outcomes,
                                }
                            }
                        },
                    }),
                    encoding="utf-8",
                )
                course_paths.append(path)
            input_path.write_text(
                yaml.safe_dump({
                    self.cluster.name: {
                        "index": self.cluster.cluster_id,
                        "courses": [
                            {"id": item.course_id, "name": item.title, "path": str(path)}
                            for item, path in zip(self.cluster.courses, course_paths)
                        ],
                    }
                }),
                encoding="utf-8",
            )
            output_dir = run_restructuring(
                input_path,
                self.config,
                self.retry,
                client=FakeClient([self.first, self.second, final_response()]),
                cache_dir=root / "cache",
                output_root=root / "output",
                now=datetime(2026, 7, 29, 12, 34),
                plantuml_renderer=FakePlantUMLRenderer(),
            )
            self.assertEqual(output_dir.name, "attempt-2026-07-29-12-34")
            self.assertTrue((output_dir / "topics-of-cluster-5.yml").exists())
            self.assertTrue((output_dir / "topics-of-course-A.yml").exists())
            self.assertTrue((output_dir / "topics-of-course-B.yml").exists())
            plantuml = (output_dir / "restructure-proposal-for-cluster-5.puml").read_text()
            self.assertTrue(plantuml.startswith("@startuml"))
            svg = output_dir / "restructure-proposal-for-cluster-5.svg"
            self.assertEqual(svg.read_text(encoding="utf-8"), "<svg></svg>")
            cluster_payload = yaml.safe_load((output_dir / "topics-of-cluster-5.yml").read_text())
            self.assertEqual(list(cluster_payload["topics"]), ["alpha", "beta"])

    def test_plantuml_rendering_retries_and_rejects_error_svg(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            destination = pathlib.Path(tmp_dir) / "proposal.svg"
            renderer = FakePlantUMLRenderer(failures=2)
            sleeps: list[float] = []
            render_plantuml_svg(
                "@startuml\nclass A\n@enduml",
                destination,
                RetryConfig(max_retries=2, initial_backoff=1, max_backoff=60),
                renderer=renderer,
                sleep=sleeps.append,
                random_uniform=lambda _minimum, maximum: maximum,
            )
            self.assertEqual(len(renderer.calls), 3)
            self.assertEqual(sleeps, [1, 2])
            self.assertTrue(destination.exists())

            with self.assertRaisesRegex(ValueError, "syntax-error SVG"):
                render_plantuml_svg(
                    "@startuml\nbad\n@enduml",
                    destination,
                    RetryConfig(max_retries=0),
                    renderer=FakePlantUMLRenderer(
                        svg="<svg><text>Syntax Error? (Assumed diagram type: class)</text></svg>"
                    ),
                    sleep=lambda _: self.fail("must not sleep"),
                )

    def test_retry_uses_exponential_backoff_and_skips_permanent_errors(self):
        class RateLimitError(Exception):
            pass

        attempts = 0
        sleeps: list[float] = []

        def operation():
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RateLimitError()
            return "ok"

        result = call_with_backoff(
            operation,
            RetryConfig(max_retries=3, initial_backoff=1, max_backoff=60),
            sleep=sleeps.append,
            random_uniform=lambda _minimum, maximum: maximum,
        )
        self.assertEqual(result, "ok")
        self.assertEqual(sleeps, [1, 2])
        with self.assertRaises(ValueError):
            call_with_backoff(
                lambda: (_ for _ in ()).throw(ValueError("permanent")),
                RetryConfig(max_retries=3),
                sleep=lambda _: self.fail("must not sleep"),
            )


class TestRepositoryRestructuringInput(unittest.TestCase):
    def test_real_input_discovers_30_clusters_and_293_courses(self):
        path = pathlib.Path(
            "data/clusters/runs/2025-20260715-120359-spectral/cluster_courses.yml"
        )
        if not path.exists():
            self.skipTest("Repository sample data is not available")
        clusters = load_clusters(path)
        self.assertEqual(len(clusters), 30)
        self.assertEqual(sum(len(item.courses) for item in clusters), 293)


if __name__ == "__main__":
    unittest.main()

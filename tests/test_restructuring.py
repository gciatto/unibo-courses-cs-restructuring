from __future__ import annotations

import json
import pathlib
import re
import tempfile
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import yaml
from pydantic import ValidationError

from restructuring.cli import build_parser
from restructuring.io import (
    DEFAULT_SYLLABUS_SECTION_KEYS,
    PROMPT_VERSION,
    conversation_cache_key,
    load_clusters,
    select_clusters,
)
from restructuring.models import (
    ClusterInput,
    CourseInput,
    CourseTopicMembership,
    CourseTopicsResponse,
    ModelConfig,
    PlantUMLResponse,
    RetryConfig,
    Topic,
    TopicDiff,
)
from restructuring.workflow import (
    PROMPTS_DIR,
    SYSTEM_PROMPT,
    ClusterTopicState,
    PlantUMLRenderError,
    apply_topic_response,
    call_with_backoff,
    process_cluster_topics,
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


def topic_response(
    covered: list[str],
    *,
    remove: list[str] | None = None,
    upsert: list[Topic] | None = None,
    updates: list[CourseTopicMembership] | None = None,
) -> CourseTopicsResponse:
    diffs = []
    if remove or upsert or updates:
        diffs.append(
            TopicDiff(
                remove_topic_keys=remove or [],
                upsert_topics=upsert or [],
                course_topic_updates=updates or [],
            )
        )
    return CourseTopicsResponse(covered_topic_keys=covered, topic_diffs=diffs)


def valid_plantuml(*course_ids: str) -> str:
    old_classes = "\n".join(
        f'class "{course_id} - Old {course_id}" as OLD_{course_id} #Transparent'
        for course_id in course_ids
    )
    links = "\n".join(
        f"OLD_{course_id} ..> Foundations : subsumed by"
        for course_id in course_ids
    )
    return f"""@startuml
{old_classes}
class Foundations {{
  alpha
}}
note right of Foundations
alpha: Alpha supported by the syllabi.
end note
{links}
@enduml"""


class FakeCompletions:
    def __init__(self, responses: list[object], on_parse=None):
        self.responses = list(responses)
        self.calls: list[dict] = []
        self.on_parse = on_parse

    def parse(self, **arguments):
        self.calls.append(arguments)
        if self.on_parse is not None:
            self.on_parse(arguments)
        if not self.responses:
            raise AssertionError("Unexpected API call")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        message = SimpleNamespace(
            parsed=response,
            content=response.model_dump_json(),
            refusal=None,
        )
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])


class FakeClient:
    def __init__(self, responses: list[object], on_parse=None):
        self.completions = FakeCompletions(responses, on_parse)
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


def write_cluster_input(root: pathlib.Path, item: ClusterInput) -> pathlib.Path:
    raw_courses = []
    for current in item.courses:
        path = root / f"course-{current.course_id}.yml"
        path.write_text(
            yaml.safe_dump(
                {
                    "course_title": {
                        "id": current.course_id,
                        "name": current.title,
                    },
                    "syllabus": {
                        "en": {
                            "contents": {
                                "Course contents": current.course_contents,
                                "Learning outcomes": current.learning_outcomes,
                            }
                        }
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        raw_courses.append(
            {
                "id": current.course_id,
                "name": current.title,
                "path": str(path),
            }
        )
    input_path = root / "clusters.yml"
    input_path.write_text(
        yaml.safe_dump(
            {
                item.name: {
                    "index": item.cluster_id,
                    "courses": raw_courses,
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return input_path


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
    def test_configuration_and_conversation_mode(self):
        environment = {
            "OPENAI_BASE_URL": "https://environment.test/v1",
            "OPENAI_MODEL": "environment-model",
            "RESTRUCTURING_TEMPERATURE": "0.25",
            "RESTRUCTURING_MAX_RETRIES": "9",
        }
        with patch.dict("os.environ", environment, clear=False):
            environmental = build_parser().parse_args(["clusters.yml"])
            overridden = build_parser().parse_args(
                [
                    "clusters.yml",
                    "--endpoint",
                    "https://cli.test/v1",
                    "--topic-conversation-mode",
                    "full",
                ]
            )
        self.assertEqual(environmental.endpoint, "https://environment.test/v1")
        self.assertEqual(environmental.model, "environment-model")
        self.assertEqual(environmental.temperature, 0.25)
        self.assertEqual(environmental.topic_conversation_mode, "stateless")
        self.assertEqual(overridden.endpoint, "https://cli.test/v1")
        self.assertEqual(overridden.topic_conversation_mode, "full")

    def test_syllabus_sections_can_be_selected(self):
        parsed = build_parser().parse_args(
            [
                "clusters.yml",
                "--syllabus-sections",
                "title",
                "bib",
                "office_hours",
            ]
        )
        self.assertEqual(
            parsed.syllabus_sections,
            ["title", "bib", "office_hours"],
        )


class TestInputAndCache(unittest.TestCase):
    def test_extracts_selected_markdown_sections_with_english_fallback(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            course_path = root / "course-A.yml"
            course_path.write_text(
                yaml.safe_dump(
                    {
                        "course_title": {"id": "A", "name": "Do not trust this"},
                        "syllabus": {
                            "en": {
                                "contents": {
                                    "Course contents": "English contents",
                                    "Readings/Bibliography": "English readings",
                                }
                            },
                            "it": {
                                "contents": {
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
                yaml.safe_dump(
                    {
                        "Cluster (4)": {
                            "index": 4,
                            "courses": [
                                {
                                    "id": "A",
                                    "name": "Fallback",
                                    "path": str(course_path),
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )
            loaded = load_clusters(
                input_path,
                ("title", "outcomes", "contents", "bib"),
            )[0].courses[0]
        self.assertEqual(
            loaded.syllabus_sections,
            (
                ("Conoscenze e abilità da conseguire", "Esiti italiani"),
                ("Course contents", "English contents"),
                ("Readings/Bibliography", "English readings"),
            ),
        )

    def test_cache_key_includes_prompt_sections_and_conversation_mode(self):
        item = cluster(7, "Cluster (7)", course("B"), course("A"))
        config = ModelConfig(endpoint="https://example.test/v1", model="model")
        stateless, metadata = conversation_cache_key(item, config)
        full, _ = conversation_cache_key(
            item,
            config,
            topic_conversation_mode="full",
        )
        self.assertNotEqual(stateless, full)
        self.assertEqual(metadata["course_ids"], ["A", "B"])
        self.assertEqual(
            metadata["syllabus_sections"],
            list(DEFAULT_SYLLABUS_SECTION_KEYS),
        )
        self.assertEqual(metadata["topic_conversation_mode"], "stateless")
        self.assertEqual(metadata["prompt_version"], PROMPT_VERSION)
        self.assertEqual(PROMPT_VERSION, 4)


class TestIncrementalTopicState(unittest.TestCase):
    def setUp(self):
        self.cluster = cluster(
            5,
            "Example (5)",
            course("A"),
            course("B", "Beta material"),
            course("C", "Gamma material"),
        )

    def test_applies_add_update_split_merge_rename_and_delete_diffs(self):
        state, rewritten, changed = apply_topic_response(
            self.cluster,
            0,
            ClusterTopicState({}, {}),
            topic_response(
                ["alpha", "obsolete"],
                upsert=[
                    Topic(key="alpha", description="Initial alpha"),
                    Topic(key="obsolete", description="Remove later"),
                ],
            ),
        )
        self.assertEqual(rewritten, {"A"})
        self.assertEqual(changed, {"alpha", "obsolete"})

        state, rewritten, _ = apply_topic_response(
            self.cluster,
            1,
            state,
            topic_response(
                ["gamma"],
                remove=["obsolete"],
                upsert=[
                    Topic(key="alpha", description="Refined alpha"),
                    Topic(key="beta", description="Split beta"),
                    Topic(key="gamma", description="Current gamma"),
                ],
                updates=[
                    CourseTopicMembership(
                        course_id="A",
                        topic_keys=["alpha", "beta"],
                    )
                ],
            ),
        )
        self.assertEqual(rewritten, {"A", "B"})
        self.assertEqual(state.memberships["A"], ["alpha", "beta"])
        self.assertNotIn("obsolete", state.topics)

        state, rewritten, _ = apply_topic_response(
            self.cluster,
            2,
            state,
            topic_response(
                ["foundations"],
                remove=["alpha", "beta"],
                upsert=[
                    Topic(
                        key="foundations",
                        description="Merged and renamed foundations",
                    )
                ],
                updates=[
                    CourseTopicMembership(
                        course_id="A",
                        topic_keys=["foundations"],
                    )
                ],
            ),
        )
        self.assertEqual(state.memberships["A"], ["foundations"])
        self.assertEqual(state.memberships["C"], ["foundations"])
        self.assertEqual(rewritten, {"A", "C"})

    def test_normalizes_empty_topic_diff_to_no_changes(self):
        response = CourseTopicsResponse.model_validate(
            {
                "covered_topic_keys": ["alpha"],
                "topic_diffs": [
                    {
                        "remove_topic_keys": [],
                        "upsert_topics": [],
                        "course_topic_updates": [],
                    }
                ],
            }
        )

        self.assertEqual(response.topic_diffs, [])
        state, rewritten, changed = apply_topic_response(
            self.cluster,
            1,
            ClusterTopicState({"alpha": "Alpha"}, {"A": ["alpha"]}),
            response,
        )
        self.assertEqual(state.topics, {"alpha": "Alpha"})
        self.assertEqual(state.memberships["B"], ["alpha"])
        self.assertEqual(rewritten, {"B"})
        self.assertEqual(changed, set())

    def test_rejects_dangling_future_and_conflicting_updates(self):
        state = ClusterTopicState(
            {"alpha": "Alpha"},
            {"A": ["alpha"]},
        )
        with self.assertRaisesRegex(ValueError, "explicit replacement"):
            apply_topic_response(
                self.cluster,
                1,
                state,
                topic_response(
                    [],
                    remove=["alpha"],
                    upsert=[Topic(key="beta", description="Beta")],
                ),
            )
        with self.assertRaisesRegex(ValueError, "previously processed"):
            apply_topic_response(
                self.cluster,
                1,
                state,
                topic_response(
                    ["alpha"],
                    updates=[
                        CourseTopicMembership(
                            course_id="C",
                            topic_keys=["alpha"],
                        )
                    ],
                ),
            )
        response = CourseTopicsResponse(
            covered_topic_keys=["alpha"],
            topic_diffs=[
                TopicDiff(
                    upsert_topics=[Topic(key="beta", description="First")]
                ),
                TopicDiff(
                    upsert_topics=[Topic(key="beta", description="Second")]
                ),
            ],
        )
        with self.assertRaisesRegex(ValueError, "conflicts"):
            apply_topic_response(self.cluster, 1, state, response)
        with self.assertRaises(ValidationError):
            CourseTopicsResponse(covered_topic_keys=["Not-Snake"], topic_diffs=[])


class TestWorkflow(unittest.TestCase):
    def setUp(self):
        self.cluster = cluster(
            5,
            "Example (5)",
            course("A"),
            course("B", "Beta material"),
        )
        self.config = ModelConfig(
            endpoint="https://example.test/v1",
            model="test-model",
        )
        self.retry = RetryConfig(max_retries=0)
        self.first = topic_response(
            ["alpha"],
            upsert=[Topic(key="alpha", description="Alpha from evidence.")],
        )
        self.second = topic_response(
            ["beta"],
            upsert=[Topic(key="beta", description="Beta from evidence.")],
        )

    def test_prompts_separate_topic_and_plantuml_instructions(self):
        self.assertEqual(
            {path.name for path in PROMPTS_DIR.glob("*.txt")},
            {
                "system.txt",
                "course.txt",
                "plantuml.txt",
                "plantuml_repair.txt",
            },
        )
        self.assertNotIn("PlantUML", SYSTEM_PROMPT)
        self.assertIn(
            "Return complete PlantUML",
            (PROMPTS_DIR / "plantuml.txt").read_text(encoding="utf-8"),
        )

    def test_stateless_and_full_modes_control_request_history(self):
        for mode, expected_message_counts in (
            ("stateless", [2, 2]),
            ("full", [2, 4]),
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp_dir:
                root = pathlib.Path(tmp_dir)
                client = FakeClient([self.first, self.second])
                process_cluster_topics(
                    self.cluster,
                    client,
                    self.config,
                    self.retry,
                    root / "cache",
                    root / "output",
                    topic_conversation_mode=mode,
                )
                self.assertEqual(
                    [
                        len(call["messages"])
                        for call in client.completions.calls
                    ],
                    expected_message_counts,
                )
                second_prompt = client.completions.calls[1]["messages"][-1]["content"]
                self.assertIn('"alpha": "Alpha from evidence."', second_prompt)
                self.assertIn("# Misleading title B", second_prompt)
                self.assertIn("<syllabus_markdown>", second_prompt)

    def test_incremental_artifacts_are_rebuilt_from_complete_cache(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            cache_dir = root / "cache"
            first_output = root / "first"
            second_refines_description = topic_response(
                ["beta"],
                upsert=[
                    Topic(
                        key="alpha",
                        description="Alpha refined by later evidence.",
                    ),
                    Topic(key="beta", description="Beta from evidence."),
                ],
            )
            process_cluster_topics(
                self.cluster,
                FakeClient([self.first, second_refines_description]),
                self.config,
                self.retry,
                cache_dir,
                first_output,
            )
            payload = yaml.safe_load(
                (first_output / "topics-of-cluster-5.yml").read_text()
            )
            self.assertEqual(list(payload["topics"]), ["alpha", "beta"])
            course_a = yaml.safe_load(
                (first_output / "topics-of-course-A.yml").read_text()
            )
            self.assertEqual(
                course_a["topics"]["alpha"],
                "Alpha refined by later evidence.",
            )

            second_output = root / "second"
            no_calls = FakeClient([])
            process_cluster_topics(
                self.cluster,
                no_calls,
                self.config,
                self.retry,
                cache_dir,
                second_output,
            )
            self.assertEqual(no_calls.completions.calls, [])
            self.assertEqual(
                (first_output / "topics-of-cluster-5.yml").read_text(),
                (second_output / "topics-of-cluster-5.yml").read_text(),
            )
            self.assertTrue((second_output / "topics-of-course-A.yml").exists())
            self.assertTrue((second_output / "topics-of-course-B.yml").exists())

    def test_topics_exist_before_plantuml_and_success_writes_svg(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            input_path = write_cluster_input(root, self.cluster)
            output_root = root / "output"

            def check_before_plantuml(arguments):
                if arguments["response_format"] is PlantUMLResponse:
                    attempt = output_root / "attempt-2026-07-29-12-34"
                    self.assertTrue(
                        (attempt / "topics-of-cluster-5.yml").exists()
                    )
                    self.assertTrue(
                        (attempt / "topics-of-course-A.yml").exists()
                    )
                    self.assertTrue(
                        (attempt / "topics-of-course-B.yml").exists()
                    )

            output_dir = run_restructuring(
                input_path,
                self.config,
                self.retry,
                client=FakeClient(
                    [
                        self.first,
                        self.second,
                        PlantUMLResponse(
                            plantuml=valid_plantuml("A", "B")
                        ),
                    ],
                    on_parse=check_before_plantuml,
                ),
                cache_dir=root / "cache",
                output_root=output_root,
                now=datetime(2026, 7, 29, 12, 34),
                plantuml_renderer=FakePlantUMLRenderer(),
            )
            puml = output_dir / "restructure-proposal-for-cluster-5.puml"
            self.assertTrue(puml.read_text().startswith("@startuml"))
            self.assertEqual(
                puml.with_suffix(".svg").read_text(encoding="utf-8"),
                "<svg></svg>",
            )

    def test_syntax_failure_regenerates_and_overwrites_puml(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            input_path = write_cluster_input(root, self.cluster)
            repaired = valid_plantuml("A", "B")
            output_dir = run_restructuring(
                input_path,
                self.config,
                RetryConfig(max_retries=1, initial_backoff=0),
                client=FakeClient(
                    [
                        self.first,
                        self.second,
                        PlantUMLResponse(
                            plantuml="@startuml\nclass Broken\n@enduml"
                        ),
                        PlantUMLResponse(plantuml=repaired),
                    ]
                ),
                cache_dir=root / "cache",
                output_root=root / "output",
                now=datetime(2026, 7, 29, 12, 34),
                plantuml_renderer=FakePlantUMLRenderer(),
            )
            puml = output_dir / "restructure-proposal-for-cluster-5.puml"
            self.assertEqual(puml.read_text().strip(), repaired)
            self.assertTrue(puml.with_suffix(".svg").exists())

    def test_render_failure_is_nonfatal_and_preserves_puml(self):
        class FailingRenderer(FakePlantUMLRenderer):
            def dump(self, path: str, output_format: str, code: str) -> None:
                self.calls.append((path, output_format, code))
                raise TimeoutError("remote service unavailable")

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            input_path = write_cluster_input(root, self.cluster)
            output_dir = run_restructuring(
                input_path,
                self.config,
                self.retry,
                client=FakeClient(
                    [
                        self.first,
                        self.second,
                        PlantUMLResponse(
                            plantuml=valid_plantuml("A", "B")
                        ),
                    ]
                ),
                cache_dir=root / "cache",
                output_root=root / "output",
                now=datetime(2026, 7, 29, 12, 34),
                plantuml_renderer=FailingRenderer(),
            )
            puml = output_dir / "restructure-proposal-for-cluster-5.puml"
            self.assertTrue(puml.exists())
            self.assertFalse(puml.with_suffix(".svg").exists())
            self.assertTrue((output_dir / "topics-of-cluster-5.yml").exists())

    def test_plantuml_llm_failure_is_nonfatal_after_topic_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            input_path = write_cluster_input(root, self.cluster)
            output_dir = run_restructuring(
                input_path,
                self.config,
                self.retry,
                client=FakeClient(
                    [
                        self.first,
                        self.second,
                        RuntimeError("diagram model unavailable"),
                    ]
                ),
                cache_dir=root / "cache",
                output_root=root / "output",
                now=datetime(2026, 7, 29, 12, 34),
                plantuml_renderer=FakePlantUMLRenderer(),
            )
            self.assertTrue((output_dir / "topics-of-cluster-5.yml").exists())
            self.assertTrue((output_dir / "topics-of-course-A.yml").exists())
            self.assertTrue((output_dir / "topics-of-course-B.yml").exists())
            self.assertFalse(
                (
                    output_dir
                    / "restructure-proposal-for-cluster-5.puml"
                ).exists()
            )

    def test_plantuml_rendering_retries_network_but_not_syntax(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            destination = pathlib.Path(tmp_dir) / "proposal.svg"
            renderer = FakePlantUMLRenderer(failures=2)
            sleeps: list[float] = []
            render_plantuml_svg(
                "@startuml\nclass A\n@enduml",
                destination,
                RetryConfig(max_retries=2, initial_backoff=1),
                renderer=renderer,
                sleep=sleeps.append,
                random_uniform=lambda _minimum, maximum: maximum,
            )
            self.assertEqual(len(renderer.calls), 3)
            self.assertEqual(sleeps, [1, 2])
            with self.assertRaisesRegex(ValueError, "syntax-error SVG"):
                render_plantuml_svg(
                    "@startuml\nbad\n@enduml",
                    destination,
                    RetryConfig(max_retries=3),
                    renderer=FakePlantUMLRenderer(
                        svg="<svg><text>Syntax Error?</text></svg>"
                    ),
                    sleep=lambda _: self.fail("syntax must not be retried"),
                )

    def test_generic_retry_skips_permanent_errors(self):
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
            RetryConfig(max_retries=3, initial_backoff=1),
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
    def test_real_input_processes_30_clusters_and_293_courses_with_fake_model(self):
        path = pathlib.Path(
            "data/clusters/runs/2025-20260715-120359-spectral/cluster_courses.yml"
        )
        if not path.exists():
            self.skipTest("Repository sample data is not available")
        clusters = load_clusters(path)
        self.assertEqual(len(clusters), 30)
        self.assertEqual(sum(len(item.courses) for item in clusters), 293)

        class DeterministicCompletions:
            def __init__(self):
                self.calls = 0

            def parse(self, **arguments):
                self.calls += 1
                prompt = arguments["messages"][-1]["content"]
                if arguments["response_format"] is CourseTopicsResponse:
                    current = re.search(
                        r"<current_topics>\s*(.*?)\s*</current_topics>",
                        prompt,
                        re.DOTALL,
                    )
                    assert current is not None
                    topics = json.loads(current.group(1))
                    response = topic_response(
                        ["cluster_topic"],
                        upsert=(
                            [
                                Topic(
                                    key="cluster_topic",
                                    description="Deterministic syllabus topic.",
                                )
                            ]
                            if not topics
                            else []
                        ),
                    )
                else:
                    labels_match = re.search(
                        r"<old_course_labels>\s*(.*?)\s*</old_course_labels>",
                        prompt,
                        re.DOTALL,
                    )
                    assert labels_match is not None
                    labels = json.loads(labels_match.group(1))
                    declarations = "\n".join(
                        f'class "{item["id"]} - {item["title"].replace(chr(34), chr(39))}" '
                        f"as OLD_{index} #Transparent"
                        for index, item in enumerate(labels)
                    )
                    links = "\n".join(
                        f"OLD_{index} ..> Proposed : subsumed by"
                        for index in range(len(labels))
                    )
                    response = PlantUMLResponse(
                        plantuml=f"""@startuml
{declarations}
class Proposed {{
  cluster_topic
}}
note right of Proposed
cluster_topic: Deterministic syllabus topic.
end note
{links}
@enduml"""
                    )
                message = SimpleNamespace(
                    parsed=response,
                    content=response.model_dump_json(),
                    refusal=None,
                )
                return SimpleNamespace(
                    choices=[SimpleNamespace(message=message)]
                )

        completions = DeterministicCompletions()
        fake_client = SimpleNamespace(
            chat=SimpleNamespace(completions=completions)
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            output = run_restructuring(
                path,
                ModelConfig(
                    endpoint="https://example.test/v1",
                    model="deterministic",
                ),
                RetryConfig(max_retries=0),
                client=fake_client,
                plantuml_renderer=FakePlantUMLRenderer(),
                cache_dir=root / "cache",
                output_root=root / "output",
                now=datetime(2026, 7, 29, 16, 45),
            )
            self.assertEqual(
                len(list(output.glob("topics-of-cluster-*.yml"))),
                30,
            )
            self.assertEqual(
                len(list(output.glob("topics-of-course-*.yml"))),
                293,
            )
            self.assertEqual(
                len(list(output.glob("restructure-proposal-*.puml"))),
                30,
            )
            self.assertEqual(
                len(list(output.glob("restructure-proposal-*.svg"))),
                30,
            )
        self.assertEqual(completions.calls, 293 + 30)


if __name__ == "__main__":
    unittest.main()

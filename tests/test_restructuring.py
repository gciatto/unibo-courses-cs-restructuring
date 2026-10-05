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

from restructuring.cli import _validate_args, build_parser
from restructuring.io import (
    DEFAULT_SYLLABUS_SECTION_KEYS,
    PROMPT_VERSION,
    conversation_cache_key,
    derive_source_course_mappings,
    estimate_topic_weights,
    load_global_corpus,
    load_clusters,
    select_clusters,
    validate_restructuring_proposal,
)
from restructuring.models import (
    ClusterInput,
    CourseAssembly,
    CoursePrerequisite,
    CourseInput,
    CourseTopicMembership,
    CourseTopicsResponse,
    ModelConfig,
    ProposedCourse,
    ProposedTopic,
    RestructuringProposal,
    RetryConfig,
    Topic,
    TopicDiff,
    TopicPartition,
)
from restructuring.workflow import (
    PROMPTS_DIR,
    SYSTEM_PROMPT,
    ClusterTopicState,
    apply_topic_response,
    call_with_backoff,
    generate_cluster_proposal,
    generate_global_proposal,
    process_cluster_topics,
    run_restructuring,
    topic_batches,
    validate_topic_partition,
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


def valid_proposal() -> RestructuringProposal:
    return RestructuringProposal(
        proposed_topics=[
            ProposedTopic(
                key="foundations",
                description="Combined foundations.",
                source_topic_keys=["alpha", "beta"],
            )
        ],
        proposed_courses=[
            ProposedCourse(
                key="foundations_course",
                title="Foundations",
                ects=6,
                topic_keys=["foundations"],
            )
        ],
        prerequisites=[],
    )


def module_partition(*groups: tuple[str, list[str]]) -> TopicPartition:
    return TopicPartition(
        proposed_topics=[
            ProposedTopic(key=key, description=f"Module {key}.", source_topic_keys=keys)
            for key, keys in groups
        ]
    )


def course_assembly(*courses: tuple[str, list[str]]) -> CourseAssembly:
    return CourseAssembly(
        proposed_courses=[
            ProposedCourse(key=key, title=key.title(), ects=6, topic_keys=keys)
            for key, keys in courses
        ],
        prerequisites=[],
    )


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

        resumed = build_parser().parse_args(
            [
                "clusters.yml",
                "--reuse-topics-from",
                "attempt-one",
                "--reuse-topics-from",
                "attempt-two",
            ]
        )
        self.assertEqual(
            resumed.reuse_topics_from,
            [pathlib.Path("attempt-one"), pathlib.Path("attempt-two")],
        )

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
        self.assertEqual(PROMPT_VERSION, 6)

    def test_global_corpus_deduplicates_and_rejects_conflicts(self):
        shared = course("A")
        global_input = load_global_corpus([
            cluster(2, "Second", course("B"), shared),
            cluster(1, "First", CourseInput(**{**shared.__dict__, "path": "elsewhere.yml"})),
        ])
        self.assertEqual([item.course_id for item in global_input.courses], ["A", "B"])
        self.assertEqual(global_input.source_clusters["A"], ((1, "First"), (2, "Second")))
        with self.assertRaisesRegex(ValueError, "Conflicting course data"):
            load_global_corpus([cluster(1, "One", course("A")), cluster(2, "Two", course("A", "different"))])

    def test_global_cache_identity_is_distinct(self):
        item = cluster(1, "One", course("A"))
        config = ModelConfig(endpoint="https://example.test/v1", model="model")
        cluster_key, _ = conversation_cache_key(item, config)
        global_key, metadata = conversation_cache_key(load_global_corpus([item]), config)
        self.assertNotEqual(cluster_key, global_key)
        self.assertEqual(metadata["analysis"], {"mode": "all-courses"})


class TestTopicWeights(unittest.TestCase):
    def test_topic_weight_is_median_of_even_credit_shares(self):
        courses = [
            CourseInput(**{**course(course_id).__dict__, "credits": credits})
            for course_id, credits in (("A", 6.0), ("B", 12.0), ("C", 3.0), ("D", None))
        ]
        weights = estimate_topic_weights(courses, {
            "A": ["alpha", "beta"],      # 3 each
            "B": ["alpha"],              # 12
            "C": ["alpha", "gamma", "delta"],  # 1 each
            "D": ["omega"],              # no credits: ignored
        })
        self.assertEqual(weights["alpha"]["ects"], 3.0)
        self.assertEqual(weights["alpha"]["per_course"], {"A": 3.0, "B": 12.0, "C": 1.0})
        self.assertEqual(weights["beta"]["ects"], 3.0)
        self.assertNotIn("omega", weights)


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

    def test_prompts_separate_topic_and_proposal_instructions(self):
        self.assertEqual(
            {path.name for path in PROMPTS_DIR.glob("*.txt")},
            {
                "system.txt",
                "course.txt",
                "proposal.txt",
                "design_system.txt",
                "modules.txt",
                "assembly.txt",
                "assembly_repair.txt",
            },
        )
        self.assertNotIn("PlantUML", SYSTEM_PROMPT)
        self.assertNotIn(
            "source_course_mappings",
            (PROMPTS_DIR / "proposal.txt").read_text(encoding="utf-8"),
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
                self.assertNotIn("# Misleading title B", second_prompt)
                self.assertIn("<syllabus_markdown>", second_prompt)
                self.assertIn('<prior_course_memberships>\n{"A": ["alpha"]}', second_prompt)

    def test_global_mode_writes_one_ontology_and_proposal(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            first_cluster = cluster(2, "Two", course("B"))
            second_cluster = cluster(1, "One", course("A"))
            paths = []
            for item in (first_cluster, second_cluster):
                for current in item.courses:
                    path = root / f"{item.cluster_id}-{current.course_id}.yml"
                    path.write_text(yaml.safe_dump({"course_title": {"id": current.course_id, "name": current.title}, "syllabus": {"en": {"contents": {"Course contents": current.course_contents, "Learning outcomes": current.learning_outcomes}}}}), encoding="utf-8")
                    paths.append((item, current, path))
            manifest = root / "clusters.yml"
            manifest.write_text(yaml.safe_dump({item.name: {"index": item.cluster_id, "courses": [{"id": current.course_id, "path": str(path)} for source, current, path in paths if source == item]} for item in (first_cluster, second_cluster)}), encoding="utf-8")
            refined = topic_response(["foundations"], remove=["alpha"], upsert=[Topic(key="foundations", description="Refined.")], updates=[CourseTopicMembership(course_id="A", topic_keys=["foundations"])])
            responses = [
                module_partition(("foundations_module", ["foundations"])),
                course_assembly(("foundations_course", ["foundations_module"])),
            ]
            output = run_restructuring(manifest, self.config, self.retry, all_courses=True, client=FakeClient([self.first, refined, *responses]), cache_dir=root / "cache", output_root=root / "output", now=datetime(2026, 7, 29, 12, 34))
            self.assertTrue((output / "topics-global.yml").exists())
            self.assertTrue((output / "modules-global.yml").exists())
            payload = yaml.safe_load((output / "restructure-proposal-global.yml").read_text())
            self.assertEqual(
                payload["source_course_mappings"],
                [
                    {"course_id": course_id, "proposed_course_keys": ["foundations_course"], "overshoot": 0.0}
                    for course_id in ("A", "B")
                ],
            )
            self.assertEqual(payload["proposed_course_reuse"], {"foundations_course": 2})
            self.assertTrue((output / "topics-of-course-A.yml").exists())
            self.assertTrue((output / "restructure-proposal-global.yml").exists())
            self.assertEqual(yaml.safe_load((output / "topics-of-course-A.yml").read_text())["topics"], {"foundations": "Refined."})

    def test_global_proposal_failure_is_nonfatal_and_cli_rejects_selectors(self):
        args = build_parser().parse_args(["clusters.yml", "--all-courses", "--cluster-id", "1"])
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            _validate_args(args)
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            corpus = load_global_corpus([self.cluster])
            conversation = process_cluster_topics(
                corpus, FakeClient([self.first, self.second]), self.config,
                self.retry, root / "cache", root / "output",
            )
            self.assertFalse(generate_global_proposal(
                corpus, conversation.state, FakeClient([RuntimeError("unavailable")]),
                self.config, self.retry, root / "cache", root / "output",
            ))
            self.assertTrue((root / "output" / "topics-global.yml").exists())
            self.assertFalse((root / "output" / "restructure-proposal-global.yml").exists())

    def test_global_topics_can_be_reused_for_a_larger_proposal_budget(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            source = root / "source"
            process_cluster_topics(
                load_global_corpus([self.cluster]),
                FakeClient([self.first, self.second]), self.config, self.retry,
                root / "source-cache", source,
            )
            client = FakeClient([
                module_partition(("alpha_module", ["alpha"]), ("beta_module", ["beta"])),
                course_assembly(("alpha_course", ["alpha_module"]), ("beta_course", ["beta_module"])),
            ])
            output = run_restructuring(
                write_cluster_input(root, self.cluster),
                ModelConfig(
                    endpoint=self.config.endpoint,
                    model=self.config.model,
                    max_completion_tokens=32768,
                ),
                self.retry,
                all_courses=True,
                client=client,
                cache_dir=root / "new-cache",
                output_root=root / "output",
                reuse_topic_dirs=(source,),
                now=datetime(2026, 7, 29, 12, 34),
            )
            self.assertEqual(
                [call["response_format"] for call in client.completions.calls],
                [TopicPartition, CourseAssembly],
            )
            self.assertTrue((output / "restructure-proposal-global.yml").exists())

    def test_global_proposal_replays_cache_and_repairs_invalid_assembly(self):
        corpus = load_global_corpus([self.cluster])
        state = ClusterTopicState(
            topics={"alpha": "Alpha.", "beta": "Beta."},
            memberships={"A": ["alpha"], "B": ["beta"]},
        )
        partition = module_partition(("alpha_module", ["alpha"]), ("beta_module", ["beta"]))
        unused_module = course_assembly(("alpha_course", ["alpha_module"]))
        # A repair reply repeating an accepted course is rejected and retried.
        duplicate = course_assembly(("alpha_course", ["beta_module"]))
        repair = course_assembly(("beta_course", ["beta_module"]))
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            client = FakeClient([partition, unused_module, duplicate, repair])
            self.assertTrue(generate_global_proposal(
                corpus, state, client, self.config,
                RetryConfig(max_retries=1, initial_backoff=0),
                root / "cache", root / "first",
            ))
            repair_messages = client.completions.calls[-1]["messages"]
            self.assertIn('["beta_module"]', repair_messages[-3]["content"])
            self.assertIn("must be unique", repair_messages[-1]["content"])
            payload = yaml.safe_load(
                (root / "first" / "restructure-proposal-global.yml").read_text()
            )
            self.assertEqual(
                [item["proposed_course_keys"] for item in payload["source_course_mappings"]],
                [["alpha_course"], ["beta_course"]],
            )
            no_calls = FakeClient([])
            self.assertTrue(generate_global_proposal(
                corpus, state, no_calls, self.config, self.retry,
                root / "cache", root / "second",
            ))
            self.assertEqual(no_calls.completions.calls, [])
            self.assertEqual(
                (root / "first" / "restructure-proposal-global.yml").read_text(),
                (root / "second" / "restructure-proposal-global.yml").read_text(),
            )

    def test_topic_batches_partition_assigned_topics_by_co_teaching(self):
        memberships = {
            f"course{index}": [f"{group}_{item}" for item in range(4)]
            for index, group in enumerate(["red", "red", "blue", "blue", "green"])
        }
        batches = topic_batches(memberships, max_size=4)
        self.assertEqual(
            sorted(map(tuple, batches)),
            sorted(
                tuple(f"{group}_{item}" for item in range(4))
                for group in ("blue", "green", "red")
            ),
        )
        packed = topic_batches(memberships, max_size=8)
        self.assertEqual(sorted(key for batch in packed for key in batch), sorted({
            key for keys in memberships.values() for key in keys
        }))
        self.assertTrue(all(len(batch) <= 8 for batch in packed))
        self.assertEqual(packed, topic_batches(memberships, max_size=8))

    def test_topic_partition_must_cover_batch_exactly_once(self):
        validate_topic_partition(["a", "b"], module_partition(("m", ["a", "b"])))
        with self.assertRaisesRegex(ValueError, "several modules"):
            validate_topic_partition(
                ["a", "b"], module_partition(("m", ["a", "b"]), ("n", ["a"]))
            )
        with self.assertRaisesRegex(ValueError, "missing=\\['b'\\], unknown=\\['c'\\]"):
            validate_topic_partition(["a", "b"], module_partition(("m", ["a", "c"])))

    def test_source_mappings_prefer_full_cover_with_least_unrelated_content(self):
        proposal = RestructuringProposal(
            proposed_topics=[
                ProposedTopic(key=key, description=key, source_topic_keys=sources)
                for key, sources in (
                    ("core", ["alpha"]),
                    ("broad", ["alpha", "beta", "gamma"]),
                    ("narrow", ["beta"]),
                )
            ],
            proposed_courses=[
                ProposedCourse(key="broad_course", title="Broad", ects=3, topic_keys=["broad"]),
                ProposedCourse(key="core_course", title="Core", ects=3, topic_keys=["core"]),
                ProposedCourse(key="narrow_course", title="Narrow", ects=3, topic_keys=["narrow"]),
            ],
            prerequisites=[],
        )
        mappings = derive_source_course_mappings(
            self.cluster,
            {"A": ["alpha", "beta"], "B": ["beta"]},
            proposal,
        )
        self.assertEqual(
            [(item.course_id, item.proposed_course_keys) for item in mappings],
            [("A", ["broad_course"]), ("B", ["narrow_course"])],
        )

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

    def test_topics_exist_before_proposal_and_writes_yaml_and_mermaid(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            input_path = write_cluster_input(root, self.cluster)
            output_root = root / "output"

            def check_before_proposal(arguments):
                if arguments["response_format"] is RestructuringProposal:
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
                        valid_proposal(),
                    ],
                    on_parse=check_before_proposal,
                ),
                cache_dir=root / "cache",
                output_root=output_root,
                now=datetime(2026, 7, 29, 12, 34),
            )
            proposal_path = output_dir / "restructure-proposal-for-cluster-5.yml"
            proposal = yaml.safe_load(proposal_path.read_text(encoding="utf-8"))
            self.assertEqual(proposal["cluster"]["id"], 5)
            self.assertEqual(proposal["source_topics"], {
                "alpha": "Alpha from evidence.",
                "beta": "Beta from evidence.",
            })
            mermaid = proposal_path.with_suffix(".mmd").read_text(encoding="utf-8")
            self.assertIn("Foundations<br/>6 ECTS", mermaid)
            self.assertNotIn("-.->", mermaid)
            topics = (output_dir / "restructure-proposal-for-cluster-5-topics.mmd").read_text(encoding="utf-8")
            self.assertIn("Foundations<br/>6 ECTS<br/>", topics)
            self.assertNotIn("-.->", topics)
            mapping = (output_dir / "restructure-proposal-for-cluster-5-mapping.mmd").read_text(encoding="utf-8")
            # Both source courses map to the same new course: one grouped node.
            self.assertEqual(mapping.count("-.->"), 1)
            self.assertEqual(mapping.count(":::source"), 1)

    def test_complete_topic_artifacts_can_be_reused_without_topic_calls(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            source = root / "source"
            process_cluster_topics(
                self.cluster,
                FakeClient([self.first, self.second]),
                self.config,
                self.retry,
                root / "source-cache",
                source,
            )
            client = FakeClient([valid_proposal()])
            output = run_restructuring(
                write_cluster_input(root, self.cluster),
                self.config,
                self.retry,
                client=client,
                cache_dir=root / "new-cache",
                output_root=root / "output",
                reuse_topic_dirs=(source,),
                now=datetime(2026, 7, 29, 12, 34),
            )
            self.assertEqual(len(client.completions.calls), 1)
            self.assertIs(
                client.completions.calls[0]["response_format"],
                RestructuringProposal,
            )
            self.assertEqual(
                (source / "topics-of-cluster-5.yml").read_text(),
                (output / "topics-of-cluster-5.yml").read_text(),
            )
            self.assertEqual(list((root / "new-cache").glob("*.yml")), [])

    def test_invalid_proposal_is_retried_with_feedback_before_writing(self):
        invalid = valid_proposal()
        invalid.proposed_topics[0].source_topic_keys = ["alpha"]
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = pathlib.Path(tmp_dir)
            client = FakeClient(
                [self.first, self.second, invalid, valid_proposal()]
            )
            output_dir = run_restructuring(
                write_cluster_input(root, self.cluster),
                self.config,
                RetryConfig(max_retries=1, initial_backoff=0),
                client=client,
                cache_dir=root / "cache",
                output_root=root / "output",
                now=datetime(2026, 7, 29, 12, 34),
            )
            payload = yaml.safe_load(
                (output_dir / "restructure-proposal-for-cluster-5.yml").read_text()
            )
            self.assertEqual(
                {item["course_id"] for item in payload["source_course_mappings"]},
                {"A", "B"},
            )
            retried = client.completions.calls[-1]["messages"]
            self.assertEqual(retried[-2]["content"], invalid.model_dump_json())
            self.assertIn("rejected", retried[-1]["content"])
            self.assertIn("beta", retried[-1]["content"])

    def test_proposal_llm_failure_is_nonfatal_after_topic_artifacts(self):
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
                        RuntimeError("proposal model unavailable"),
                    ]
                ),
                cache_dir=root / "cache",
                output_root=root / "output",
                now=datetime(2026, 7, 29, 12, 34),
            )
            self.assertTrue((output_dir / "topics-of-cluster-5.yml").exists())
            self.assertTrue((output_dir / "topics-of-course-A.yml").exists())
            self.assertTrue((output_dir / "topics-of-course-B.yml").exists())
            self.assertFalse(
                (
                    output_dir
                    / "restructure-proposal-for-cluster-5.yml"
                ).exists()
            )

    def test_proposal_validation_rejects_cycles(self):
        proposal = RestructuringProposal(
            proposed_topics=[
                ProposedTopic(
                    key="alpha",
                    description="Alpha",
                    source_topic_keys=["alpha"],
                )
            ],
            proposed_courses=[
                ProposedCourse(key="one", title="One", ects=3, topic_keys=["alpha"]),
                ProposedCourse(key="two", title="Two", ects=3, topic_keys=["alpha"]),
            ],
            prerequisites=[
                CoursePrerequisite(
                    prerequisite_course_key="one",
                    dependent_course_key="two",
                ),
                CoursePrerequisite(
                    prerequisite_course_key="two",
                    dependent_course_key="one",
                ),
            ],
        )
        with self.assertRaisesRegex(ValueError, "acyclic"):
            validate_restructuring_proposal(
                self.cluster,
                {"alpha": "Alpha"},
                {"A": ["alpha"], "B": ["alpha"]},
                proposal,
            )

        incomplete = valid_proposal()
        incomplete.proposed_topics[0].source_topic_keys = ["alpha"]
        with self.assertRaisesRegex(ValueError, "missing from proposed topic provenance.*beta"):
            validate_restructuring_proposal(
                self.cluster,
                {"alpha": "Alpha", "beta": "Beta"},
                {"A": ["alpha"], "B": ["beta"]},
                incomplete,
            )

        fat = valid_proposal()
        fat.proposed_courses[0].ects = 3
        fat.proposed_courses[0].topic_keys = ["foundations", "a", "b", "c"]
        fat.proposed_topics.extend(
            ProposedTopic(key=key, description=key, source_topic_keys=[])
            for key in ("a", "b", "c")
        )
        with self.assertRaisesRegex(ValueError, "more topics than ECTS.*foundations_course"):
            validate_restructuring_proposal(
                self.cluster,
                {"alpha": "Alpha", "beta": "Beta"},
                {"A": ["alpha"], "B": ["beta"]},
                fat,
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
                    response = RestructuringProposal(
                        proposed_topics=[
                            ProposedTopic(
                                key="cluster_topic",
                                description="Deterministic syllabus topic.",
                                source_topic_keys=["cluster_topic"],
                            )
                        ],
                        proposed_courses=[
                            ProposedCourse(
                                key="proposed_course",
                                title="Proposed course",
                                ects=3,
                                topic_keys=["cluster_topic"],
                            )
                        ],
                        prerequisites=[],
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
                len(list(output.glob("restructure-proposal-*.yml"))),
                30,
            )
            self.assertEqual(
                len(list(output.glob("restructure-proposal-*.mmd"))),
                90,
            )
        self.assertEqual(completions.calls, 293 + 30)


if __name__ == "__main__":
    unittest.main()

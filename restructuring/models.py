from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


TOPIC_KEY_PATTERN = r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$"


class Topic(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    key: str = Field(pattern=TOPIC_KEY_PATTERN)
    description: str = Field(min_length=1)


class CourseTopicMembership(BaseModel):
    model_config = ConfigDict(extra="forbid")

    course_id: str = Field(min_length=1)
    topic_keys: list[str]

    @model_validator(mode="after")
    def validate_topic_keys(self) -> "CourseTopicMembership":
        if len(self.topic_keys) != len(set(self.topic_keys)):
            raise ValueError("topic_keys must be unique")
        invalid = sorted(
            key for key in self.topic_keys
            if re.fullmatch(TOPIC_KEY_PATTERN, key) is None
        )
        if invalid:
            raise ValueError(f"invalid topic keys: {invalid}")
        return self


class TopicDiff(BaseModel):
    model_config = ConfigDict(extra="forbid")

    remove_topic_keys: list[str] = Field(default_factory=list)
    upsert_topics: list[Topic] = Field(default_factory=list)
    course_topic_updates: list[CourseTopicMembership] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_diff(self) -> "TopicDiff":
        if len(self.remove_topic_keys) != len(set(self.remove_topic_keys)):
            raise ValueError("remove_topic_keys must be unique")
        invalid = sorted(
            key for key in self.remove_topic_keys
            if re.fullmatch(TOPIC_KEY_PATTERN, key) is None
        )
        if invalid:
            raise ValueError(f"invalid removed topic keys: {invalid}")
        upserted = [topic.key for topic in self.upsert_topics]
        if len(upserted) != len(set(upserted)):
            raise ValueError("upsert topic keys must be unique within a diff")
        updated_courses = [item.course_id for item in self.course_topic_updates]
        if len(updated_courses) != len(set(updated_courses)):
            raise ValueError(
                "course assignment updates must be unique within a diff"
            )
        overlap = sorted(set(self.remove_topic_keys) & set(upserted))
        if overlap:
            raise ValueError(
                f"a diff cannot remove and upsert the same topics: {overlap}"
            )
        return self


class CourseTopicsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    covered_topic_keys: list[str]
    topic_diffs: list[TopicDiff] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_response(self) -> "CourseTopicsResponse":
        # Structured-output schemas cannot express the cross-field constraint that
        # at least one of a diff's three arrays must be non-empty. Treat an empty
        # diff as the model's harmless, verbose spelling of "no changes".
        self.topic_diffs = [
            diff
            for diff in self.topic_diffs
            if diff.remove_topic_keys
            or diff.upsert_topics
            or diff.course_topic_updates
        ]
        if len(self.covered_topic_keys) != len(set(self.covered_topic_keys)):
            raise ValueError("covered_topic_keys must be unique")
        invalid = sorted(
            key for key in self.covered_topic_keys
            if re.fullmatch(TOPIC_KEY_PATTERN, key) is None
        )
        if invalid:
            raise ValueError(f"invalid covered topic keys: {invalid}")
        return self


class PlantUMLResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plantuml: str = Field(min_length=1)


@dataclass(frozen=True)
class CourseInput:
    course_id: str
    title: str
    path: str
    course_contents: str
    course_contents_language: str | None
    learning_outcomes: str
    learning_outcomes_language: str | None
    syllabus_sections: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class ClusterInput:
    cluster_id: int
    name: str
    courses: tuple[CourseInput, ...]


@dataclass(frozen=True)
class ModelConfig:
    endpoint: str
    model: str
    temperature: float | None = None
    max_completion_tokens: int = 8192

    def cache_parameters(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "temperature": self.temperature,
            "max_completion_tokens": self.max_completion_tokens,
            "response_format": "pydantic_structured_output",
        }


@dataclass(frozen=True)
class RetryConfig:
    max_retries: int = 6
    initial_backoff: float = 1.0
    max_backoff: float = 60.0

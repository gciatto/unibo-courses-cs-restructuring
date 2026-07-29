from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


TOPIC_KEY_PATTERN = r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$"


class Topic(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str = Field(pattern=TOPIC_KEY_PATTERN)
    description: str = Field(min_length=1)


class CourseTopicsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topics: list[Topic]
    covered_topic_keys: list[str]

    @model_validator(mode="after")
    def validate_topics(self) -> "CourseTopicsResponse":
        keys = [topic.key for topic in self.topics]
        if len(keys) != len(set(keys)):
            raise ValueError("topic keys must be unique")
        unknown = sorted(set(self.covered_topic_keys) - set(keys))
        if unknown:
            raise ValueError(f"covered topics are not present in topics: {unknown}")
        if len(self.covered_topic_keys) != len(set(self.covered_topic_keys)):
            raise ValueError("covered_topic_keys must be unique")
        return self


class CourseTopicMembership(BaseModel):
    model_config = ConfigDict(extra="forbid")

    course_id: str = Field(min_length=1)
    topic_keys: list[str]


class FinalClusterResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topics: list[Topic]
    course_topics: list[CourseTopicMembership]
    plantuml: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_topics(self) -> "FinalClusterResponse":
        keys = [topic.key for topic in self.topics]
        if len(keys) != len(set(keys)):
            raise ValueError("topic keys must be unique")
        course_ids = [course.course_id for course in self.course_topics]
        if len(course_ids) != len(set(course_ids)):
            raise ValueError("course IDs in course_topics must be unique")
        known = set(keys)
        for course in self.course_topics:
            unknown = sorted(set(course.topic_keys) - known)
            if unknown:
                raise ValueError(f"course {course.course_id} references unknown topics: {unknown}")
            if len(course.topic_keys) != len(set(course.topic_keys)):
                raise ValueError(f"course {course.course_id} has duplicate topic keys")
        return self


@dataclass(frozen=True)
class CourseInput:
    course_id: str
    title: str
    path: str
    course_contents: str
    course_contents_language: str | None
    learning_outcomes: str
    learning_outcomes_language: str | None


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


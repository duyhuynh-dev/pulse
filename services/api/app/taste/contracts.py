from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class SubredditActivitySummary(BaseModel):
    subreddit: str
    comment_count: int = 0
    submission_count: int = 0
    total_karma: int = 0


class RecentComment(BaseModel):
    subreddit: str
    body: str
    score: int = 0
    created_at: datetime
    post_title: str | None = None
    permalink: str | None = None
    signal_source: Literal["authored", "saved"] = "authored"


class RecentSubmission(BaseModel):
    subreddit: str
    title: str
    score: int = 0
    created_at: datetime
    permalink: str | None = None
    body: str | None = None
    signal_source: Literal["authored", "saved", "subscribed"] = "authored"


class NormalizedRedditActivity(BaseModel):
    source: Literal["public_username", "reddit_export", "manual"]
    source_key: str
    username: str | None = None
    fetched_at: datetime
    total_comments: int
    total_submissions: int
    subreddit_activity: list[SubredditActivitySummary] = Field(default_factory=list)
    recent_comments: list[RecentComment] = Field(default_factory=list)
    recent_submissions: list[RecentSubmission] = Field(default_factory=list)


class CachedActivityEnvelope(BaseModel):
    schema_version: str
    saved_at: datetime
    activity: NormalizedRedditActivity

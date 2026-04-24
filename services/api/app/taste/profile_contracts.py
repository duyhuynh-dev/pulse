from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field


class ThemeEvidenceCount(BaseModel):
    key: str
    count: int


class ThemeEvidenceSnippet(BaseModel):
    type: Literal[
        "comment",
        "submission",
        "saved_comment",
        "saved_submission",
        "subscription",
        "manual",
        "provider_note",
        "spotify_artist",
        "spotify_track",
    ]
    subreddit: str | None = None
    snippet: str
    permalink: str | None = None


class ThemeEvidence(BaseModel):
    matched_subreddits: list[ThemeEvidenceCount] = Field(default_factory=list)
    matched_keywords: list[ThemeEvidenceCount] = Field(default_factory=list)
    top_examples: list[ThemeEvidenceSnippet] = Field(default_factory=list)
    provider_notes: list[str] = Field(default_factory=list)


class TasteTheme(BaseModel):
    id: str
    label: str
    confidence: int
    confidence_label: Literal["Strong", "Clear", "Emerging", "Weak"]
    evidence: ThemeEvidence = Field(default_factory=ThemeEvidence)


class ThemeCatalogItem(BaseModel):
    id: str
    label: str
    description: str


class TasteProfile(BaseModel):
    source: Literal["manual", "public_username", "reddit_export", "spotify"]
    source_key: str
    username: str | None = None
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    themes: list[TasteTheme] = Field(default_factory=list)
    unmatched_activity: dict = Field(default_factory=dict)

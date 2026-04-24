from __future__ import annotations

import csv
import io
import zipfile

import pytest

from app.taste.errors import InsufficientSignalError, InvalidRedditExportError
from app.taste.providers.reddit_export import RedditExportProvider


def _build_export_zip(*, comments: list[dict[str, object]], posts: list[dict[str, object]]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        comments_io = io.StringIO()
        comment_writer = csv.DictWriter(
            comments_io,
            fieldnames=["id", "author", "subreddit", "body", "score", "created_utc", "link_title", "permalink"],
        )
        comment_writer.writeheader()
        comment_writer.writerows(comments)
        archive.writestr("comments.csv", comments_io.getvalue())

        posts_io = io.StringIO()
        post_writer = csv.DictWriter(
            posts_io,
            fieldnames=["id", "author", "subreddit", "title", "selftext", "score", "created_utc", "permalink"],
        )
        post_writer.writeheader()
        post_writer.writerows(posts)
        archive.writestr("posts.csv", posts_io.getvalue())

    return buffer.getvalue()


def test_reddit_export_provider_parses_zip_activity() -> None:
    provider = RedditExportProvider()
    export_bytes = _build_export_zip(
        comments=[
            {
                "id": "c1",
                "author": "duy",
                "subreddit": "aves",
                "body": "Looking for a warehouse techno night in Bushwick.",
                "score": 11,
                "created_utc": 1713877200,
                "link_title": "Any good afters this weekend?",
                "permalink": "/r/aves/comments/abc123/comment/c1/",
            },
            {
                "id": "c2",
                "author": "duy",
                "subreddit": "FoodNYC",
                "body": "Need a great late-night food crawl after the show.",
                "score": 7,
                "created_utc": 1713880800,
                "link_title": "Best spots after midnight",
                "permalink": "/r/FoodNYC/comments/def456/comment/c2/",
            },
        ],
        posts=[
            {
                "id": "p1",
                "author": "duy",
                "subreddit": "indieheads",
                "title": "Best intimate venue for an indie band this month?",
                "selftext": "Looking for a singer-songwriter room with good sound.",
                "score": 14,
                "created_utc": 1713884400,
                "permalink": "/r/indieheads/comments/post1/best_intimate_venue/",
            }
        ],
    )

    activity = provider.parse_bytes(export_bytes, filename="reddit-export.zip")

    assert activity.source == "reddit_export"
    assert activity.username == "duy"
    assert activity.total_comments == 2
    assert activity.total_submissions == 1
    assert [summary.subreddit for summary in activity.subreddit_activity] == ["FoodNYC", "aves", "indieheads"]
    assert activity.recent_comments[0].permalink == "https://www.reddit.com/r/FoodNYC/comments/def456/comment/c2/"
    assert activity.recent_submissions[0].body == "Looking for a singer-songwriter room with good sound."


def test_reddit_export_provider_builds_theme_profile_with_evidence() -> None:
    provider = RedditExportProvider()
    export_bytes = _build_export_zip(
        comments=[
            {
                "id": "c1",
                "author": "duy",
                "subreddit": "aves",
                "body": "Need a warehouse rave with techno DJs and good sound system.",
                "score": 10,
                "created_utc": 1713877200,
                "link_title": "Bushwick afters recs",
                "permalink": "/r/aves/comments/abc123/comment/c1/",
            },
            {
                "id": "c2",
                "author": "duy",
                "subreddit": "FoodNYC",
                "body": "Where should we go for late-night food after the club?",
                "score": 9,
                "created_utc": 1713880800,
                "link_title": "Post-show food",
                "permalink": "/r/FoodNYC/comments/def456/comment/c2/",
            },
        ],
        posts=[
            {
                "id": "p1",
                "author": "duy",
                "subreddit": "indieheads",
                "title": "Which venue is best for an indie live show this weekend?",
                "selftext": "Looking for a small room with a touring band.",
                "score": 14,
                "created_utc": 1713884400,
                "permalink": "/r/indieheads/comments/post1/venue_question/",
            }
        ],
    )

    profile = provider.build_profile_from_bytes(export_bytes, filename="reddit-export.zip")

    assert profile.source == "reddit_export"
    theme_ids = [theme.id for theme in profile.themes]
    assert "underground_dance" in theme_ids
    assert "indie_live_music" in theme_ids
    assert "late_night_food" in theme_ids
    underground = next(theme for theme in profile.themes if theme.id == "underground_dance")
    assert underground.evidence.matched_subreddits[0].key == "aves"
    assert underground.evidence.top_examples[0].type in {"comment", "submission"}
    assert underground.evidence.provider_notes


def test_reddit_export_provider_rejects_invalid_archives() -> None:
    provider = RedditExportProvider()

    with pytest.raises(InvalidRedditExportError):
        provider.parse_bytes(b"not-a-zip", filename="reddit-export.zip")


def test_reddit_export_provider_raises_insufficient_signal_for_irrelevant_activity() -> None:
    provider = RedditExportProvider()
    export_bytes = _build_export_zip(
        comments=[
            {
                "id": "c1",
                "author": "duy",
                "subreddit": "Python",
                "body": "Dependency injection is nice here.",
                "score": 5,
                "created_utc": 1713877200,
                "link_title": "FastAPI discussion",
                "permalink": "/r/Python/comments/abc123/comment/c1/",
            }
        ],
        posts=[],
    )

    with pytest.raises(InsufficientSignalError):
        provider.build_profile_from_bytes(export_bytes, filename="reddit-export.zip")

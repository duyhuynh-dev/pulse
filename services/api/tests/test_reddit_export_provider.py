from __future__ import annotations

import csv
import io
import zipfile

import pytest

from app.taste.errors import InsufficientSignalError, InvalidRedditExportError
from app.taste.providers.reddit_export import RedditExportProvider


def _build_export_zip(
    *,
    comments: list[dict[str, object]],
    posts: list[dict[str, object]],
    saved_posts: list[dict[str, object]] | None = None,
    saved_comments: list[dict[str, object]] | None = None,
    subscribed_subreddits: list[dict[str, object]] | None = None,
) -> bytes:
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

        if saved_posts is not None:
            saved_posts_io = io.StringIO()
            saved_post_writer = csv.DictWriter(saved_posts_io, fieldnames=["id", "permalink"])
            saved_post_writer.writeheader()
            saved_post_writer.writerows(saved_posts)
            archive.writestr("saved_posts.csv", saved_posts_io.getvalue())

        if saved_comments is not None:
            saved_comments_io = io.StringIO()
            saved_comment_writer = csv.DictWriter(saved_comments_io, fieldnames=["id", "permalink"])
            saved_comment_writer.writeheader()
            saved_comment_writer.writerows(saved_comments)
            archive.writestr("saved_comments.csv", saved_comments_io.getvalue())

        if subscribed_subreddits is not None:
            subscribed_io = io.StringIO()
            subscribed_writer = csv.DictWriter(subscribed_io, fieldnames=["subreddit"])
            subscribed_writer.writeheader()
            subscribed_writer.writerows(subscribed_subreddits)
            archive.writestr("subscribed_subreddits.csv", subscribed_io.getvalue())

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


def test_reddit_export_provider_uses_saved_posts_and_subscriptions_as_signal() -> None:
    provider = RedditExportProvider()
    export_bytes = _build_export_zip(
        comments=[],
        posts=[],
        saved_posts=[
            {
                "id": "sp1",
                "permalink": "https://www.reddit.com/r/FoodNYC/comments/xyz123/best_late_night_popup_dinner_in_manhattan/",
            },
            {
                "id": "sp2",
                "permalink": "https://www.reddit.com/r/indieheads/comments/xyz124/favorite_room_for_a_touring_band_in_brooklyn/",
            },
        ],
        saved_comments=[
            {
                "id": "sc1",
                "permalink": "https://www.reddit.com/r/aves/comments/xyz125/best_warehouse_rave_afters_in_bushwick/sc1/",
            }
        ],
        subscribed_subreddits=[
            {"subreddit": "rupaulsdragrace"},
        ],
    )

    activity = provider.parse_bytes(export_bytes, filename="reddit-export.zip")
    assert activity.total_comments == 1
    assert activity.total_submissions == 3
    assert {summary.subreddit for summary in activity.subreddit_activity} >= {"FoodNYC", "indieheads", "aves", "rupaulsdragrace"}

    profile = provider.build_profile_from_activity(activity)
    assert {theme.id for theme in profile.themes} >= {
        "late_night_food",
        "indie_live_music",
        "underground_dance",
        "queer_nightlife",
    }
    assert any(
        example.type in {"saved_submission", "saved_comment", "subscription"}
        for theme in profile.themes
        for example in theme.evidence.top_examples
    )


def test_reddit_export_provider_accepts_subscription_only_archives() -> None:
    provider = RedditExportProvider()
    export_bytes = _build_export_zip(
        comments=[],
        posts=[],
        subscribed_subreddits=[
            {"subreddit": "aves"},
            {"subreddit": "FoodNYC"},
        ],
    )

    activity = provider.parse_bytes(export_bytes, filename="reddit-export.zip")
    assert activity.total_comments == 0
    assert activity.total_submissions == 2
    assert [summary.subreddit for summary in activity.subreddit_activity] == ["FoodNYC", "aves"]


def test_reddit_export_provider_supports_nested_export_filenames_and_account_csv() -> None:
    provider = RedditExportProvider()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        account_io = io.StringIO()
        account_writer = csv.DictWriter(account_io, fieldnames=["username"])
        account_writer.writeheader()
        account_writer.writerow({"username": "nested-duy"})
        archive.writestr("reddit/account.csv", account_io.getvalue())

        comments_io = io.StringIO()
        comment_writer = csv.DictWriter(
            comments_io,
            fieldnames=["subreddit_name_prefixed", "body_md", "score", "created_at", "link_title", "link_permalink"],
        )
        comment_writer.writeheader()
        comment_writer.writerow(
            {
                "subreddit_name_prefixed": "r/aves",
                "body_md": "Need another rooftop afters with a real sound system.",
                "score": 23,
                "created_at": "2026-04-01T03:00:00Z",
                "link_title": "Bushwick night recs",
                "link_permalink": "/r/aves/comments/x1/comment/y1/",
            }
        )
        archive.writestr("reddit_data/comments-2026.csv", comments_io.getvalue())

        posts_io = io.StringIO()
        post_writer = csv.DictWriter(
            posts_io,
            fieldnames=["subreddit_name_prefixed", "title", "selftext_md", "score", "created_at", "permalink"],
        )
        post_writer.writeheader()
        post_writer.writerow(
            {
                "subreddit_name_prefixed": "r/FoodNYC",
                "title": "Best popup dinner after midnight?",
                "selftext_md": "Need an omakase-ish late night plan.",
                "score": 18,
                "created_at": "2026-04-02T04:00:00Z",
                "permalink": "/r/FoodNYC/comments/x2/best_popup_dinner/",
            }
        )
        archive.writestr("archive/submitted-posts.csv", posts_io.getvalue())

    activity = provider.parse_bytes(buffer.getvalue(), filename="nested-export.zip")

    assert activity.username == "nested-duy"
    assert activity.recent_comments[0].subreddit == "aves"
    assert activity.recent_comments[0].score == 23
    assert activity.recent_submissions[0].subreddit == "FoodNYC"
    profile = provider.build_profile_from_activity(activity)
    assert {theme.id for theme in profile.themes} & {"underground_dance", "late_night_food", "rooftop_lounges"}


def test_reddit_export_provider_supports_json_array_payloads() -> None:
    provider = RedditExportProvider()
    payload = [
        {
            "author": "duy",
            "subreddit": "indieheads",
            "title": "Best room for a touring band",
            "selftext": "Need a venue rec.",
            "score": 9,
            "created_utc": 1713884400,
            "permalink": "/r/indieheads/comments/post1/venue_question/",
        },
        {
            "author": "duy",
            "subreddit": "aves",
            "body": "Any good warehouse rave afters?",
            "link_title": "Bushwick afters",
            "score": 12,
            "created_utc": 1713877200,
            "permalink": "/r/aves/comments/c1/",
        },
    ]

    activity = provider.parse_bytes(
        io.BytesIO(str(payload).replace("'", '"').encode("utf-8")).getvalue(),
        filename="reddit-export.json",
    )

    assert activity.total_comments == 1
    assert activity.total_submissions == 1

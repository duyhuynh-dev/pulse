from __future__ import annotations

import csv
import io
import json
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.taste.contracts import (
    NormalizedRedditActivity,
    RecentComment,
    RecentSubmission,
    SubredditActivitySummary,
)
from app.taste.errors import (
    InsufficientSignalError,
    InvalidRedditExportError,
    NoPublicActivityError,
)
from app.taste.profile_contracts import (
    TasteProfile,
    TasteTheme,
    ThemeEvidence,
    ThemeEvidenceCount,
    ThemeEvidenceSnippet,
)
from app.taste.theme_catalog import THEME_CATALOG_BY_ID


@dataclass(frozen=True)
class RedditThemeRule:
    theme_id: str
    subreddit_weights: dict[str, int]
    keyword_weights: dict[str, int]
    provider_note: str


REDDIT_THEME_RULES: tuple[RedditThemeRule, ...] = (
    RedditThemeRule(
        theme_id="underground_dance",
        subreddit_weights={
            "aves": 4,
            "techno": 4,
            "electronicmusic": 3,
            "dnb": 3,
            "drumandbass": 3,
            "nycnightlife": 2,
        },
        keyword_weights={
            "warehouse": 3,
            "techno": 3,
            "rave": 3,
            "afters": 3,
            "dj": 1,
            "sound system": 2,
            "club night": 2,
        },
        provider_note="Built from dance-music communities and rave language in your Reddit export.",
    ),
    RedditThemeRule(
        theme_id="indie_live_music",
        subreddit_weights={
            "indieheads": 4,
            "letstalkmusic": 3,
            "listentothis": 2,
            "music": 1,
            "brooklynmusic": 2,
        },
        keyword_weights={
            "live show": 3,
            "tour": 2,
            "band": 2,
            "venue": 2,
            "gig": 2,
            "singer-songwriter": 2,
            "alt-pop": 2,
        },
        provider_note="Built from band-forward communities and live-show language in your Reddit export.",
    ),
    RedditThemeRule(
        theme_id="gallery_nights",
        subreddit_weights={
            "art": 2,
            "contemporaryart": 3,
            "artistlounge": 2,
            "streetart": 2,
        },
        keyword_weights={
            "gallery": 3,
            "opening": 2,
            "installation": 3,
            "exhibit": 2,
            "museum": 2,
            "artist talk": 2,
            "curator": 2,
        },
        provider_note="Built from art communities and opening-night language in your Reddit export.",
    ),
    RedditThemeRule(
        theme_id="jazz_intimate_shows",
        subreddit_weights={
            "jazz": 4,
            "jazzguitar": 2,
            "vinyljazz": 2,
            "jazzpiano": 2,
        },
        keyword_weights={
            "jazz": 2,
            "quartet": 3,
            "trio": 3,
            "listening room": 3,
            "improv": 2,
            "small room": 2,
        },
        provider_note="Built from jazz communities and intimate-room language in your Reddit export.",
    ),
    RedditThemeRule(
        theme_id="hiphop_rap_shows",
        subreddit_weights={
            "hiphopheads": 4,
            "rap": 3,
            "makinghiphop": 2,
            "trapmuzik": 2,
        },
        keyword_weights={
            "rap": 2,
            "hip hop": 2,
            "cypher": 3,
            "freestyle": 3,
            "beats": 1,
            "showcase": 2,
        },
        provider_note="Built from rap communities and performance-heavy language in your Reddit export.",
    ),
    RedditThemeRule(
        theme_id="comedy_nights",
        subreddit_weights={
            "standup": 4,
            "comedy": 2,
            "improv": 3,
            "livefromnewyork": 2,
        },
        keyword_weights={
            "stand-up": 3,
            "comedian": 2,
            "open mic": 3,
            "improv": 2,
            "sketch": 2,
            "comic": 2,
        },
        provider_note="Built from comedy communities and room-night language in your Reddit export.",
    ),
    RedditThemeRule(
        theme_id="dive_bar_scene",
        subreddit_weights={
            "beer": 2,
            "cocktails": 1,
            "brooklyn": 1,
            "asknyc": 1,
        },
        keyword_weights={
            "dive bar": 4,
            "neighborhood bar": 3,
            "cheap drinks": 3,
            "jukebox": 2,
            "pool table": 2,
            "local spot": 2,
        },
        provider_note="Built from neighborhood-bar language and low-key night recommendations in your Reddit export.",
    ),
    RedditThemeRule(
        theme_id="rooftop_lounges",
        subreddit_weights={
            "asknyc": 1,
            "cocktails": 2,
            "nycbitcheswithtaste": 2,
        },
        keyword_weights={
            "rooftop": 4,
            "cocktail bar": 3,
            "lounge": 2,
            "dress code": 2,
            "happy hour": 1,
            "view": 1,
        },
        provider_note="Built from rooftop, cocktail, and polished-night language in your Reddit export.",
    ),
    RedditThemeRule(
        theme_id="late_night_food",
        subreddit_weights={
            "foodnyc": 4,
            "nycfood": 4,
            "seriouseats": 2,
            "askculinary": 1,
        },
        keyword_weights={
            "late-night": 3,
            "late night": 3,
            "food crawl": 2,
            "reservation": 1,
            "popup dinner": 3,
            "tasting menu": 2,
            "omakase": 2,
        },
        provider_note="Built from food communities and after-hours dining language in your Reddit export.",
    ),
    RedditThemeRule(
        theme_id="queer_nightlife",
        subreddit_weights={
            "lgbt": 2,
            "rupaulsdragrace": 3,
            "ainbow": 2,
            "askgaybros": 1,
            "actuallesbians": 1,
        },
        keyword_weights={
            "queer": 3,
            "drag": 3,
            "gay bar": 3,
            "lesbian bar": 3,
            "ballroom": 2,
            "pride": 2,
        },
        provider_note="Built from queer community signals and nightlife-specific language in your Reddit export.",
    ),
)

_COMMENT_FILENAMES = ("comments.csv", "comments.json")
_SUBMISSION_FILENAMES = ("posts.csv", "posts.json", "submissions.csv", "submissions.json")
_ACCOUNT_FILENAMES = ("account.json", "user.json", "profile.json", "account.csv", "user.csv", "profile.csv")
_SAVED_COMMENT_FILENAMES = ("saved_comments.csv", "saved_comments.json")
_SAVED_SUBMISSION_FILENAMES = ("saved_posts.csv", "saved_posts.json", "saved_submissions.csv", "saved_submissions.json")
_SUBSCRIBED_SUBREDDIT_FILENAMES = (
    "subscribed_subreddits.csv",
    "subscribed_subreddits.json",
    "subscriptions.csv",
    "subscriptions.json",
)


class RedditExportProvider:
    source_name = "reddit_export"

    async def fetch(self, source_key: str) -> NormalizedRedditActivity:
        path = Path(source_key)
        raw_bytes = path.read_bytes()
        return self.parse_bytes(raw_bytes, filename=path.name)

    async def build_profile(self, source_key: str) -> TasteProfile:
        activity = await self.fetch(source_key)
        return self.build_profile_from_activity(activity)

    def parse_bytes(self, raw_bytes: bytes, *, filename: str = "reddit-export.zip") -> NormalizedRedditActivity:
        normalized_filename = filename.lower()
        if normalized_filename.endswith(".zip"):
            return self._parse_zip(raw_bytes, filename=filename)
        if normalized_filename.endswith(".json"):
            return self._parse_json_document(raw_bytes, filename=filename)
        raise InvalidRedditExportError("Reddit export must be a zip archive or JSON document.")

    def build_profile_from_bytes(self, raw_bytes: bytes, *, filename: str = "reddit-export.zip") -> TasteProfile:
        activity = self.parse_bytes(raw_bytes, filename=filename)
        return self.build_profile_from_activity(activity, source_key=filename)

    def build_profile_from_activity(
        self,
        activity: NormalizedRedditActivity,
        *,
        source_key: str | None = None,
    ) -> TasteProfile:
        themes: list[TasteTheme] = []
        matched_subreddits_global: set[str] = set()

        combined_items: list[tuple[str, RecentComment | RecentSubmission]] = [
            *[
                ("saved_comment" if comment.signal_source == "saved" else "comment", comment)
                for comment in activity.recent_comments
            ],
            *[
                (
                    "saved_submission"
                    if submission.signal_source == "saved"
                    else "subscription"
                    if submission.signal_source == "subscribed"
                    else "submission",
                    submission,
                )
                for submission in activity.recent_submissions
            ],
        ]

        for rule in REDDIT_THEME_RULES:
            theme = self._score_theme(rule, combined_items)
            if theme is None:
                continue
            matched_subreddits_global.update(item.key for item in theme.evidence.matched_subreddits)
            themes.append(theme)

        themes.sort(key=lambda item: item.confidence, reverse=True)
        if not themes:
            raise InsufficientSignalError(
                "Reddit export did not surface enough cultural taste signal yet."
            )

        unmatched_activity = {
            "topUnmatchedSubreddits": [
                {
                    "subreddit": summary.subreddit,
                    "interactionCount": summary.comment_count + summary.submission_count,
                }
                for summary in activity.subreddit_activity
                if summary.subreddit.lower() not in matched_subreddits_global
            ][:8]
        }

        return TasteProfile(
            source="reddit_export",
            source_key=source_key or activity.source_key,
            username=activity.username,
            themes=themes,
            unmatched_activity=unmatched_activity,
        )

    def _parse_zip(self, raw_bytes: bytes, *, filename: str) -> NormalizedRedditActivity:
        try:
            archive = zipfile.ZipFile(io.BytesIO(raw_bytes))
        except zipfile.BadZipFile as error:
            raise InvalidRedditExportError("Unable to read the uploaded Reddit export zip.") from error

        comments_rows: list[dict[str, Any]] = []
        submission_rows: list[dict[str, Any]] = []
        saved_comment_rows: list[dict[str, Any]] = []
        saved_submission_rows: list[dict[str, Any]] = []
        subscribed_subreddit_rows: list[dict[str, Any]] = []
        username: str | None = None

        with archive:
            for member_name in archive.namelist():
                path = Path(member_name)
                if member_name.endswith("/") or any(part == "__MACOSX" for part in path.parts):
                    continue
                basename = path.name.lower()
                if basename.startswith("._"):
                    continue
                with archive.open(member_name) as member:
                    member_bytes = member.read()

                if _is_comment_member(basename):
                    comments_rows = self._parse_rows(member_bytes, basename)
                    username = username or _extract_username_from_rows(comments_rows)
                elif _is_submission_member(basename):
                    submission_rows = self._parse_rows(member_bytes, basename)
                    username = username or _extract_username_from_rows(submission_rows)
                elif _is_saved_comment_member(basename):
                    saved_comment_rows = self._parse_rows(member_bytes, basename)
                    username = username or _extract_username_from_rows(saved_comment_rows)
                elif _is_saved_submission_member(basename):
                    saved_submission_rows = self._parse_rows(member_bytes, basename)
                    username = username or _extract_username_from_rows(saved_submission_rows)
                elif _is_subscribed_subreddit_member(basename):
                    subscribed_subreddit_rows = self._parse_rows(member_bytes, basename)
                elif _is_account_member(basename):
                    username = username or _extract_username_from_payload(member_bytes, basename)

        return self._build_activity(
            comments_rows,
            submission_rows,
            saved_comment_rows=saved_comment_rows,
            saved_submission_rows=saved_submission_rows,
            subscribed_subreddit_rows=subscribed_subreddit_rows,
            username=username,
            source_key=filename,
        )

    def _parse_json_document(self, raw_bytes: bytes, *, filename: str) -> NormalizedRedditActivity:
        try:
            payload = json.loads(raw_bytes.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise InvalidRedditExportError("Unable to parse the uploaded Reddit export JSON.") from error

        if isinstance(payload, list):
            rows = _coerce_json_rows(payload)
            comment_rows, submission_rows = _split_rows_by_shape(rows)
            return self._build_activity(
                comment_rows,
                submission_rows,
                username=None,
                source_key=filename,
            )

        if not isinstance(payload, dict):
            raise InvalidRedditExportError("Reddit export JSON must be an object or an array.")

        comments_rows = _coerce_json_rows(payload.get("comments"))
        submission_rows = _coerce_json_rows(
            payload.get("posts") or payload.get("submissions") or payload.get("submitted")
        )
        saved_comment_rows = _coerce_json_rows(payload.get("saved_comments") or payload.get("savedComments"))
        saved_submission_rows = _coerce_json_rows(
            payload.get("saved_posts")
            or payload.get("savedPosts")
            or payload.get("saved_submissions")
            or payload.get("savedSubmissions")
        )
        subscribed_subreddit_rows = _coerce_json_rows(
            payload.get("subscribed_subreddits")
            or payload.get("subscribedSubreddits")
            or payload.get("subscriptions")
        )
        username = (
            _string_or_none(payload.get("username"))
            or _string_or_none(payload.get("user"))
            or _extract_username_from_json(raw_bytes)
        )

        return self._build_activity(
            comments_rows,
            submission_rows,
            saved_comment_rows=saved_comment_rows,
            saved_submission_rows=saved_submission_rows,
            subscribed_subreddit_rows=subscribed_subreddit_rows,
            username=username,
            source_key=filename,
        )

    def _parse_rows(self, raw_bytes: bytes, filename: str) -> list[dict[str, Any]]:
        if filename.endswith(".csv"):
            try:
                text = raw_bytes.decode("utf-8-sig")
            except UnicodeDecodeError as error:
                raise InvalidRedditExportError(f"Unable to decode {filename} from the Reddit export.") from error
            return list(csv.DictReader(io.StringIO(text)))

        if filename.endswith(".json"):
            try:
                payload = json.loads(raw_bytes.decode("utf-8-sig"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise InvalidRedditExportError(f"Unable to parse {filename} from the Reddit export.") from error
            return _coerce_json_rows(payload)

        raise InvalidRedditExportError(f"Unsupported Reddit export file format: {filename}")

    def _build_activity(
        self,
        comments_rows: list[dict[str, Any]],
        submission_rows: list[dict[str, Any]],
        *,
        saved_comment_rows: list[dict[str, Any]] | None = None,
        saved_submission_rows: list[dict[str, Any]] | None = None,
        subscribed_subreddit_rows: list[dict[str, Any]] | None = None,
        username: str | None,
        source_key: str,
    ) -> NormalizedRedditActivity:
        comments = [comment for comment in (_comment_from_row(row) for row in comments_rows) if comment is not None]
        submissions = [
            submission
            for submission in (_submission_from_row(row) for row in submission_rows)
            if submission is not None
        ]
        saved_comments = [
            comment
            for comment in (_saved_comment_from_row(row) for row in (saved_comment_rows or []))
            if comment is not None
        ]
        saved_submissions = [
            submission
            for submission in (_saved_submission_from_row(row) for row in (saved_submission_rows or []))
            if submission is not None
        ]
        subscribed_submissions = [
            submission
            for submission in (_subscription_from_row(row) for row in (subscribed_subreddit_rows or []))
            if submission is not None
        ]
        comments.extend(saved_comments)
        submissions.extend(saved_submissions)
        submissions.extend(subscribed_submissions)

        if not comments and not submissions:
            raise NoPublicActivityError(
                "The Reddit export did not include usable comments, posts, saved items, or followed communities."
            )

        subreddit_counts: dict[str, dict[str, int]] = defaultdict(
            lambda: {"comment_count": 0, "submission_count": 0, "total_karma": 0}
        )

        for comment in comments:
            summary = subreddit_counts[comment.subreddit]
            summary["comment_count"] += 1
            summary["total_karma"] += comment.score

        for submission in submissions:
            summary = subreddit_counts[submission.subreddit]
            summary["submission_count"] += 1
            summary["total_karma"] += submission.score

        subreddit_activity = [
            SubredditActivitySummary(subreddit=subreddit, **counts)
            for subreddit, counts in sorted(
                subreddit_counts.items(),
                key=lambda item: (
                    -(item[1]["comment_count"] + item[1]["submission_count"]),
                    item[0],
                ),
            )
        ]

        comments.sort(key=lambda item: item.created_at, reverse=True)
        submissions.sort(key=lambda item: item.created_at, reverse=True)

        return NormalizedRedditActivity(
            source="reddit_export",
            source_key=source_key,
            username=username,
            fetched_at=datetime.now(UTC),
            total_comments=len(comments),
            total_submissions=len(submissions),
            subreddit_activity=subreddit_activity,
            recent_comments=comments,
            recent_submissions=submissions,
        )

    def _score_theme(
        self,
        rule: RedditThemeRule,
        items: list[tuple[str, RecentComment | RecentSubmission]],
    ) -> TasteTheme | None:
        subreddit_hits: Counter[str] = Counter()
        keyword_hits: Counter[str] = Counter()
        examples: list[ThemeEvidenceSnippet] = []
        weighted_subreddit_score = 0.0
        weighted_keyword_score = 0.0

        for item_type, item in items:
            subreddit = item.subreddit.lower()
            text = _item_text(item)
            matched = False
            score_bonus = _item_score_bonus(item)
            source_multiplier = _signal_source_multiplier(item)

            if subreddit in rule.subreddit_weights:
                subreddit_hits[subreddit] += 1
                weighted_subreddit_score += rule.subreddit_weights[subreddit] * 5 * source_multiplier * (1 + (0.25 * score_bonus))
                matched = True

            for keyword in rule.keyword_weights:
                if keyword in text:
                    keyword_hits[keyword] += 1
                    weighted_keyword_score += rule.keyword_weights[keyword] * 3 * source_multiplier * (1 + (0.25 * score_bonus))
                    matched = True

            if matched and len(examples) < 3:
                examples.append(
                    ThemeEvidenceSnippet(
                        type=item_type,
                        subreddit=item.subreddit,
                        snippet=_example_snippet(item),
                        permalink=getattr(item, "permalink", None),
                    )
                )

        if not subreddit_hits and not keyword_hits:
            return None

        diversity_bonus = min(12, len(subreddit_hits) * 3 + len(keyword_hits) * 2)
        confidence = min(95, round(weighted_subreddit_score + weighted_keyword_score + diversity_bonus))
        if confidence < 32 or not examples:
            return None

        item = THEME_CATALOG_BY_ID[rule.theme_id]
        return TasteTheme(
            id=item.id,
            label=item.label,
            confidence=confidence,
            confidence_label=_confidence_label(confidence),
            evidence=ThemeEvidence(
                matched_subreddits=[
                    ThemeEvidenceCount(key=subreddit, count=count)
                    for subreddit, count in subreddit_hits.most_common(4)
                ],
                matched_keywords=[
                    ThemeEvidenceCount(key=keyword, count=count)
                    for keyword, count in keyword_hits.most_common(5)
                ],
                top_examples=examples,
                provider_notes=[rule.provider_note],
            ),
        )


def _coerce_json_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        items = value.get("items")
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
    return []


def _extract_username_from_json(raw_bytes: bytes) -> str | None:
    try:
        payload = json.loads(raw_bytes.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    return (
        _string_or_none(payload.get("username"))
        or _string_or_none(payload.get("user"))
        or _string_or_none(payload.get("name"))
        or _string_or_none((payload.get("account") or {}).get("username"))
    )


def _extract_username_from_payload(raw_bytes: bytes, filename: str) -> str | None:
    if filename.endswith(".json"):
        return _extract_username_from_json(raw_bytes)

    rows = RedditExportProvider()._parse_rows(raw_bytes, filename)
    return _extract_username_from_rows(rows)


def _extract_username_from_rows(rows: list[dict[str, Any]]) -> str | None:
    for row in rows:
        username = _first_present(row, "username", "author", "user", "account")
        if username:
            return username
    return None


def _comment_from_row(row: dict[str, Any]) -> RecentComment | None:
    subreddit = _subreddit_from_row(row)
    body = _first_present(row, "body", "body_md", "comment", "comment_body", "text")
    if not subreddit or not body:
        return None

    return RecentComment(
        subreddit=subreddit,
        body=body,
        score=_parse_int(_first_present(row, "score", "karma", "ups", "comment_karma")) or 0,
        created_at=_parse_datetime(
            _first_present(row, "created_utc", "created_at", "created", "date", "timestamp", "created")
        ),
        post_title=_first_present(row, "link_title", "post_title", "submission_title", "title")
        or _permalink_title(_first_present(row, "permalink", "link_permalink", "link", "url")),
        permalink=_normalize_permalink(_first_present(row, "permalink", "link_permalink", "link", "url")),
        signal_source="authored",
    )


def _submission_from_row(row: dict[str, Any]) -> RecentSubmission | None:
    subreddit = _subreddit_from_row(row)
    title = _first_present(row, "title", "post_title", "submission_title") or _permalink_title(
        _first_present(row, "permalink", "link", "url")
    )
    if not subreddit or not title:
        return None

    return RecentSubmission(
        subreddit=subreddit,
        title=title,
        score=_parse_int(_first_present(row, "score", "karma", "ups")) or 0,
        created_at=_parse_datetime(
            _first_present(row, "created_utc", "created_at", "created", "date", "timestamp")
        ),
        permalink=_normalize_permalink(_first_present(row, "permalink", "link", "url")),
        body=_first_present(row, "body", "selftext", "selftext_md", "text"),
        signal_source="authored",
    )


def _saved_comment_from_row(row: dict[str, Any]) -> RecentComment | None:
    permalink = _normalize_permalink(_first_present(row, "permalink", "link", "url"))
    subreddit = _subreddit_from_row(row)
    if not subreddit:
        return None

    post_title = _first_present(row, "link_title", "post_title", "submission_title", "title") or _permalink_title(permalink)
    body = _first_present(row, "body", "body_md", "comment", "comment_body", "text")
    synthesized_body = body or post_title or f"Saved comment in r/{subreddit}"

    return RecentComment(
        subreddit=subreddit,
        body=synthesized_body,
        score=_parse_int(_first_present(row, "score", "karma", "ups", "comment_karma")) or 0,
        created_at=_parse_datetime(
            _first_present(row, "saved_at", "created_utc", "created_at", "created", "date", "timestamp")
        ),
        post_title=post_title,
        permalink=permalink,
        signal_source="saved",
    )


def _saved_submission_from_row(row: dict[str, Any]) -> RecentSubmission | None:
    permalink = _normalize_permalink(_first_present(row, "permalink", "link", "url"))
    subreddit = _subreddit_from_row(row)
    title = _first_present(row, "title", "post_title", "submission_title") or _permalink_title(permalink)
    if not subreddit or not title:
        return None

    return RecentSubmission(
        subreddit=subreddit,
        title=title,
        score=_parse_int(_first_present(row, "score", "karma", "ups")) or 0,
        created_at=_parse_datetime(
            _first_present(row, "saved_at", "created_utc", "created_at", "created", "date", "timestamp")
        ),
        permalink=permalink,
        body=_first_present(row, "body", "selftext", "selftext_md", "text"),
        signal_source="saved",
    )


def _subscription_from_row(row: dict[str, Any]) -> RecentSubmission | None:
    subreddit = _normalize_subreddit(
        _first_present(row, "subreddit", "subreddit_name_prefixed", "display_name", "name")
    )
    if not subreddit:
        return None

    return RecentSubmission(
        subreddit=subreddit,
        title=f"Subscribed to r/{subreddit}",
        score=0,
        created_at=_parse_datetime(_first_present(row, "created_at", "created", "timestamp")),
        signal_source="subscribed",
    )


def _first_present(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _parse_datetime(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)

    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except ValueError:
        pass

    normalized = value.strip().replace("Z", "+00:00")
    if normalized.endswith(" UTC"):
        normalized = normalized.removesuffix(" UTC") + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return datetime.now(UTC)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _normalize_permalink(value: str | None) -> str | None:
    if not value:
        return None
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("/"):
        return f"https://www.reddit.com{value}"
    return value


def _normalize_subreddit(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.lower().startswith("r/"):
        return normalized[2:]
    return normalized


def _subreddit_from_row(row: dict[str, Any]) -> str | None:
    explicit = _normalize_subreddit(_first_present(row, "subreddit", "subreddit_name_prefixed"))
    if explicit:
        return explicit
    return _permalink_subreddit(_first_present(row, "permalink", "link_permalink", "link", "url"))


def _permalink_subreddit(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    marker = "/r/"
    if marker not in normalized:
        return None
    tail = normalized.split(marker, 1)[1]
    subreddit = tail.split("/", 1)[0].strip()
    return subreddit or None


def _permalink_title(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().rstrip("/")
    marker = "/comments/"
    if marker not in normalized:
        return None
    tail = normalized.split(marker, 1)[1]
    parts = [part for part in tail.split("/") if part]
    if len(parts) < 2:
        return None
    slug = parts[1]
    words = slug.replace("_", " ").replace("-", " ").strip()
    return words or None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _item_text(item: RecentComment | RecentSubmission) -> str:
    parts = [
        item.subreddit,
        getattr(item, "title", None),
        getattr(item, "body", None),
        getattr(item, "post_title", None),
    ]
    return " ".join(part.lower() for part in parts if part)


def _item_score_bonus(item: RecentComment | RecentSubmission) -> int:
    if item.score >= 20:
        return 2
    if item.score >= 5:
        return 1
    return 0


def _signal_source_multiplier(item: RecentComment | RecentSubmission) -> float:
    signal_source = getattr(item, "signal_source", "authored")
    if signal_source == "saved":
        return 1.5
    if signal_source == "subscribed":
        return 2.0
    return 1.0


def _example_snippet(item: RecentComment | RecentSubmission) -> str:
    source_text = getattr(item, "body", None) or getattr(item, "title", None) or getattr(item, "post_title", None)
    if source_text is None:
        return item.subreddit
    return source_text if len(source_text) <= 140 else f"{source_text[:137].rstrip()}..."


def _confidence_label(confidence: int) -> str:
    if confidence >= 80:
        return "Strong"
    if confidence >= 60:
        return "Clear"
    if confidence >= 40:
        return "Emerging"
    return "Weak"


def _is_comment_member(basename: str) -> bool:
    if basename in _COMMENT_FILENAMES:
        return True
    return basename.startswith("comments") and basename.endswith((".csv", ".json")) and "header" not in basename


def _is_submission_member(basename: str) -> bool:
    if basename in _SUBMISSION_FILENAMES:
        return True
    if not basename.endswith((".csv", ".json")):
        return False
    if "header" in basename:
        return False
    return any(
        token in basename
        for token in ("posts", "post-", "submissions", "submission-", "submitted", "submitted-")
    )


def _is_saved_comment_member(basename: str) -> bool:
    if basename in _SAVED_COMMENT_FILENAMES:
        return True
    return basename.startswith("saved_comments") and basename.endswith((".csv", ".json"))


def _is_saved_submission_member(basename: str) -> bool:
    if basename in _SAVED_SUBMISSION_FILENAMES:
        return True
    return (
        basename.endswith((".csv", ".json"))
        and ("saved_posts" in basename or "saved_submissions" in basename)
        and "header" not in basename
    )


def _is_subscribed_subreddit_member(basename: str) -> bool:
    if basename in _SUBSCRIBED_SUBREDDIT_FILENAMES:
        return True
    return basename.endswith((".csv", ".json")) and (
        "subscribed_subreddits" in basename or basename.startswith("subscriptions")
    )


def _is_account_member(basename: str) -> bool:
    if basename in _ACCOUNT_FILENAMES:
        return True
    return "account" in basename and basename.endswith((".csv", ".json"))


def _split_rows_by_shape(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    comments: list[dict[str, Any]] = []
    submissions: list[dict[str, Any]] = []
    for row in rows:
        if _first_present(row, "comment", "comment_body", "body", "body_md") and _first_present(
            row, "link_title", "post_title", "submission_title"
        ):
            comments.append(row)
        elif _first_present(row, "title", "post_title", "submission_title"):
            submissions.append(row)
        elif _first_present(row, "comment", "comment_body", "body", "body_md"):
            comments.append(row)
    return comments, submissions

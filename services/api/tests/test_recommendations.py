from datetime import UTC, datetime

from app.models.recommendation import RecommendationRun
from app.models.user import UserAnchorLocation, UserConstraint
from app.schemas.recommendations import (
    RecommendationFreshness,
    RecommendationProvenance,
    RecommendationScoreBreakdownItem,
    TravelEstimate,
    VenueRecommendationCard,
)
from app.services.recommendations import (
    AnchorResolution,
    CandidateScoreComponents,
    _compare_shortlists,
    _comparison_summary_sentence,
    _context_hash,
    _deletable_run_ids,
    _driver_summaries,
    _pack_reason_payload,
    _score_breakdown_items,
    _score_summary,
    _unpack_reason_payload,
)


def test_score_breakdown_summarizes_positive_and_negative_drivers() -> None:
    items = _score_breakdown_items(
        components=CandidateScoreComponents(
            interest_fit=0.82,
            category_fit=0.45,
            distance_fit=0.95,
            budget_fit=0.92,
            source_confidence=0.88,
            transit_minutes=18,
            weighted_interest=0.525,
            weighted_category=0.068,
            weighted_distance=0.105,
            weighted_budget=0.092,
            weighted_source=0.044,
        ),
        matched_labels=["Underground dance", "Indie live music"],
        muted_labels=[],
        feedback_adjustment=-0.08,
        feedback_reason={"detail": "You recently dismissed similar club-heavy picks."},
    )

    assert any(item["key"] == "profile_fit" for item in items)
    assert any(item["key"] == "feedback" and item["direction"] == "negative" for item in items)

    summary = _score_summary(items)

    assert summary is not None
    assert "profile fit" in summary
    assert "holding it back" in summary


def test_pack_and_unpack_reason_payload_preserves_explainability_metadata() -> None:
    score_breakdown = [
        {
            "key": "profile_fit",
            "label": "Profile fit",
            "impactLabel": "driving this pick",
            "detail": "Matched Underground dance.",
            "contribution": 0.44,
            "direction": "positive",
            "summaryLabel": "profile fit",
        }
    ]
    payload = _pack_reason_payload(
        [
            {
                "title": "Profile match",
                "detail": "This venue lines up with your underground dance signals.",
            }
        ],
        score_summary="Led by profile fit.",
        score_breakdown=score_breakdown,
    )

    reasons, score_summary, unpacked_breakdown = _unpack_reason_payload(payload)

    assert reasons[0].title == "Profile match"
    assert score_summary == "Led by profile fit."
    assert unpacked_breakdown[0].key == "profile_fit"
    assert unpacked_breakdown[0].impactLabel == "driving this pick"


def _sample_card(
    *,
    venue_id: str,
    venue_name: str,
    score: float,
    score_band: str,
    score_summary: str,
    score_breakdown: list[RecommendationScoreBreakdownItem],
) -> VenueRecommendationCard:
    return VenueRecommendationCard(
        venueId=venue_id,
        venueName=venue_name,
        neighborhood="East Village",
        address="123 Example St",
        eventTitle="Sample Event",
        eventId=f"{venue_id}-event",
        startsAt="2026-04-24T20:00:00+00:00",
        priceLabel="$25",
        scoreBand=score_band,
        score=score,
        travel=[TravelEstimate(mode="transit", label="18 min transit", minutes=18)],
        reasons=[],
        freshness=RecommendationFreshness(
            discoveredAt="2026-04-23T20:00:00+00:00",
            lastVerifiedAt="2026-04-24T18:00:00+00:00",
            freshnessLabel="Checked today",
        ),
        provenance=RecommendationProvenance(
            sourceName="Curated venue calendars",
            sourceKind="curated_calendar",
            sourceConfidence=0.88,
            sourceConfidenceLabel="High trust",
            sourceBaseUrl="https://example.com",
            hasTicketUrl=True,
        ),
        scoreSummary=score_summary,
        scoreBreakdown=score_breakdown,
        secondaryEvents=[],
    )


def test_driver_summaries_split_positive_and_negative_signals() -> None:
    cards = [
        _sample_card(
            venue_id="venue-1",
            venue_name="Public Records",
            score=0.92,
            score_band="high",
            score_summary="Led by profile fit and travel convenience.",
            score_breakdown=[
                RecommendationScoreBreakdownItem(
                    key="profile_fit",
                    label="Profile fit",
                    impactLabel="driving this pick",
                    detail="Matched Underground dance.",
                    contribution=0.44,
                    direction="positive",
                ),
                RecommendationScoreBreakdownItem(
                    key="feedback",
                    label="Recent feedback",
                    impactLabel="holding it back",
                    detail="You recently dismissed similar club-heavy picks.",
                    contribution=-0.08,
                    direction="negative",
                ),
            ],
        ),
        _sample_card(
            venue_id="venue-2",
            venue_name="Elsewhere",
            score=0.87,
            score_band="high",
            score_summary="Led by profile fit and source trust.",
            score_breakdown=[
                RecommendationScoreBreakdownItem(
                    key="profile_fit",
                    label="Profile fit",
                    impactLabel="strong support",
                    detail="Matched Underground dance.",
                    contribution=0.39,
                    direction="positive",
                ),
                RecommendationScoreBreakdownItem(
                    key="source_trust",
                    label="Source trust",
                    impactLabel="helping",
                    detail="Backed by a highly trusted source.",
                    contribution=0.05,
                    direction="positive",
                ),
            ],
        ),
    ]

    positive, negative = _driver_summaries(cards)

    assert positive[0].key == "profile_fit"
    assert "Public Records" in positive[0].topVenues
    assert negative[0].key == "feedback"
    assert negative[0].averageContribution < 0


def test_context_hash_changes_when_shortlist_changes() -> None:
    run = RecommendationRun(
        id="run-1",
        user_id="user-1",
        provider="catalog",
        model_name="pulse-deterministic-v1",
        viewport_json={},
        created_at=datetime(2026, 4, 24, 18, 0, tzinfo=UTC),
    )
    active_anchor = UserAnchorLocation(
        user_id="user-1",
        source="neighborhood",
        neighborhood="East Village",
        latitude=40.7265,
        longitude=-73.9815,
    )
    constraints = UserConstraint(
        user_id="user-1",
        city="New York City",
        neighborhood="East Village",
        zip_code="10003",
        radius_miles=8,
        budget_level="under_75",
        preferred_days_csv="Thursday,Friday,Saturday",
        social_mode="either",
    )
    resolution = AnchorResolution(
        requested_anchor=active_anchor,
        active_anchor=active_anchor,
        requested_within_service_area=True,
        used_fallback_anchor=False,
    )
    topics = []
    shortlist = [
        _sample_card(
            venue_id="venue-1",
            venue_name="Public Records",
            score=0.92,
            score_band="high",
            score_summary="Led by profile fit.",
            score_breakdown=[],
        )
    ]

    first_hash = _context_hash(
        run=run,
        resolution=resolution,
        constraints=constraints,
        topics=topics,
        items=shortlist,
    )
    second_hash = _context_hash(
        run=run,
        resolution=resolution,
        constraints=constraints,
        topics=topics,
        items=[
            _sample_card(
                venue_id="venue-2",
                venue_name="Elsewhere",
                score=0.88,
                score_band="high",
                score_summary="Led by travel fit.",
                score_breakdown=[],
            )
        ],
    )

    assert first_hash != second_hash


def test_deletable_run_ids_keeps_recent_runs_and_protected_snapshots() -> None:
    run_ids = ["run-newest", "run-2", "run-3", "run-4", "run-5"]
    protected_run_ids = {"run-4"}

    deletable = _deletable_run_ids(run_ids, protected_run_ids, keep_recent_count=2)

    assert deletable == ["run-3", "run-5"]


def test_compare_shortlists_identifies_new_dropped_and_moved_venues() -> None:
    previous_items = [
        _sample_card(
            venue_id="venue-a",
            venue_name="Public Records",
            score=0.92,
            score_band="high",
            score_summary="Led by profile fit.",
            score_breakdown=[],
        ),
        _sample_card(
            venue_id="venue-b",
            venue_name="Elsewhere",
            score=0.88,
            score_band="high",
            score_summary="Led by source trust.",
            score_breakdown=[],
        ),
        _sample_card(
            venue_id="venue-c",
            venue_name="Nowadays",
            score=0.84,
            score_band="medium",
            score_summary="Led by travel fit.",
            score_breakdown=[],
        ),
    ]
    current_items = [
        _sample_card(
            venue_id="venue-b",
            venue_name="Elsewhere",
            score=0.91,
            score_band="high",
            score_summary="Led by profile fit.",
            score_breakdown=[],
        ),
        _sample_card(
            venue_id="venue-a",
            venue_name="Public Records",
            score=0.9,
            score_band="high",
            score_summary="Led by source trust.",
            score_breakdown=[],
        ),
        _sample_card(
            venue_id="venue-d",
            venue_name="Paragon",
            score=0.83,
            score_band="medium",
            score_summary="Led by category overlap.",
            score_breakdown=[],
        ),
    ]

    new_entrants, dropped_venues, movers, steady_leaders = _compare_shortlists(current_items, previous_items)

    assert new_entrants[0].venueId == "venue-d"
    assert dropped_venues[0].venueId == "venue-c"
    assert {item.venueId for item in movers} == {"venue-a", "venue-b"}
    assert steady_leaders == []

    summary = _comparison_summary_sentence(
        new_entrants=new_entrants,
        dropped_venues=dropped_venues,
        movers=movers,
    )
    assert summary is not None
    assert "entered the shortlist" in summary
    assert "dropped out" in summary

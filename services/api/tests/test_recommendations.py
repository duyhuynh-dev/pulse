from app.services.recommendations import (
    CandidateScoreComponents,
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

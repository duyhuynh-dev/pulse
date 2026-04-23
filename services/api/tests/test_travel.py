from datetime import UTC, datetime

from app.models.recommendation import RecommendationRun
from app.models.user import UserAnchorLocation, UserConstraint
from app.services.recommendations import _run_context_changed, _select_active_anchor
from app.services.recommendations import _price_label
from app.services.travel import estimate_travel_bands, haversine_miles


def test_haversine_distance_stays_positive() -> None:
    miles = haversine_miles(40.7315, -73.9897, 40.7063, -73.9232)
    assert miles > 0


def test_travel_bands_return_walk_and_transit() -> None:
    bands = estimate_travel_bands(40.7315, -73.9897, 40.7063, -73.9232)
    assert [band["mode"] for band in bands] == ["walk", "transit"]
    assert bands[0]["minutes"] > bands[1]["minutes"]
    assert bands[0]["minutes"] == 86
    assert bands[1]["minutes"] == 55


def test_price_label_formats_ranges() -> None:
    assert _price_label(20, 35) == "$20-$35"
    assert _price_label(25, 25) == "$25"
    assert _price_label(None, 40) == "Up to $40"


def test_select_active_anchor_skips_out_of_area_live_location() -> None:
    outside_live = UserAnchorLocation(
        user_id="user-1",
        source="live",
        latitude=41.5555,
        longitude=-72.6603,
        is_session_only=True,
    )
    neighborhood_anchor = UserAnchorLocation(
        user_id="user-1",
        source="neighborhood",
        zip_code="10003",
        neighborhood="East Village",
    )

    selected = _select_active_anchor([outside_live, neighborhood_anchor])
    assert selected is neighborhood_anchor


def test_run_context_changed_when_new_anchor_or_constraint_arrives() -> None:
    run = RecommendationRun(
        user_id="user-1",
        created_at=datetime(2026, 4, 23, 1, 0, tzinfo=UTC),
    )
    newer_anchor = UserAnchorLocation(
        user_id="user-1",
        source="zip",
        zip_code="10003",
        created_at=datetime(2026, 4, 23, 1, 5, tzinfo=UTC),
    )
    newer_constraints = UserConstraint(
        user_id="user-1",
        city="New York City",
        updated_at=datetime(2026, 4, 23, 1, 6, tzinfo=UTC),
    )
    older_anchor = UserAnchorLocation(
        user_id="user-1",
        source="zip",
        zip_code="10003",
        created_at=datetime(2026, 4, 23, 0, 55, tzinfo=UTC),
    )

    assert _run_context_changed(run, newer_anchor, None) is True
    assert _run_context_changed(run, older_anchor, newer_constraints) is True
    assert _run_context_changed(run, older_anchor, None) is False

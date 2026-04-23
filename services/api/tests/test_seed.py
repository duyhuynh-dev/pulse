from datetime import datetime

from app.services.seed import NYC_TZ, _next_demo_local_start


def test_next_demo_local_start_returns_expected_nyc_evening_slot() -> None:
    now_local = datetime(2026, 4, 23, 12, 0, tzinfo=NYC_TZ)

    start = _next_demo_local_start(4, 20, 30, now_local=now_local)
    local_start = start.astimezone(NYC_TZ)

    assert local_start.weekday() == 4
    assert local_start.hour == 20
    assert local_start.minute == 30

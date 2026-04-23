from app.connectors.ticketmaster import _normalize_ticketmaster_datetime


def test_normalize_ticketmaster_datetime_prefers_datetime_field() -> None:
    payload = {
        "dateTime": "2026-04-28T00:30:00Z",
        "localDate": "2026-04-27",
        "localTime": "20:30:00",
        "timezone": "America/New_York",
    }

    assert _normalize_ticketmaster_datetime(payload, "America/New_York") == "2026-04-28T00:30:00+00:00"


def test_normalize_ticketmaster_datetime_uses_local_date_time_and_timezone() -> None:
    payload = {
        "localDate": "2026-04-27",
        "localTime": "20:30:00",
    }

    assert _normalize_ticketmaster_datetime(payload, "America/New_York") == "2026-04-28T00:30:00+00:00"


def test_normalize_ticketmaster_datetime_returns_none_without_time_information() -> None:
    payload = {
        "localDate": "2026-04-27",
    }

    assert _normalize_ticketmaster_datetime(payload, "America/New_York") is None

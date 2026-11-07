"""Tests de batch/weather_history.py (sans internet : on simule la réponse de l'API)."""
from datetime import date

from batch.weather_history import clip_to_archive, parse_hourly

PAYLOAD = {
    "hourly": {
        "time": ["2026-10-01T00:00", "2026-10-01T01:00", "2026-10-01T02:00"],
        "temperature_2m": [8.1, 7.6, None],
        "precipitation": [0.0, 1.2, None],
        "rain": [0.0, 1.2, None],
        "wind_speed_10m": [12.0, 14.5, None],
        "is_day": [0, 0, None],
    }
}


def test_parse_hourly_columns_and_types():
    df = parse_hourly("chicago", PAYLOAD)
    assert list(df.columns) == ["city", "weather_ts", "temperature_c", "precipitation_mm",
                                "rain_mm", "wind_speed_kmh", "is_day"]
    assert (df["city"] == "chicago").all()
    assert str(df["weather_ts"].iloc[1]) == "2026-10-01 01:00:00"
    assert df["precipitation_mm"].iloc[1] == 1.2


def test_parse_hourly_drops_empty_hours():
    df = parse_hourly("chicago", PAYLOAD)
    assert len(df) == 2          # la 3e heure n'a aucune mesure


def test_parse_hourly_empty_payload():
    assert parse_hourly("chicago", {}).empty


def test_clip_to_archive_stops_6_days_before_today():
    today = date(2026, 11, 7)
    assert clip_to_archive(date(2026, 10, 1), date(2026, 11, 5), today) == \
        (date(2026, 10, 1), date(2026, 11, 1))


def test_clip_to_archive_too_recent():
    today = date(2026, 11, 7)
    assert clip_to_archive(date(2026, 11, 3), date(2026, 11, 5), today) is None
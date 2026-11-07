"""Tests des contrôles qualité (sans Spark, avec de petits DataFrames)."""
import pandas as pd

from batch.quality_checks import check_gold, check_silver


def _crimes(**overrides):
    df = pd.DataFrame({
        "event_id": ["chicago-1", "chicago-2", "london-1"],
        "city": ["chicago", "chicago", "london"],
        "latitude": [41.88, 41.90, 51.51],
        "longitude": [-87.63, -87.65, -0.13],
        "occurred_ts": pd.to_datetime(["2026-10-01 14:00", "2026-10-01 15:00", "2026-09-01 00:00"]),
        "crime_category": ["theft", "assault", "theft"],
        "hour_known": [1, 1, 0],
    })
    for column, values in overrides.items():
        df[column] = values
    return df


def test_clean_silver_passes():
    assert check_silver(_crimes()) == []


def test_duplicates_are_detected():
    errors = check_silver(_crimes(event_id=["chicago-1", "chicago-1", "london-1"]))
    assert any("double" in e for e in errors)


def test_crime_outside_its_city_is_detected():
    errors = check_silver(_crimes(latitude=[41.88, 51.51, 51.51]))   # un crime de Chicago à Londres
    assert any("hors de leur ville" in e for e in errors)


def test_unknown_category_and_london_hour_are_detected():
    errors = check_silver(_crimes(crime_category=["theft", "aliens", "theft"], hour_known=[1, 1, 1]))
    assert any("catégories inconnues" in e for e in errors)
    assert any("Londres" in e for e in errors)


def test_gold_row_count_and_weather():
    ok = pd.DataFrame({"event_id": ["a", "b"], "weather_level": ["hourly", "daily"]})
    assert check_gold(ok, silver_count=2) == []
    bad = pd.DataFrame({"event_id": ["a", "b", "c"], "weather_level": ["none", "none", "hourly"]})
    errors = check_gold(bad, silver_count=2)
    assert len(errors) == 2
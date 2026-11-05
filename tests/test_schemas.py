"""Tests du format commun (common/schemas.py). Lancer avec : pytest"""
from datetime import datetime

from common.schemas import build_crime_event, categorize, parse_datetime


def test_categorize_examples_from_each_city():
    assert categorize("THEFT") == "theft"                          # Chicago
    assert categorize("PETIT LARCENY") == "theft"                  # New York
    assert categorize("GRAND LARCENY OF MOTOR VEHICLE") == "vehicle_theft"
    assert categorize("MOTOR VEHICLE THEFT") == "vehicle_theft"
    assert categorize("487(D)(1) - PC - F - Grand Theft Auto - GTA  - 240") == "vehicle_theft"  # LA
    assert categorize("211 - PC - F - Robbery - 120") == "robbery"                              # LA
    assert categorize("CRIMINAL SEXUAL ASSAULT") == "sexual"       # pas "assault"
    assert categorize("FELONY ASSAULT") == "assault"
    assert categorize("anti-social-behaviour") == "public_order"   # UK Police
    assert categorize("violent-crime") == "assault"
    assert categorize("criminal-damage-arson") == "vandalism"
    assert categorize("HARRASSMENT 2") == "public_order"           # faute d'orthographe NYPD
    assert categorize("DANGEROUS DRUGS") == "drugs"
    assert categorize("something new") == "other"
    assert categorize(None) == "other"


def test_parse_datetime_formats():
    assert parse_datetime("2026-10-28T14:30:00.000") == datetime(2026, 10, 28, 14, 30)
    assert parse_datetime("2026-09") == datetime(2026, 9, 1)
    assert parse_datetime("2026-11-05T18:00") == datetime(2026, 11, 5, 18, 0)
    assert parse_datetime("pas une date") is None
    assert parse_datetime(None) is None


def _event(**overrides):
    params = dict(city="chicago", source="test", source_id="1", crime_type="theft",
                  description="x", latitude="41.88", longitude="-87.63",
                  occurred_at=datetime(2026, 10, 28, 14, 30), raw={"a": 1})
    params.update(overrides)
    return build_crime_event(**params)


def test_valid_event_has_all_fields():
    event = _event()
    assert event["event_id"] == "chicago-1"
    assert event["crime_type"] == "THEFT"
    assert event["crime_category"] == "theft"
    assert event["latitude"] == 41.88
    assert event["occurred_at"] == "2026-10-28T14:30:00"
    assert event["ingested_at"].endswith("Z")
    assert event["raw"] == {"a": 1}


def test_invalid_events_are_rejected():
    assert _event(latitude=None) is None
    assert _event(latitude="0", longitude="0") is None        # point (0,0) dans l'océan
    assert _event(latitude="51.5", longitude="-0.12") is None  # Londres != Chicago
    assert _event(source_id="") is None
    assert _event(occurred_at=None) is None
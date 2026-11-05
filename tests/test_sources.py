"""
Tests des sources, SANS internet : on donne des lignes "brutes" écrites à la main
(même forme que les vraies APIs) et on vérifie le résultat de normalize().
"""
from producer.sources.base import SocrataSource
from producer.sources.chicago import ChicagoSource
from producer.sources.los_angeles import LosAngelesSource
from producer.sources.new_york import NewYorkSource
from producer.sources.uk_police import UKPoliceSource


def test_chicago_normalize():
    raw = {"id": "13245678", "date": "2026-10-28T14:30:00.000", "primary_type": "THEFT",
           "description": "RETAIL THEFT", "latitude": "41.881", "longitude": "-87.627"}
    event = ChicagoSource().normalize(raw)
    assert event["event_id"] == "chicago-13245678"
    assert event["crime_category"] == "theft"
    assert event["occurred_at"] == "2026-10-28T14:30:00"


def test_new_york_combines_date_and_time():
    raw = {"cmplnt_num": "298765432", "cmplnt_fr_dt": "2026-08-15T00:00:00.000",
           "cmplnt_fr_tm": "21:05:00", "ofns_desc": "PETIT LARCENY",
           "pd_desc": "LARCENY,PETIT FROM STORE-SHOPL", "latitude": "40.75", "longitude": "-73.98"}
    event = NewYorkSource().normalize(raw)
    assert event["occurred_at"] == "2026-08-15T21:05:00"
    assert event["crime_category"] == "theft"


def test_los_angeles_accepts_old_and_new_column_names():
    old_format = {"dr_no": "240104567", "date_occ": "2026-10-01T00:00:00.000", "time_occ": "1430",
                  "crm_cd_desc": "BURGLARY FROM VEHICLE", "lat": "34.05", "lon": "-118.25"}
    new_format = {"uniquenibrno": "X1", "date_occ": "2026-10-01T09:15:00.000",
                  "nibr_description": "Simple Assault", "latitude": "34.05", "longitude": "-118.25"}
    e1 = LosAngelesSource().normalize(old_format)
    e2 = LosAngelesSource().normalize(new_format)
    assert e1["occurred_at"] == "2026-10-01T14:30:00"
    assert e1["crime_category"] == "burglary"
    assert e2["event_id"] == "los_angeles-X1"
    assert e2["crime_category"] == "assault"

def test_los_angeles_real_nibrs_row():
    """Vraie ligne de l'API LA (k7nn-b2ep), récupérée le 05/11/2026."""
    raw = {"caseno": "26210493", "uniquenibrno": "26210493_240_0",
           "date_occ": "2026-10-17T00:00:00.000", "time_occ": "0000",
           "area_name": "West Valley         ",
           "nibr_description": "487(D)(1) - PC - F - Grand Theft Auto - GTA  - 240",
           "crime_against": "Property", "premis_desc": "Street/Parkway",
           "hndrdth_loc_chk": "7900 Wilbur Ave", "hndrdth_lat": "34.2138", "hndrdth_lon": "-118.5448"}
    event = LosAngelesSource().normalize(raw)
    assert event is not None
    assert event["event_id"] == "los_angeles-26210493_240_0"
    assert event["latitude"] == 34.2138 and event["longitude"] == -118.5448
    assert event["crime_category"] == "vehicle_theft"
    assert event["description"] == "Street/Parkway"



def test_uk_police_normalize():
    raw = {"id": 123456789, "category": "anti-social-behaviour", "month": "2026-09",
           "location": {"latitude": "51.5098", "longitude": "-0.1342",
                        "street": {"name": "On or near Regent Street"}}}
    event = UKPoliceSource().normalize(raw)
    assert event["city"] == "london"
    assert event["crime_category"] == "public_order"
    assert event["occurred_at"] == "2026-09-01T00:00:00"
    assert event["description"] == "On or near Regent Street"


class FakeSocrata(SocrataSource):
    """Fausse source : renvoie des lignes fixes au lieu d'appeler internet."""
    city = "chicago"
    date_field = "date"

    def __init__(self, rows):
        self.rows = rows

    def record_id(self, record):
        return record["id"]

    def normalize(self, record):
        return ChicagoSource().normalize(record)


def test_checkpoint_skips_already_sent_rows(monkeypatch):
    rows = [
        {"id": "1", "date": "2026-10-01T10:00:00.000", "primary_type": "THEFT",
         "latitude": "41.88", "longitude": "-87.63"},
        {"id": "2", "date": "2026-10-01T12:00:00.000", "primary_type": "THEFT",
         "latitude": "41.88", "longitude": "-87.63"},
    ]
    monkeypatch.setattr("producer.sources.base.get_json", lambda *a, **k: rows)

    source = FakeSocrata(rows)
    events, checkpoint = source.poll({})
    assert len(events) == 2
    assert checkpoint == {"since": "2026-10-01T12:00:00", "ids_at_since": ["2"]}

    # 2e tour : l'API renvoie la ligne "2" (date >= since) -> elle doit être ignorée
    monkeypatch.setattr("producer.sources.base.get_json", lambda *a, **k: [rows[1]])
    events, checkpoint2 = source.poll(checkpoint)
    assert events == []
    assert checkpoint2 == checkpoint
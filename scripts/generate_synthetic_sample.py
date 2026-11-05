"""
Génère des crimes INVENTÉS (synthétiques) pour tester le pipeline sans internet.

On place quelques "hotspots" (zones avec beaucoup de crimes très proches)
dans chaque ville + du "bruit" (crimes dispersés). DBSCAN doit retrouver
les hotspots et ignorer le bruit : c'est un bon test.

Usage :  python scripts/generate_synthetic_sample.py
Résultat : data/sample/crimes_synthetic.jsonl

IMPORTANT : ces données sont fausses (source = "synthetic").
Pour la démo finale, utilise de vraies données (--save-sample).
"""
import json
import os
import random
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import CITIES  # noqa: E402
from common.schemas import SCHEMA_VERSION, categorize  # noqa: E402

random.seed(42)  # même résultat à chaque exécution

CRIME_TYPES = ["THEFT", "BATTERY", "CRIMINAL DAMAGE", "ASSAULT", "BURGLARY",
               "MOTOR VEHICLE THEFT", "ROBBERY", "NARCOTICS", "WEAPONS VIOLATION"]
WEIGHTS = [30, 18, 12, 10, 8, 8, 6, 5, 3]

# (décalage lat, décalage lon) par rapport au centre-ville, nombre de crimes
HOTSPOTS = [((0.010, 0.012), 25), ((-0.030, -0.020), 15), ((0.045, -0.040), 10)]
NOISE_PER_CITY = 60
HOTSPOT_SPREAD = 0.0015  # ~150 m


def make_event(city_key, i, lat, lon, start):
    crime_type = random.choices(CRIME_TYPES, WEIGHTS)[0]
    occurred = start + timedelta(minutes=random.randint(0, 60 * 24))
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{city_key}-synthetic-{i}",
        "source_id": f"synthetic-{i}",
        "city": city_key,
        "source": "synthetic",
        "crime_type": crime_type,
        "crime_category": categorize(crime_type),
        "description": "synthetic test data",
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "occurred_at": occurred.strftime("%Y-%m-%dT%H:%M:%S"),
        "ingested_at": None,  # rempli au moment du replay
        "ingest_mode": "replay",
        "raw": {"note": "synthetic"},
    }


def main():
    start = datetime(2026, 11, 1)
    events = []
    for city in CITIES.values():
        i = 0
        for (dlat, dlon), count in HOTSPOTS:
            for _ in range(count):
                lat = city.latitude + dlat + random.gauss(0, HOTSPOT_SPREAD)
                lon = city.longitude + dlon + random.gauss(0, HOTSPOT_SPREAD)
                events.append(make_event(city.key, i, lat, lon, start))
                i += 1
        for _ in range(NOISE_PER_CITY):
            lat = random.uniform(city.min_lat + 0.05, city.max_lat - 0.05)
            lon = random.uniform(city.min_lon + 0.05, city.max_lon - 0.05)
            events.append(make_event(city.key, i, lat, lon, start))
            i += 1

    out = os.path.join("data", "sample", "crimes_synthetic.jsonl")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    print(f"{len(events)} crimes synthétiques écrits dans {out}")


if __name__ == "__main__":
    main()
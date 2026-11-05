"""Tests de DBSCAN (streaming/clustering.py) — sans Spark."""
import random
from datetime import datetime

from streaming.clustering import build_alerts, find_hotspots, haversine_km


def _cluster(lat, lon, n, spread=0.001, category="theft"):
    return [{"latitude": lat + random.gauss(0, spread), "longitude": lon + random.gauss(0, spread),
             "crime_category": category} for _ in range(n)]


def test_haversine_known_distance():
    # Paris -> Londres ≈ 344 km
    assert 340 < haversine_km(48.8566, 2.3522, 51.5074, -0.1278) < 348


def test_finds_two_hotspots_and_ignores_noise():
    random.seed(0)
    points = _cluster(41.88, -87.63, 20) + _cluster(41.80, -87.70, 8, category="assault")
    # bruit : 10 crimes très éloignés les uns des autres
    points += [{"latitude": 41.70 + i * 0.03, "longitude": -87.90 + i * 0.03,
                "crime_category": "other"} for i in range(10)]
    hotspots = find_hotspots(points, eps_km=0.5, min_samples=5)
    assert len(hotspots) == 2
    assert hotspots[0]["crime_count"] == 20          # le plus gros en premier
    assert hotspots[1]["top_category"] == "assault"


def test_not_enough_points():
    assert find_hotspots(_cluster(41.88, -87.63, 3), min_samples=5) == []


def test_build_alerts_fields():
    random.seed(1)
    alerts = build_alerts("chicago", datetime(2026, 11, 5, 18, 0), datetime(2026, 11, 5, 18, 30),
                          _cluster(41.88, -87.63, 16), eps_km=0.5, min_samples=5)
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert["severity"] == "high"  # 16 >= 3 x 5
    assert alert["window_start"] == "2026-11-05T18:00:00Z"
    assert alert["alert_id"].startswith("chicago|2026-11-05T18:00:00Z|")


def test_chain_effect_is_filtered_by_max_radius():
    # 30 crimes alignés tous les 300 m sur ~9 km : chacun touche son voisin,
    # donc DBSCAN les fusionne en UN groupe géant (effet de chaîne).
    chain = [{"latitude": 34.00 + i * 0.0027, "longitude": -118.30, "crime_category": "theft"}
             for i in range(30)]
    giant = find_hotspots(chain, eps_km=0.5, min_samples=3)
    assert len(giant) == 1 and giant[0]["radius_km"] > 3
    # Avec la limite de 1 km, ce faux hotspot est ignoré.
    assert find_hotspots(chain, eps_km=0.5, min_samples=3, max_radius_km=1.0) == []
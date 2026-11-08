"""Phase 3 : scénarios et top-3 de ml/predict_zones.py (sans MLflow ni modèle)."""
import numpy as np
import pandas as pd

from ml.predict_zones import build_scenarios, top_k_long


def small_gold() -> pd.DataFrame:
    rows = []
    for i, zone in enumerate(["chicago_1_1", "chicago_1_2", "chicago_1_3"]):
        for h in range(0, 24, 3):
            for _ in range(i + 1):          # zone 3 = la plus touchée
                rows.append({"city": "chicago", "zone_id": zone, "zone_lat": 1.0, "zone_lon": float(i),
                             "occurred_hour": h, "temperature_c": 10.0 + h, "wind_speed_kmh": 5.0,
                             "is_night": int(h < 7 or h >= 19), "crime_category": "theft"})
    return pd.DataFrame(rows)


def test_scenarios_keep_top_zones_and_cover_24h_and_2_day_types():
    scen = build_scenarios(small_gold(), top_zones=2)
    assert set(scen["zone_id"]) == {"chicago_1_3", "chicago_1_2"}
    assert len(scen) == 2 * 24 * 2
    assert scen["temperature_c"].notna().all()           # heures jamais vues : médiane de la ville
    assert set(scen["day_type"]) == {"semaine", "week-end"}
    assert (scen.loc[scen["day_type"] == "week-end", "is_weekend"] == 1).all()


def test_top_k_long_orders_probabilities():
    scen = pd.DataFrame({"city": ["chicago"], "zone_id": ["z"], "day_type": ["semaine"], "occurred_hour": [20]})
    proba = np.array([[0.1, 0.5, 0.15, 0.25]])
    out = top_k_long(scen, proba, ["a", "b", "c", "d"], k=3)
    assert list(out["crime_category"]) == ["b", "d", "c"]
    assert list(out["prediction_rank"]) == [1, 2, 3]
    assert out["probability"].is_monotonic_decreasing

"""
Phase 3 : génère un PETIT Gold synthétique (pandas, sans Spark) pour tester dbt.

Pourquoi ?
  Le vrai Gold est écrit par Airflow (Spark) et n'est pas dans GitHub (trop lourd).
  La CI et toute personne sans Docker ont quand même besoin de tables Gold pour lancer
  `dbt build`. Ce script écrit les 3 tables Gold avec EXACTEMENT les mêmes colonnes,
  types et partitions (`city=...`) que batch/silver_to_gold.py :

    <sortie>/gold/crime_features/city=.../*.parquet
    <sortie>/gold/zone_hourly/city=.../*.parquet
    <sortie>/gold/zone_stats/city=.../*.parquet
    <sortie>/silver/weather_hourly/city=.../*.parquet   (pour mesurer l'effet de la météo)

  Les crimes sont inventés (positions autour du centre de chaque ville, quelques zones
  plus chargées), mais respectent les règles du contrat (docs/CONTRAT_DONNEES.md) :
  Londres sans heure fiable, crimes à 00:00 = heure inconnue, météo hourly/daily...

Lancement :
  python scripts/generate_gold_fixture.py                         -> data/fixture/gold
  python scripts/generate_gold_fixture.py --out data/fixture --crimes 3000
  puis :  cd dbt && CRIMEWATCH_LAKE=../data/fixture dbt build
"""
import argparse
import os
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.schemas import CATEGORIES  # noqa: E402

GRID_DEG = 0.005   # même grille que batch/silver_to_gold.py

# centre approximatif de chaque ville + part des crimes (NYC domine, comme les vraies données)
CITIES = {
    "chicago":     (41.8781, -87.6298, 0.20),
    "los_angeles": (34.0522, -118.2437, 0.20),
    "new_york":    (40.7580, -73.9855, 0.45),
    "london":      (51.5130, -0.1340, 0.15),
}


def _zone(city: str, lat: np.ndarray, lon: np.ndarray):
    row = np.floor(lat / GRID_DEG).astype("int64")
    col = np.floor(lon / GRID_DEG).astype("int64")
    zone_id = [f"{city}_{r}_{c}" for r, c in zip(row, col)]
    return zone_id, np.round((row + 0.5) * GRID_DEG, 6), np.round((col + 0.5) * GRID_DEG, 6)


def build_crime_features(n_crimes: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    end = pd.Timestamp("2026-11-01")
    frames = []
    for city, (lat0, lon0, share) in CITIES.items():
        n = max(50, int(n_crimes * share))
        # 3 "hotspots" + du bruit autour du centre
        hot = rng.normal(0, 0.01, size=(3, 2))
        which = rng.integers(0, 4, n)                      # 3 = bruit
        lat = np.where(which < 3, lat0 + hot[np.minimum(which, 2), 0] + rng.normal(0, 0.002, n),
                       lat0 + rng.normal(0, 0.04, n))
        lon = np.where(which < 3, lon0 + hot[np.minimum(which, 2), 1] + rng.normal(0, 0.002, n),
                       lon0 + rng.normal(0, 0.05, n))

        days = rng.integers(0, 60, n)
        hours = rng.choice(24, n, p=_hour_profile())
        minutes = rng.integers(0, 60, n)
        ts = end - pd.to_timedelta(days, unit="D") + pd.to_timedelta(hours, unit="h") \
            + pd.to_timedelta(minutes, unit="m")
        ts = pd.Series(ts).dt.floor("min")
        if city == "london":                                   # UK Police : seulement le mois
            ts = ts.dt.to_period("M").dt.to_timestamp()
        else:                                                  # ~2 % à 00:00 pile = heure inconnue
            midnight = rng.random(n) < 0.02
            ts = ts.where(~midnight, ts.dt.normalize())

        hour_known = ((ts.dt.hour != 0) | (ts.dt.minute != 0)).astype("int32")
        if city == "london":
            hour_known[:] = 0

        cats = rng.choice(CATEGORIES, n, p=_category_profile())
        temp = np.round(rng.normal(12, 6, n), 1)
        precip = np.round(np.where(rng.random(n) < 0.25, rng.exponential(1.2, n), 0.0), 2)
        wind = np.round(np.abs(rng.normal(14, 6, n)), 1)
        is_night = np.where(hour_known == 1, ((ts.dt.hour < 7) | (ts.dt.hour >= 19)).astype(float), np.nan)

        zone_id, zone_lat, zone_lon = _zone(city, lat, lon)
        frames.append(pd.DataFrame({
            "event_id": [f"{city}:{seed}:{i}" for i in range(n)],
            "city": city,
            "crime_category": cats,
            "crime_type": [c.upper().replace("_", " ") for c in cats],
            "occurred_ts": ts.values,
            "occurred_date": ts.dt.date.values,
            "occurred_hour": ts.dt.hour.astype("int32").values,
            "hour_known": hour_known.values,
            # Spark dayofweek : 1 = dimanche ... 7 = samedi
            "day_of_week": ((ts.dt.dayofweek + 1) % 7 + 1).astype("int32").values,
            "month": ts.dt.month.astype("int32").values,
            "is_weekend": ts.dt.dayofweek.isin([5, 6]).astype("int32").values,
            "latitude": lat, "longitude": lon,
            "zone_id": zone_id, "zone_lat": zone_lat, "zone_lon": zone_lon,
            "temperature_c": temp, "precipitation_mm": precip, "wind_speed_kmh": wind,
            "is_raining": (precip > 0).astype("int32"),
            "is_night": pd.array(is_night, dtype="Int32"),
            "weather_level": np.where(hour_known == 1, "hourly", "daily"),
        }))
    return pd.concat(frames, ignore_index=True)


def _hour_profile() -> np.ndarray:
    p = np.array([3, 2, 2, 1, 1, 1, 2, 3, 4, 5, 5, 6, 6, 6, 6, 6, 7, 7, 7, 7, 6, 5, 4, 3], float)
    return p / p.sum()


def _category_profile() -> np.ndarray:
    p = np.ones(len(CATEGORIES))
    for i, c in enumerate(CATEGORIES):
        p[i] = {"theft": 8, "assault": 5, "harassment": 4, "vandalism": 3, "burglary": 2}.get(c, 1)
    return p / p.sum()


def build_zone_hourly(features: pd.DataFrame) -> pd.DataFrame:
    known = features[features["hour_known"] == 1]
    return (known.groupby(["city", "zone_id", "zone_lat", "zone_lon", "occurred_date", "occurred_hour"])
            .size().rename("crime_count").reset_index())


def build_zone_stats(features: pd.DataFrame) -> pd.DataFrame:
    per_cat = (features.groupby(["city", "zone_id", "crime_category"]).size().rename("n").reset_index()
               .sort_values(["city", "zone_id", "n", "crime_category"], ascending=[True, True, False, True]))
    top = per_cat.drop_duplicates(["city", "zone_id"])[["city", "zone_id", "crime_category"]] \
        .rename(columns={"crime_category": "top_category"})
    zones = (features.groupby(["city", "zone_id", "zone_lat", "zone_lon"])
             .agg(crime_count=("event_id", "size"),
                  active_days=("occurred_date", "nunique"),
                  first_date=("occurred_date", "min"),
                  last_date=("occurred_date", "max"))
             .reset_index().merge(top, on=["city", "zone_id"]))
    city_total = zones.groupby("city")["crime_count"].transform("sum")
    zones["share_of_city_pct"] = (100 * zones["crime_count"] / city_total).round(3)
    zones = zones.sort_values(["city", "crime_count", "zone_id"], ascending=[True, False, True])
    zones["rank_in_city"] = zones.groupby("city").cumcount().astype("int32") + 1
    zones["crime_count"] = zones["crime_count"].astype("int64")
    zones["active_days"] = zones["active_days"].astype("int64")
    return zones


def build_weather_hourly(features: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """silver/weather_hourly : une ligne par ville et par heure (heure locale) sur les dates des crimes."""
    rng = np.random.default_rng(seed + 1)
    frames = []
    for city, part in features.groupby("city"):
        start = pd.Timestamp(min(part["occurred_date"]))
        end = pd.Timestamp(max(part["occurred_date"])) + pd.Timedelta(hours=23)
        ts = pd.date_range(start, end, freq="h")
        n = len(ts)
        rain = np.round(np.where(rng.random(n) < 0.15, rng.exponential(1.0, n), 0.0), 2)
        frames.append(pd.DataFrame({
            "city": city,
            "weather_ts": ts,
            "temperature_c": np.round(12 + 6 * np.sin((ts.hour - 9) / 24 * 2 * np.pi) + rng.normal(0, 2, n), 1),
            "precipitation_mm": rain,
            "rain_mm": rain,
            "wind_speed_kmh": np.round(np.abs(rng.normal(14, 6, n)), 1),
            "is_day": ((ts.hour >= 7) & (ts.hour < 19)).astype("int64"),
        }))
    return pd.concat(frames, ignore_index=True)


def write_partitioned(df: pd.DataFrame, path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    for city, part in df.groupby("city"):
        target = path / f"city={city}"
        target.mkdir(parents=True, exist_ok=True)
        part.drop(columns="city").to_parquet(target / "part-0.parquet", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="data/fixture", help="dossier du 'lake' de test (défaut : data/fixture)")
    parser.add_argument("--crimes", type=int, default=2000, help="nombre approximatif de crimes")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    features = build_crime_features(args.crimes, args.seed)
    gold = Path(args.out) / "gold"
    write_partitioned(features, gold / "crime_features")
    write_partitioned(build_zone_hourly(features), gold / "zone_hourly")
    write_partitioned(build_zone_stats(features), gold / "zone_stats")
    write_partitioned(build_weather_hourly(features, args.seed), Path(args.out) / "silver" / "weather_hourly")
    print(f"Gold de test écrit dans {gold} : {len(features)} crimes, "
          f"{features['zone_id'].nunique()} zones, villes : {', '.join(CITIES)}")


if __name__ == "__main__":
    main()

"""
Contrôles qualité des données (utilisés par Airflow entre deux étapes).

Si un contrôle échoue, le script s'arrête avec une erreur -> Airflow marque la tâche
en rouge et NE lance PAS les étapes suivantes. On évite ainsi d'entraîner un modèle
sur des données cassées.

Lancement :
  python -m batch.quality_checks silver
  python -m batch.quality_checks gold
"""
from __future__ import annotations

import logging
import os
import sys

import pandas as pd

from common.config import CITIES
from common.schemas import CATEGORIES

log = logging.getLogger("quality_checks")

LAKE = os.getenv("LAKE_PATH", "data/lake")
MAX_WEATHER_MISSING_PCT = 5.0   # au-delà, la jointure météo est considérée comme cassée


def check_silver(crimes: pd.DataFrame) -> list[str]:
    """Renvoie la liste des problèmes trouvés (vide = tout va bien)."""
    errors = []
    if crimes.empty:
        return ["Silver crimes est vide"]

    duplicates = int(crimes["event_id"].duplicated().sum())
    if duplicates:
        errors.append(f"{duplicates} event_id en double")

    for column in ["event_id", "city", "latitude", "longitude", "occurred_ts", "crime_category"]:
        missing = int(crimes[column].isna().sum())
        if missing:
            errors.append(f"{missing} valeurs vides dans {column}")

    unknown = sorted(set(crimes["crime_category"].dropna()) - set(CATEGORIES))
    if unknown:
        errors.append(f"catégories inconnues : {unknown}")

    outside = 0
    for city, part in crimes.groupby(crimes["city"].astype(str)):
        box = CITIES.get(city)
        if box is None:
            errors.append(f"ville inconnue : {city}")
            continue
        inside = (part["latitude"].between(box.min_lat, box.max_lat)
                  & part["longitude"].between(box.min_lon, box.max_lon))
        outside += int((~inside).sum())
    if outside:
        errors.append(f"{outside} crimes hors de leur ville")

    if not crimes["hour_known"].isin([0, 1]).all():
        errors.append("hour_known contient autre chose que 0 et 1")
    london = crimes[crimes["city"].astype(str) == "london"]
    if len(london) and (london["hour_known"] != 0).any():
        errors.append("Londres doit avoir hour_known = 0 (l'API ne donne que le mois)")
    return errors


def check_gold(features: pd.DataFrame, silver_count: int) -> list[str]:
    errors = []
    if len(features) != silver_count:
        errors.append(f"crime_features a {len(features)} lignes, Silver en a {silver_count} "
                      "(la jointure météo a créé ou perdu des lignes)")
    missing = 100 * (features["weather_level"] == "none").mean() if len(features) else 100.0
    if missing > MAX_WEATHER_MISSING_PCT:
        errors.append(f"{missing:.1f} % des crimes sans météo (max {MAX_WEATHER_MISSING_PCT} %)")
    return errors


def main(layer: str, lake: str = LAKE) -> int:
    crimes = pd.read_parquet(f"{lake}/silver/crimes")
    if layer == "silver":
        errors = check_silver(crimes)
    elif layer == "gold":
        features = pd.read_parquet(f"{lake}/gold/crime_features", columns=["event_id", "weather_level"])
        errors = check_gold(features, len(crimes))
    else:
        raise SystemExit("Usage : python -m batch.quality_checks [silver|gold]")

    if errors:
        for error in errors:
            log.error("Contrôle %s ÉCHOUÉ : %s", layer, error)
        return 1
    log.info("Contrôle %s OK (%d crimes)", layer, len(crimes))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else ""))
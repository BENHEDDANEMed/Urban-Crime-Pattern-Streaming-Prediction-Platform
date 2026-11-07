"""
Phase 2 (S3) : météo HISTORIQUE heure par heure -> Silver

Problème : le producer récupère la météo ACTUELLE, mais les crimes datent
d'une semaine à plusieurs mois (retard des APIs publiques). On ne peut donc
pas joindre un crime à la météo du moment où il a eu lieu.

Solution : l'API d'archives d'Open-Meteo (gratuite, sans clé) donne la météo
heure par heure pour n'importe quelle date passée.
  https://archive-api.open-meteo.com/v1/archive
Les données ont environ 5 jours de retard : on s'arrête donc à "aujourd'hui - 6 jours".

Ce que fait ce script :
  1. lit Silver crimes et trouve, pour chaque ville, la période couverte (date min / date max)
  2. appelle l'API d'archives une fois par ville sur cette période
  3. écrit data/lake/silver/weather_hourly/city=.../ (une ligne par ville et par heure)

L'heure est l'heure LOCALE de la ville (comme occurred_ts dans Silver crimes),
pour que la jointure se fasse directement.

Lancement (depuis la racine du projet, venv activé) :
  python -m batch.weather_history
"""
from __future__ import annotations

import logging
import os
import shutil
from datetime import date, timedelta

import pandas as pd

from common import config
from producer.http_client import get_json

log = logging.getLogger("weather_history")

ARCHIVE_URL = os.getenv("OPEN_METEO_ARCHIVE_URL", "https://archive-api.open-meteo.com/v1/archive")
LAKE = os.getenv("LAKE_PATH", "data/lake")
ARCHIVE_DELAY_DAYS = 6          # l'archive a ~5 jours de retard, on garde 1 jour de marge
HOURLY_VARIABLES = "temperature_2m,precipitation,rain,wind_speed_10m,is_day"

# Noms de l'API -> noms du projet (les mêmes que dans Silver météo)
COLUMNS = {
    "temperature_2m": "temperature_c",
    "precipitation": "precipitation_mm",
    "rain": "rain_mm",
    "wind_speed_10m": "wind_speed_kmh",
    "is_day": "is_day",
}


def crime_date_ranges(lake: str = LAKE) -> dict[str, tuple[date, date]]:
    """Pour chaque ville : (première date de crime, dernière date de crime) dans Silver."""
    crimes = pd.read_parquet(f"{lake}/silver/crimes", columns=["city", "occurred_date"])
    crimes["occurred_date"] = pd.to_datetime(crimes["occurred_date"]).dt.date
    ranges = crimes.groupby("city", observed=True)["occurred_date"].agg(["min", "max"])
    return {str(city): (row["min"], row["max"]) for city, row in ranges.iterrows()}


def clip_to_archive(start: date, end: date, today: date | None = None) -> tuple[date, date] | None:
    """On ne peut pas demander des jours plus récents que (aujourd'hui - 6 jours)."""
    last_available = (today or date.today()) - timedelta(days=ARCHIVE_DELAY_DAYS)
    end = min(end, last_available)
    if start > end:
        return None
    return start, end


def parse_hourly(city: str, payload: dict) -> pd.DataFrame:
    """
    Réponse de l'API :
      {"hourly": {"time": ["2026-10-01T00:00", ...], "temperature_2m": [8.1, ...], ...}}
    -> un DataFrame : city | weather_ts | temperature_c | precipitation_mm | ...
    """
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    df = pd.DataFrame({"weather_ts": pd.to_datetime(times)})
    for api_name, our_name in COLUMNS.items():
        values = hourly.get(api_name)
        df[our_name] = values if values is not None else [None] * len(times)
    df.insert(0, "city", city)
    # Une heure sans aucune mesure ne sert à rien
    return df.dropna(subset=["temperature_c", "precipitation_mm"], how="all")


def fetch_city(city: str, start: date, end: date) -> pd.DataFrame:
    c = config.CITIES[city]
    payload = get_json(ARCHIVE_URL, params={
        "latitude": c.latitude,
        "longitude": c.longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": HOURLY_VARIABLES,
        "timezone": c.timezone,   # heures renvoyées en heure locale de la ville
    })
    return parse_hourly(city, payload)


def run(lake: str = LAKE) -> pd.DataFrame:
    frames = []
    for city, (start, end) in sorted(crime_date_ranges(lake).items()):
        period = clip_to_archive(start, end)
        if period is None:
            log.warning("[%s] crimes trop récents pour l'archive (%s -> %s) : ignoré", city, start, end)
            continue
        try:
            df = fetch_city(city, *period)
        except Exception as exc:  # noqa: BLE001  (une ville en panne ne bloque pas les autres)
            log.error("[%s] Erreur API archive : %s", city, exc)
            continue
        log.info("[%s] %s -> %s : %d heures de météo", city, period[0], period[1], len(df))
        frames.append(df)

    if not frames:
        raise SystemExit("Aucune météo récupérée.")
    weather = pd.concat(frames, ignore_index=True)

    out = f"{lake}/silver/weather_hourly"
    shutil.rmtree(out, ignore_errors=True)       # on réécrit tout : relancer = même résultat
    weather.to_parquet(out, partition_cols=["city"], index=False)
    log.info("Silver weather_hourly : %d lignes -> %s", len(weather), out)
    return weather


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    run()
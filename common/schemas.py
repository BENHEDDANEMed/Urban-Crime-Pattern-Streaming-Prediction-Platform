"""
Le "contrat de données" : la forme EXACTE des messages envoyés dans Kafka.

Pourquoi c'est important ?
Chaque ville a ses propres noms de colonnes (Chicago dit "primary_type",
New York dit "ofns_desc"...). On transforme tout dans UN SEUL format commun.
Comme ça, Spark (S2) et les personnes des phases suivantes n'ont qu'un seul
format à connaître. Voir aussi docs/CONTRAT_DONNEES.md.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from common.config import CITIES

SCHEMA_VERSION = 1

# ---------------------------------------------------------------------------
# Catégories communes
# ---------------------------------------------------------------------------
# Chaque ville nomme les crimes différemment. On les range dans des catégories
# communes pour pouvoir comparer les villes (et pour le ML en phase 2).
# L'ORDRE compte : la première catégorie qui correspond gagne.
# Exemple : "CRIMINAL SEXUAL ASSAULT" doit aller dans "sexual" et pas "assault".
CATEGORY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("vehicle_theft", ["MOTOR VEHICLE THEFT", "VEHICLE STOLEN", "VEHICLE   STOLEN",
                                   "OF MOTOR VEHICLE", "VEHICLE CRIME", "GRAND THEFT AUTO"]),
    ("robbery", ["ROBBERY"]),
    ("burglary", ["BURGLARY"]),
    ("sexual", ["SEX", "RAPE"]),
    ("assault", ["ASSAULT", "BATTERY", "HOMICIDE", "MURDER", "VIOLENT", "VIOLENCE"]),
    ("drugs", ["NARCOTIC", "DRUG", "CONTROLLED SUBSTANCE"]),
    ("weapons", ["WEAPON"]),
    ("vandalism", ["CRIMINAL DAMAGE", "VANDALISM", "MISCHIEF", "ARSON"]),
    ("theft", ["THEFT", "LARCENY", "SHOPLIFTING", "STOLEN", "PICKPOCKET", "PURSE SNATCHING"]),
    ("public_order", ["ANTI SOCIAL", "PUBLIC ORDER", "DISORDERLY", "HARASS", "HARRASS",
                      "PUBLIC PEACE"]),
]
CATEGORIES = [name for name, _ in CATEGORY_KEYWORDS] + ["other"]


def categorize(label: Optional[str]) -> str:
    """Transforme un libellé de crime (ex: 'PETIT LARCENY') en catégorie commune ('theft')."""
    if not label:
        return "other"
    text = label.upper().replace("-", " ").replace("_", " ")
    for category, keywords in CATEGORY_KEYWORDS:
        if any(keyword in text for keyword in keywords):
            return category
    return "other"


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------
def now_utc_iso() -> str:
    """Heure actuelle en UTC, format '2026-11-05T18:00:00Z'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%f",  # Socrata : 2025-10-28T14:30:00.000
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",        # Open-Meteo : 2026-11-05T18:00
    "%Y-%m-%d",
    "%Y-%m",                 # UK Police : seulement le mois, ex 2025-09
    "%m/%d/%Y %I:%M:%S %p",  # anciens exports CSV
    "%m/%d/%Y",
]


def parse_datetime(value: Any) -> Optional[datetime]:
    """Essaie plusieurs formats de date. Renvoie None si rien ne marche."""
    if value is None:
        return None
    text = str(value).strip().replace("Z", "")
    if not text:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def to_iso(dt: Optional[datetime]) -> Optional[str]:
    """datetime -> texte '2025-10-28T14:30:00' (heure locale de la ville, sans fuseau)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S") if dt else None


def to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Construction des messages
# ---------------------------------------------------------------------------
def build_crime_event(
    *,
    city: str,
    source: str,
    source_id: Any,
    crime_type: Optional[str],
    description: Optional[str],
    latitude: Any,
    longitude: Any,
    occurred_at: Optional[datetime],
    raw: dict,
    ingest_mode: str = "api",
) -> Optional[dict]:
    """
    Crée un message "crime" au format commun.
    Renvoie None si le crime n'est pas utilisable (pas d'id, pas de coordonnées,
    coordonnées hors de la ville, pas de date). On préfère jeter une ligne
    plutôt que d'envoyer une donnée fausse.
    """
    city_info = CITIES[city]
    lat, lon = to_float(latitude), to_float(longitude)
    if source_id in (None, ""):
        return None
    if lat is None or lon is None or not city_info.contains(lat, lon):
        return None
    if occurred_at is None:
        return None

    crime_type_clean = (crime_type or "UNKNOWN").strip().upper()
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{city}-{source_id}",
        "source_id": str(source_id),
        "city": city,
        "source": source,
        "crime_type": crime_type_clean,
        "crime_category": categorize(crime_type_clean),
        "description": (description or "").strip() or None,
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "occurred_at": to_iso(occurred_at),
        "ingested_at": now_utc_iso(),
        "ingest_mode": ingest_mode,
        "raw": raw,
    }


def build_weather_event(*, city: str, payload: dict) -> Optional[dict]:
    """Crée un message "météo" au format commun à partir de la réponse Open-Meteo."""
    current = payload.get("current") or {}
    if not current:
        return None
    observed = parse_datetime(current.get("time"))
    return {
        "schema_version": SCHEMA_VERSION,
        "city": city,
        "source": "open_meteo",
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "observed_at": to_iso(observed),
        "temperature_c": to_float(current.get("temperature_2m")),
        "precipitation_mm": to_float(current.get("precipitation")),
        "rain_mm": to_float(current.get("rain")),
        "wind_speed_kmh": to_float(current.get("wind_speed_10m")),
        "is_day": current.get("is_day"),
        "ingested_at": now_utc_iso(),
        "raw": payload,
    }
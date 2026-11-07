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
# Exemples : "CRIMINAL SEXUAL ASSAULT" doit aller dans "sexual" et pas "assault",
#            "IDENTITY THEFT" dans "fraud" et pas "theft",
#            "DUI DRUG" dans "traffic" et pas "drugs".
#
# Version 2 (phase 2) : mesuré sur 176 000 vrais crimes, 15 à 24 % tombaient dans "other",
# et le harcèlement (22 000 crimes à New York) était compté dans "public_order".
# -> nouvelles catégories : harassment, fraud, trespass, traffic, justice.
CATEGORY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("vehicle_theft", ["MOTOR VEHICLE THEFT", "VEHICLE STOLEN", "VEHICLE   STOLEN",
                                   "OF MOTOR VEHICLE", "VEHICLE CRIME", "GRAND THEFT AUTO"]),
    ("traffic", ["VEHICLE AND TRAFFIC", "TRAFFIC LAW", "TRAFFIC VIOLATION", "IMPAIRED DRIVING",
                 " DUI ", "DRIVING UNDER THE INFLUENCE", "RECKLESS DRIVING"]),
    ("robbery", ["ROBBERY"]),
    ("burglary", ["BURGLARY"]),
    ("sexual", ["SEX", "RAPE"]),
    ("harassment", ["HARASS", "HARRASS", "STALKING", "INTIMIDATION", "THREAT"]),
    ("assault", ["ASSAULT", "BATTERY", "HOMICIDE", "MURDER", "VIOLENT", "VIOLENCE"]),
    ("drugs", ["NARCOTIC", "DRUG", "CONTROLLED SUBSTANCE", "PARAPHERNALIA"]),
    ("weapons", ["WEAPON", "CONCEALED CARRY"]),
    ("vandalism", ["CRIMINAL DAMAGE", "VANDALISM", "MISCHIEF", "ARSON"]),
    ("fraud", ["DECEPTIVE PRACTICE", "FORGERY", "FRAUD", "EMBEZZLE", "COUNTERFEIT", "IDENTITY THEFT"]),
    ("theft", ["THEFT", "LARCENY", "SHOPLIFTING", "STOLEN", "PICKPOCKET", "PURSE SNATCHING"]),
    ("trespass", ["TRESPASS"]),
    ("justice", ["WARRANT", "CONTEMPT", "PAROLE", "PROBATION", "PUBLIC ADMINI", "PUBLIC OFFICER",
                 "RESISTING", "OBSTRUCT"]),
    ("public_order", ["ANTI SOCIAL", "PUBLIC ORDER", "DISORDERLY", "PUBLIC PEACE", "PUB ORD",
                      "PROSTITUTION", "LIQUOR", "GAMBLING"]),
]
CATEGORIES = [name for name, _ in CATEGORY_KEYWORDS] + ["other"]

# Villes dont la colonne "description" contient un sous-type du crime (et pas un lieu) :
# Chicago ("RETAIL THEFT", "TELEPHONE THREAT"...) et New York (pd_desc).
# À Los Angeles, description = type de lieu ; à Londres = nom de rue -> pas utilisable.
CITIES_WITH_DETAIL = {"chicago", "new_york"}


def categorize(label: Optional[str]) -> str:
    """Transforme un libellé de crime (ex: 'PETIT LARCENY') en catégorie commune ('theft')."""
    if not label:
        return "other"
    # Espaces autour du texte : permet de chercher un mot entier comme " DUI ".
    text = " " + label.upper().replace("-", " ").replace("_", " ") + " "
    for category, keywords in CATEGORY_KEYWORDS:
        if any(keyword in text for keyword in keywords):
            return category
    return "other"


def categorize_crime(crime_type: Optional[str], description: Optional[str] = None,
                     city: Optional[str] = None) -> str:
    """
    Comme categorize(), mais si le type principal donne "other" (ex: Chicago "OTHER OFFENSE"),
    on regarde le sous-type dans description (ex: "TELEPHONE THREAT" -> harassment).
    """
    category = categorize(crime_type)
    if category == "other" and city in CITIES_WITH_DETAIL:
        category = categorize(description)
    return category


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
        "crime_category": categorize_crime(crime_type_clean, description, city),
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
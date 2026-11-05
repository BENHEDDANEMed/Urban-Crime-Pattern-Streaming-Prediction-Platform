"""
Configuration du projet.

Idée simple : tout ce qui peut changer (adresse de Kafka, liste des villes,
URL des APIs...) est écrit ICI, et nulle part ailleurs.
Les valeurs peuvent être remplacées par des variables d'environnement
(fichier .env à la racine du projet).
"""
import os
from dataclasses import dataclass

# On charge le fichier .env s'il existe (pratique quand on lance depuis VS Code).
# Si la librairie python-dotenv n'est pas installée, on continue sans elle.
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def _env(name: str, default: str) -> str:
    """Lit une variable d'environnement, ou renvoie la valeur par défaut."""
    value = os.getenv(name)
    return value if value not in (None, "") else default


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
# Depuis ton PC (VS Code)       -> localhost:9092
# Depuis un conteneur Docker    -> kafka:29092 (défini dans docker-compose.yml)
KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_CRIMES_RAW = _env("TOPIC_CRIMES_RAW", "crimes.raw")
TOPIC_WEATHER_RAW = _env("TOPIC_WEATHER_RAW", "weather.raw")
TOPIC_CRIMES_ALERTS = _env("TOPIC_CRIMES_ALERTS", "crimes.alerts")

# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------
# Temps d'attente entre deux appels aux APIs (en secondes).
POLL_INTERVAL_SECONDS = int(_env("POLL_INTERVAL_SECONDS", "300"))
# Nombre maximum de crimes récupérés par ville à chaque appel.
MAX_RECORDS_PER_CITY = int(_env("MAX_RECORDS_PER_CITY", "2000"))
# Taille d'une page Socrata (Chicago / LA / NYC limitent à 1000 lignes par appel).
SOCRATA_PAGE_SIZE = 1000
# Optionnel : un "app token" Socrata gratuit augmente les quotas.
SOCRATA_APP_TOKEN = _env("SOCRATA_APP_TOKEN", "")
# Si tu mets une valeur ici, elle remplace la valeur par défaut de chaque ville.
LOOKBACK_DAYS_OVERRIDE = os.getenv("LOOKBACK_DAYS")
# Fichier où le producer note "jusqu'où il a déjà lu" pour chaque ville.
CHECKPOINT_FILE = _env("CHECKPOINT_FILE", "data/checkpoints/producer_state.json")
# Dossier des échantillons de données (mode replay + option --save-sample).
SAMPLE_DIR = _env("SAMPLE_DIR", "data/sample")

# ---------------------------------------------------------------------------
# URLs des APIs (toutes gratuites, sans clé)
# ---------------------------------------------------------------------------
CHICAGO_URL = _env("CHICAGO_URL", "https://data.cityofchicago.org/resource/ijzp-q8t2.json")
# LAPD a changé de système (NIBRS). Depuis août 2026, tout est dans k7nn-b2ep.
LA_URL = _env("LA_URL", "https://data.lacity.org/resource/k7nn-b2ep.json")
NYC_URL = _env("NYC_URL", "https://data.cityofnewyork.us/resource/5uac-w243.json")
UK_POLICE_URL = _env("UK_POLICE_URL", "https://data.police.uk/api")
OPEN_METEO_URL = _env("OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast")


# ---------------------------------------------------------------------------
# Les villes suivies
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class City:
    key: str            # identifiant court utilisé partout (clé Kafka, dossiers...)
    name: str           # nom lisible
    latitude: float     # centre de la ville (pour la météo et UK Police)
    longitude: float
    timezone: str
    # "Boîte" autour de la ville : un crime en dehors est considéré comme invalide
    # (ex : coordonnées 0,0 ou erreurs de saisie).
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float

    def contains(self, lat: float, lon: float) -> bool:
        return self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon


CITIES = {
    "chicago": City("chicago", "Chicago", 41.8781, -87.6298, "America/Chicago",
                    41.60, 42.10, -87.95, -87.50),
    "los_angeles": City("los_angeles", "Los Angeles", 34.0522, -118.2437, "America/Los_Angeles",
                        33.60, 34.40, -118.70, -118.10),
    "new_york": City("new_york", "New York", 40.7128, -74.0060, "America/New_York",
                     40.45, 40.95, -74.30, -73.65),
    "london": City("london", "London (UK Police)", 51.5074, -0.1278, "Europe/London",
                   51.20, 51.80, -0.60, 0.40),
}
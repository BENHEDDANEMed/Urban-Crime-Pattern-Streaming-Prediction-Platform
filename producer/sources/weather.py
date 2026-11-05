"""
Open-Meteo — météo actuelle pour chaque ville (gratuit, sans clé).
Un appel par ville et par tour de boucle suffit.

Réponse (simplifiée) :
{"latitude": 41.88, "longitude": -87.63,
 "current": {"time": "2026-11-05T12:00", "temperature_2m": 8.4, "precipitation": 0.0,
             "rain": 0.0, "wind_speed_10m": 14.2, "is_day": 1}}
"""
from common import config
from common.schemas import build_weather_event
from producer.http_client import get_json

CURRENT_VARIABLES = "temperature_2m,precipitation,rain,wind_speed_10m,is_day"


def fetch_weather(city_key: str):
    city = config.CITIES[city_key]
    payload = get_json(config.OPEN_METEO_URL, params={
        "latitude": city.latitude,
        "longitude": city.longitude,
        "current": CURRENT_VARIABLES,
        "timezone": city.timezone,  # l'heure renvoyée est l'heure locale de la ville
    })
    return build_weather_event(city=city_key, payload=payload)
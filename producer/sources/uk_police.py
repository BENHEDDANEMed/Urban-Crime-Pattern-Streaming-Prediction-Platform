"""
UK Police API (data.police.uk) — on suit le centre de Londres.

Différence importante : cette API donne les crimes PAR MOIS (pas d'heure exacte),
et un nouveau mois est publié environ 1 fois par mois.
Donc :
  1. on demande quel est le dernier mois disponible  (/crimes-street-dates)
  2. si on ne l'a pas encore envoyé, on récupère les crimes de ce mois
     dans un rayon de 1 mile autour du centre (/crimes-street/all-crime)

Exemple de ligne brute (simplifiée) :
{"id": 123456789, "category": "anti-social-behaviour", "month": "2026-09",
 "location": {"latitude": "51.5098", "longitude": "-0.1342",
              "street": {"name": "On or near Regent Street"}}}
"""
import logging

from common import config
from common.schemas import build_crime_event, parse_datetime
from producer.http_client import get_json
from producer.sources.base import CrimeSource

log = logging.getLogger(__name__)


class UKPoliceSource(CrimeSource):
    city = "london"
    source = "uk_police_api"

    def __init__(self):
        self._month_being_fetched = None

    def latest_month(self) -> str:
        dates = get_json(f"{config.UK_POLICE_URL}/crimes-street-dates")
        return max(item["date"] for item in dates)  # ex: "2026-09"

    def fetch_raw(self, checkpoint: dict) -> list[dict]:
        month = self.latest_month()
        if checkpoint.get("month") and checkpoint["month"] >= month:
            log.info("[london] Le mois %s est déjà envoyé, rien de nouveau.", month)
            return []
        city = config.CITIES[self.city]
        self._month_being_fetched = month
        return get_json(
            f"{config.UK_POLICE_URL}/crimes-street/all-crime",
            params={"lat": city.latitude, "lng": city.longitude, "date": month},
        )

    def next_checkpoint(self, checkpoint: dict, raw_records: list[dict]) -> dict:
        return {"month": self._month_being_fetched}

    def normalize(self, record: dict):
        location = record.get("location") or {}
        street = (location.get("street") or {}).get("name")
        return build_crime_event(
            city=self.city,
            source=self.source,
            source_id=record.get("id") or record.get("persistent_id"),
            crime_type=record.get("category"),
            description=street,
            latitude=location.get("latitude"),
            longitude=location.get("longitude"),
            occurred_at=parse_datetime(record.get("month")),  # 1er jour du mois, 00:00
            raw=record,
        )
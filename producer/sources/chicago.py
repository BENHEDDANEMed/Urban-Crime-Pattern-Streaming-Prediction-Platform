"""
Chicago Data Portal — dataset "Crimes - 2001 to Present" (ijzp-q8t2).
Mis à jour chaque jour, avec environ 7 jours de retard.

Exemple de ligne brute (simplifiée) :
{"id": "13245678", "date": "2026-10-28T14:30:00.000", "primary_type": "THEFT",
 "description": "RETAIL THEFT", "latitude": "41.88", "longitude": "-87.63", ...}
"""
from common import config
from common.schemas import build_crime_event, parse_datetime
from producer.sources.base import SocrataSource


class ChicagoSource(SocrataSource):
    city = "chicago"
    source = "chicago_data_portal"
    url = config.CHICAGO_URL
    date_field = "date"
    default_lookback_days = 30

    def record_id(self, record: dict):
        return record.get("id")

    def normalize(self, record: dict):
        return build_crime_event(
            city=self.city,
            source=self.source,
            source_id=self.record_id(record),
            crime_type=record.get("primary_type"),
            description=record.get("description"),
            latitude=record.get("latitude"),
            longitude=record.get("longitude"),
            occurred_at=parse_datetime(record.get("date")),
            raw=record,
        )
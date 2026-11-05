"""
NYC Open Data — "NYPD Complaint Data Current (Year To Date)" (5uac-w243).
ATTENTION : ce dataset est mis à jour seulement chaque trimestre.
C'est pour ça qu'on regarde 150 jours en arrière la première fois.

Exemple de ligne brute (simplifiée) :
{"cmplnt_num": "298765432", "cmplnt_fr_dt": "2026-08-15T00:00:00.000",
 "cmplnt_fr_tm": "14:30:00", "ofns_desc": "PETIT LARCENY",
 "pd_desc": "LARCENY,PETIT FROM STORE-SHOPL", "latitude": "40.75", "longitude": "-73.98"}
"""
from datetime import datetime

from common import config
from common.schemas import build_crime_event, parse_datetime
from producer.sources.base import SocrataSource


class NewYorkSource(SocrataSource):
    city = "new_york"
    source = "nyc_open_data"
    url = config.NYC_URL
    date_field = "cmplnt_fr_dt"
    default_lookback_days = 150

    def record_id(self, record: dict):
        return record.get("cmplnt_num")

    @staticmethod
    def occurred_at(record: dict):
        """La date et l'heure sont dans 2 colonnes séparées : on les combine."""
        day = parse_datetime(record.get("cmplnt_fr_dt"))
        if day is None:
            return None
        time_text = record.get("cmplnt_fr_tm") or "00:00:00"
        try:
            t = datetime.strptime(time_text[:8], "%H:%M:%S").time()
        except ValueError:
            return day
        return datetime.combine(day.date(), t)

    def normalize(self, record: dict):
        return build_crime_event(
            city=self.city,
            source=self.source,
            source_id=self.record_id(record),
            crime_type=record.get("ofns_desc"),
            description=record.get("pd_desc"),
            latitude=record.get("latitude"),
            longitude=record.get("longitude"),
            occurred_at=self.occurred_at(record),
            raw=record,
        )
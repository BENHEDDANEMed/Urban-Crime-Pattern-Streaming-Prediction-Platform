"""
LA Open Data — "LAPD NIBRS Offenses Dataset" (k7nn-b2ep).

Contexte : en 2024, la police de LA (LAPD) a changé de système (NIBRS).
L'ancien dataset "Crime Data from 2020 to Present" n'est plus mis à jour.
Le nouveau dataset est mis à jour toutes les 2 semaines environ.

Comme les noms de colonnes ont changé entre l'ancien et le nouveau dataset,
on accepte PLUSIEURS noms possibles pour chaque info (fonction first_value).
Vérifié le 05/11/2026 sur une vraie ligne : les coordonnées s'appellent
"hndrdth_lat" / "hndrdth_lon", le type "nibr_description", l'id "uniquenibrno".

Exemple de ligne brute (simplifiée) :
{"uniquenibrno": "26210493_240_0", "date_occ": "2026-10-17T00:00:00.000", "time_occ": "0000",
 "nibr_description": "487(D)(1) - PC - F - Grand Theft Auto - GTA  - 240",
 "premis_desc": "Street/Parkway", "hndrdth_lat": "34.2138", "hndrdth_lon": "-118.5448"}"""
import hashlib
import json
from datetime import datetime

from common import config
from common.schemas import build_crime_event, parse_datetime
from producer.sources.base import SocrataSource, first_value

ID_KEYS = ["uniquenibrno", "dr_no", "caseno", "case_number", "incident_id", "id"]
TYPE_KEYS = ["nibr_description", "nibrs_description", "offense_description",
             "crm_cd_desc", "ucr_description"]
DESCRIPTION_KEYS = ["premis_desc", "crime_against", "location_type", "area_name"]
# Nouveau dataset NIBRS : coordonnées du "bloc de 100 numéros" (hundredth) de la rue.
LAT_KEYS = ["hndrdth_lat", "lat", "latitude"]
LON_KEYS = ["hndrdth_lon", "lon", "lng", "longitude"]


class LosAngelesSource(SocrataSource):
    city = "los_angeles"
    source = "la_open_data"
    url = config.LA_URL
    date_field = "date_occ"
    default_lookback_days = 45

    def record_id(self, record: dict):
        value = first_value(record, ID_KEYS)
        if value is not None:
            return value
        # Pas d'identifiant connu : on fabrique un id stable à partir du contenu.
        text = json.dumps(record, sort_keys=True)
        return hashlib.md5(text.encode()).hexdigest()[:16]

    @staticmethod
    def occurred_at(record: dict):
        day = parse_datetime(record.get("date_occ"))
        if day is None:
            return None
        # Dans l'ancien format, l'heure est dans "time_occ" sous la forme "1430".
        time_occ = str(record.get("time_occ") or "").strip()
        if time_occ.isdigit() and day.hour == 0 and day.minute == 0:
            time_occ = time_occ.zfill(4)
            try:
                return datetime.combine(day.date(), datetime.strptime(time_occ, "%H%M").time())
            except ValueError:
                pass
        return day

    def normalize(self, record: dict):
        return build_crime_event(
            city=self.city,
            source=self.source,
            source_id=self.record_id(record),
            crime_type=first_value(record, TYPE_KEYS),
            description=first_value(record, DESCRIPTION_KEYS),
            latitude=first_value(record, LAT_KEYS),
            longitude=first_value(record, LON_KEYS),
            occurred_at=self.occurred_at(record),
            raw=record,
        )
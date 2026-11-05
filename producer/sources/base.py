"""
Classes de base pour les sources de crimes.

Chaque source (une ville) sait faire 3 choses :
  1. fetch_raw()        -> appeler l'API et récupérer les lignes brutes
  2. normalize()        -> transformer UNE ligne brute en message au format commun
  3. next_checkpoint()  -> calculer le nouveau "marque-page"

La méthode poll() enchaîne les 3 étapes. Le fichier main.py appelle seulement poll().
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

from common import config
from common.schemas import parse_datetime, to_iso
from producer.http_client import get_json

log = logging.getLogger(__name__)


class CrimeSource(ABC):
    city: str = ""    # ex: "chicago"
    source: str = ""  # ex: "chicago_data_portal"

    @abstractmethod
    def fetch_raw(self, checkpoint: dict) -> list[dict]:
        ...

    @abstractmethod
    def normalize(self, record: dict) -> dict | None:
        ...

    @abstractmethod
    def next_checkpoint(self, checkpoint: dict, raw_records: list[dict]) -> dict:
        ...

    def poll(self, checkpoint: dict) -> tuple[list[dict], dict]:
        """Récupère les nouveaux crimes. Renvoie (messages valides, nouveau checkpoint)."""
        raw_records = self.fetch_raw(checkpoint)
        events = []
        for record in raw_records:
            event = self.normalize(record)
            if event is not None:
                events.append(event)
        rejected = len(raw_records) - len(events)
        log.info("[%s] %d lignes reçues, %d valides, %d rejetées (coordonnées/date manquantes)",
                 self.city, len(raw_records), len(events), rejected)
        new_checkpoint = self.next_checkpoint(checkpoint, raw_records) if raw_records else checkpoint
        return events, new_checkpoint


class SocrataSource(CrimeSource):
    """
    Chicago, Los Angeles et New York utilisent la même plateforme : Socrata.
    On peut donc partager le code. Socrata comprend un mini-langage SQL (SoQL) :
      $where  = filtre (ex: date >= '2026-10-01T00:00:00')
      $order  = tri
      $limit  = nombre de lignes (max 1000 par appel)
      $offset = pour passer à la page suivante
    """

    url: str = ""
    date_field: str = ""            # nom de la colonne date dans l'API
    default_lookback_days: int = 30  # première exécution : combien de jours en arrière

    def record_id(self, record: dict) -> str | None:
        """Identifiant unique d'une ligne (différent pour chaque ville)."""
        raise NotImplementedError

    def record_datetime(self, record: dict) -> datetime | None:
        return parse_datetime(record.get(self.date_field))

    def lookback_days(self) -> int:
        if config.LOOKBACK_DAYS_OVERRIDE:
            return int(config.LOOKBACK_DAYS_OVERRIDE)
        return self.default_lookback_days

    def fetch_raw(self, checkpoint: dict) -> list[dict]:
        since = checkpoint.get("since")
        if not since:
            # Première fois : on part de "aujourd'hui - N jours".
            # (Les données publiques ont souvent quelques jours de retard.)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            since = to_iso(now - timedelta(days=self.lookback_days()))
        already_sent = set(checkpoint.get("ids_at_since", []))

        headers = {"X-App-Token": config.SOCRATA_APP_TOKEN} if config.SOCRATA_APP_TOKEN else None
        records: list[dict] = []
        offset = 0
        while len(records) < config.MAX_RECORDS_PER_CITY:
            params = {
                # ">=" et pas ">" : sinon on raterait les crimes qui ont exactement
                # la même heure que le dernier crime lu. Les doublons sont filtrés plus bas.
                "$where": f"{self.date_field} >= '{since}'",
                "$order": f"{self.date_field} ASC",
                "$limit": config.SOCRATA_PAGE_SIZE,
                "$offset": offset,
            }
            page = get_json(self.url, params=params, headers=headers)
            records.extend(page)
            if len(page) < config.SOCRATA_PAGE_SIZE:
                break  # dernière page
            offset += config.SOCRATA_PAGE_SIZE
        records = records[: config.MAX_RECORDS_PER_CITY]

        # On enlève les crimes déjà envoyés au tour précédent.
        since_dt = parse_datetime(since)
        return [
            r for r in records
            if not (self.record_datetime(r) == since_dt and str(self.record_id(r)) in already_sent)
        ]

    def next_checkpoint(self, checkpoint: dict, raw_records: list[dict]) -> dict:
        dated = [(self.record_datetime(r), r) for r in raw_records]
        dated = [(d, r) for d, r in dated if d is not None]
        if not dated:
            return checkpoint
        max_dt = max(d for d, _ in dated)
        ids = {str(self.record_id(r)) for d, r in dated if d == max_dt}
        # Si la date max n'a pas changé, on garde aussi les anciens ids.
        if checkpoint.get("since") and parse_datetime(checkpoint["since"]) == max_dt:
            ids |= set(checkpoint.get("ids_at_since", []))
        return {"since": to_iso(max_dt), "ids_at_since": sorted(ids)}


def first_value(record: dict, keys: list[str]):
    """Renvoie la valeur de la première clé qui existe. Utile quand une API change ses noms."""
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return value
    return None
"""
Mode REPLAY : rejouer des crimes enregistrés, comme s'ils arrivaient maintenant.

Pourquoi ?
Les APIs publiques ne sont pas vraiment "temps réel" : Chicago a ~7 jours de
retard, New York ~1 trimestre, UK Police ~1 mois. Pour tester (et pour la démo),
on relit un fichier .jsonl (un crime par ligne) et on envoie les crimes à une
vitesse choisie (ex: 10 par seconde), avec ingested_at = maintenant.
Spark voit alors un vrai flux continu et peut détecter des "hotspots".

Fichiers possibles :
  - data/sample/crimes_synthetic.jsonl  (données inventées, fournies)
  - data/sample/crimes_<ville>.jsonl     (vraies données, créées avec --save-sample)
"""
import json
import logging
import random
import time

from common import config
from common.schemas import now_utc_iso

log = logging.getLogger(__name__)


def load_events(path: str) -> list[dict]:
    events = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    log.info("%d crimes chargés depuis %s", len(events), path)
    return events


def replay(sender, path: str, speed: float = 10.0, loop: bool = False,
           shuffle: bool = True, max_events: int | None = None) -> int:
    """
    speed = nombre de crimes envoyés par seconde.
    loop  = recommencer au début quand le fichier est fini.
    Renvoie le nombre de messages envoyés.
    """
    events = load_events(path)
    if not events:
        log.warning("Fichier vide : %s", path)
        return 0

    delay = 1.0 / speed if speed > 0 else 0
    sent = 0
    round_number = 0
    while True:
        round_number += 1
        batch = events[:]
        if shuffle:
            random.shuffle(batch)  # mélange : les hotspots apparaissent petit à petit
        for event in batch:
            message = dict(event)
            message["ingested_at"] = now_utc_iso()
            message["ingest_mode"] = "replay"
            if round_number > 1:
                # Nouvel id à chaque tour, sinon la dédup (phase 2) supprimerait les messages.
                message["event_id"] = f"{event['event_id']}-r{round_number}"
            sender.send(config.TOPIC_CRIMES_RAW, key=message["city"], value=message)
            sent += 1
            if max_events and sent >= max_events:
                return sent
            if sent % 100 == 0:
                log.info("Replay : %d crimes envoyés", sent)
            if delay:
                time.sleep(delay)
        if not loop:
            return sent
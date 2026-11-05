"""
Point d'entrée du producer (Semaine 1).

Exemples (à lancer depuis la racine du projet) :

  # Tester sans Kafka : on affiche les messages, un seul tour
  python -m producer.main --once --dry-run

  # Vraies APIs -> Kafka, en boucle (toutes les 5 min par défaut)
  python -m producer.main

  # Une seule ville
  python -m producer.main --cities chicago --once

  # Récupérer des vraies données ET les sauvegarder pour le mode replay
  python -m producer.main --once --save-sample

  # Mode replay (démo) : rejoue un fichier à 10 crimes/seconde, en boucle
  python -m producer.main --mode replay --speed 10 --loop
"""
import argparse
import json
import logging
import os
import time

from common import config
from producer.checkpoint import CheckpointStore
from producer.kafka_client import KafkaSender
from producer.replay import replay
from producer.sources import SOURCES
from producer.sources.weather import fetch_weather

log = logging.getLogger("producer")


def parse_args():
    parser = argparse.ArgumentParser(description="CrimeWatch - Kafka producer multi-villes")
    parser.add_argument("--mode", choices=["api", "replay"], default="api",
                        help="api = vraies APIs, replay = rejouer un fichier .jsonl")
    parser.add_argument("--cities", default=",".join(SOURCES.keys()),
                        help="villes séparées par des virgules (défaut : toutes)")
    parser.add_argument("--once", action="store_true", help="faire un seul tour puis s'arrêter")
    parser.add_argument("--dry-run", action="store_true",
                        help="afficher les messages au lieu de les envoyer à Kafka")
    parser.add_argument("--save-sample", action="store_true",
                        help="sauvegarder les crimes reçus dans data/sample/ (pour le replay)")
    parser.add_argument("--no-weather", action="store_true", help="ne pas appeler la météo")
    # Options du mode replay
    parser.add_argument("--file", default=os.path.join(config.SAMPLE_DIR, "crimes_synthetic.jsonl"),
                        help="fichier .jsonl à rejouer")
    parser.add_argument("--speed", type=float, default=10.0, help="crimes par seconde (replay)")
    parser.add_argument("--loop", action="store_true", help="rejouer en boucle (replay)")
    return parser.parse_args()


def save_sample(city: str, events: list[dict]) -> None:
    """Ajoute les crimes à data/sample/crimes_<ville>.jsonl (1 crime = 1 ligne JSON)."""
    os.makedirs(config.SAMPLE_DIR, exist_ok=True)
    path = os.path.join(config.SAMPLE_DIR, f"crimes_{city}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    log.info("[%s] %d crimes ajoutés à %s", city, len(events), path)


def poll_once(cities, sender, checkpoints, save, with_weather) -> None:
    """Un tour : pour chaque ville -> crimes, puis météo."""
    for city in cities:
        # try/except : si UNE ville plante (API en panne), les autres continuent.
        try:
            source = SOURCES[city]()
            events, new_checkpoint = source.poll(checkpoints.get(city))
            for event in events:
                sender.send(config.TOPIC_CRIMES_RAW, key=city, value=event)
            sender.flush()
            # On met à jour le marque-page seulement APRÈS l'envoi réussi.
            if not sender.dry_run:
                checkpoints.set(city, new_checkpoint)
            if save and events:
                save_sample(city, events)
        except Exception as exc:  # noqa: BLE001
            log.error("[%s] Erreur crimes : %s", city, exc)

        if with_weather:
            try:
                weather = fetch_weather(city)
                if weather:
                    sender.send(config.TOPIC_WEATHER_RAW, key=city, value=weather)
                    log.info("[%s] Météo : %s°C, pluie %s mm", city,
                             weather["temperature_c"], weather["rain_mm"])
            except Exception as exc:  # noqa: BLE001
                log.error("[%s] Erreur météo : %s", city, exc)
    sender.flush()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = parse_args()

    cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    unknown = [c for c in cities if c not in SOURCES]
    if unknown:
        raise SystemExit(f"Ville(s) inconnue(s) : {unknown}. Choix possibles : {list(SOURCES)}")

    sender = KafkaSender(config.KAFKA_BOOTSTRAP_SERVERS, dry_run=args.dry_run)

    if args.mode == "replay":
        total = replay(sender, args.file, speed=args.speed, loop=args.loop)
        sender.flush()
        log.info("Replay terminé : %d messages envoyés", total)
        return

    checkpoints = CheckpointStore(config.CHECKPOINT_FILE)
    while True:
        started = time.time()
        log.info("===== Nouveau tour : %s =====", ", ".join(cities))
        poll_once(cities, sender, checkpoints, args.save_sample, not args.no_weather)
        log.info("Tour terminé en %.1fs. Total envoyé : %d (erreurs Kafka : %d)",
                 time.time() - started, sender.sent, sender.errors)
        if args.once:
            break
        log.info("Pause de %ds avant le prochain tour...", config.POLL_INTERVAL_SECONDS)
        time.sleep(config.POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
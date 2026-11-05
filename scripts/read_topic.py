"""
Lire les messages d'un topic Kafka dans le terminal (pour vérifier que tout marche).

Usage :
  python scripts/read_topic.py crimes.alerts
  python scripts/read_topic.py crimes.raw --from-beginning --max 5
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Consumer  # noqa: E402

from common import config  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic", help="ex: crimes.raw, weather.raw, crimes.alerts")
    parser.add_argument("--from-beginning", action="store_true", help="lire depuis le début du topic")
    parser.add_argument("--max", type=int, default=0, help="s'arrêter après N messages (0 = jamais)")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        # Un nouveau group.id à chaque fois = on ne "vole" pas les messages des autres.
        "group.id": f"read-topic-{os.getpid()}",
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
    })
    consumer.subscribe([args.topic])
    print(f"Lecture de '{args.topic}' sur {config.KAFKA_BOOTSTRAP_SERVERS} (Ctrl+C pour arrêter)")

    count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Erreur :", msg.error())
                continue
            value = json.loads(msg.value())
            value.pop("raw", None)  # on cache le JSON brut, trop long
            print(json.dumps(value, indent=2, ensure_ascii=False))
            count += 1
            if args.max and count >= args.max:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
"""
Envoi des messages vers Kafka.

- Mode normal : on utilise la librairie confluent-kafka.
- Mode --dry-run : on n'envoie RIEN, on affiche juste les messages dans le terminal.
  Très utile pour tester le producer sans lancer Docker.
"""
import json
import logging

log = logging.getLogger(__name__)


class KafkaSender:
    def __init__(self, bootstrap_servers: str, dry_run: bool = False):
        self.dry_run = dry_run
        self.sent = 0
        self.errors = 0
        self._producer = None
        if not dry_run:
            # Import ici (et pas en haut du fichier) pour que le mode dry-run
            # marche même si confluent-kafka n'est pas installé.
            from confluent_kafka import Producer

            self._producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",               # Kafka confirme que le message est bien écrit
                "enable.idempotence": True,  # évite les doublons si on réessaie
                "linger.ms": 50,             # regroupe les messages par petits paquets
                "compression.type": "gzip",
            })
            log.info("Connecté à Kafka : %s", bootstrap_servers)

    def _on_delivery(self, err, msg) -> None:
        """Appelée par Kafka pour chaque message : OK ou erreur."""
        if err is not None:
            self.errors += 1
            log.error("Échec d'envoi vers %s : %s", msg.topic(), err)

    def send(self, topic: str, key: str, value: dict) -> None:
        """
        key = la ville. Kafka met tous les messages d'une même clé dans la même
        partition, donc l'ordre des crimes d'une ville est conservé.
        """
        payload = json.dumps(value, ensure_ascii=False, default=str)
        if self.dry_run:
            preview = {k: v for k, v in value.items() if k != "raw"}
            print(f"[DRY-RUN] {topic} | key={key} | {json.dumps(preview, ensure_ascii=False)}")
            self.sent += 1
            return

        while True:
            try:
                self._producer.produce(topic, key=key.encode(), value=payload.encode("utf-8"),
                                       on_delivery=self._on_delivery)
                break
            except BufferError:
                # La file d'attente locale est pleine : on laisse Kafka respirer.
                self._producer.poll(1)
        self._producer.poll(0)
        self.sent += 1

    def flush(self) -> None:
        if self._producer is not None:
            self._producer.flush(30)
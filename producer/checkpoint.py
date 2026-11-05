"""
Checkpoint = "marque-page".

Le producer note, pour chaque ville, la date du dernier crime déjà envoyé.
Au prochain appel, il demande seulement les crimes PLUS RÉCENTS.
Comme ça on n'envoie pas deux fois les mêmes crimes, même après un redémarrage.

Le fichier ressemble à :
{
  "chicago": {"since": "2026-10-20T14:30:00", "ids_at_since": ["13245678"]},
  "london":  {"month": "2026-09"}
}
"ids_at_since" = les crimes qui ont exactement la même date que "since"
(on les garde pour ne pas les renvoyer).
"""
import json
import logging
import os

log = logging.getLogger(__name__)


class CheckpointStore:
    def __init__(self, path: str):
        self.path = path
        self.state: dict = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as f:
                self.state = json.load(f)
            log.info("Checkpoint chargé depuis %s", self.path)

    def get(self, city: str) -> dict:
        return self.state.get(city, {})

    def set(self, city: str, value: dict) -> None:
        self.state[city] = value
        self._save()

    def _save(self) -> None:
        folder = os.path.dirname(self.path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        # On écrit d'abord dans un fichier temporaire, puis on remplace :
        # si le programme plante pendant l'écriture, l'ancien fichier reste intact.
        tmp_path = self.path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2)
        os.replace(tmp_path, self.path)
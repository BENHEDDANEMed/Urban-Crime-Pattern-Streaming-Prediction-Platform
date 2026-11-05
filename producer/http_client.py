"""
Petit client HTTP réutilisable.

Les APIs publiques tombent parfois en panne quelques secondes.
Ici on configure des "retries" : si un appel échoue (erreur 429, 500, 503...),
on réessaie automatiquement 3 fois en attendant un peu plus à chaque fois.
"""
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

TIMEOUT_SECONDS = 30

_session = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        retry = Retry(
            total=3,
            backoff_factor=2,  # attend 2s, 4s, 8s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({"User-Agent": "CrimeWatch-student-project/1.0"})
        _session = session
    return _session


def get_json(url: str, params: dict | None = None, headers: dict | None = None):
    """Fait un GET et renvoie le JSON. Lève une exception si ça échoue."""
    log.debug("GET %s %s", url, params)
    response = get_session().get(url, params=params, headers=headers, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()
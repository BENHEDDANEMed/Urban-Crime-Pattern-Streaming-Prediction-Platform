# Guide Phase 1 — Comprendre le code (S1 + S2)

Ce guide explique **simplement** ce que fait chaque partie du code.
Lis-le dans l'ordre : chaque partie utilise la précédente.

---

## 1. L'idée en une image

Imagine une **usine** :

```
 APIs (villes)      Producer         Kafka             Spark              Résultats
 ┌──────────┐     ┌─────────┐     ┌─────────┐      ┌──────────┐     ┌──────────────┐
 │ Chicago  │──┐  │ récupère│     │ tapis   │      │ analyse  │────►│ alertes      │
 │ LA       │──┼─►│ nettoie │────►│ roulant │─────►│ en temps │     │ (hotspots)   │
 │ New York │──┤  │ envoie  │     │ (topics)│      │ réel     │────►│ Bronze       │
 │ Londres  │──┘  └─────────┘     └─────────┘      └──────────┘     │ (archives)   │
 └──────────┘                                                       └──────────────┘
```

- **Producer (S1)** = l'ouvrier qui va chercher les colis (les crimes) et les pose sur le tapis.
- **Kafka** = le tapis roulant. Il garde les colis dans l'ordre, même si personne ne les prend tout de suite.
- **Spark (S2)** = la machine au bout du tapis qui analyse les colis en continu.

**Pourquoi Kafka au milieu ?** Si Spark tombe en panne, les messages ne sont pas perdus :
ils attendent dans Kafka (7 jours). Le producer et Spark ne dépendent pas l'un de l'autre.

---

## 2. Le voyage d'un crime (du début à la fin)

Suivons UN crime de Chicago :

**Étape 1 — L'API renvoie une ligne brute**
```json
{"id": "13245678", "date": "2026-10-28T14:30:00.000", "primary_type": "THEFT",
 "latitude": "41.881", "longitude": "-87.627"}
```

**Étape 2 — `producer/sources/chicago.py` la transforme au format commun**
(`normalize()` → `build_crime_event()` dans `common/schemas.py`)
```json
{"event_id": "chicago-13245678", "city": "chicago", "crime_type": "THEFT",
 "crime_category": "theft", "latitude": 41.881, "longitude": -87.627,
 "occurred_at": "2026-10-28T14:30:00", "ingested_at": "2026-11-05T18:00:00Z", ...}
```
New York dirait `ofns_desc: "PETIT LARCENY"` → on obtient aussi `crime_category: "theft"`.
**C'est ça l'intérêt du format commun** : après cette étape, toutes les villes se ressemblent.

**Étape 3 — `producer/kafka_client.py` l'envoie dans le topic `crimes.raw`**
avec la clé `chicago`.

**Étape 4 — Spark le lit** (`streaming/crime_stream_job.py`) et fait 2 choses en parallèle :
- il le copie **tel quel** dans `data/lake/bronze/crimes/city=chicago/ingest_date=2026-11-05/` ;
- il le range dans la **fenêtre de 30 minutes** 18:00–18:30 de Chicago.

**Étape 5 — DBSCAN** (`streaming/clustering.py`) regarde tous les crimes de cette fenêtre.
S'il y a au moins 5 crimes à moins de 500 m les uns des autres, dans une zone de moins de 1 km
→ **hotspot** → un message est envoyé dans `crimes.alerts`.

---

## 3. Les fichiers, un par un

### `common/` — partagé par tout le projet

| Fichier | Rôle | À retenir |
|---|---|---|
| `config.py` | Toute la configuration | Les villes (centre + "boîte" de validation), les topics, les URLs. **Une seule source de vérité.** |
| `schemas.py` | Le format commun | `build_crime_event()` rejette les crimes sans coordonnées ou hors de la ville. `categorize()` range les crimes en 11 catégories (l'ordre des mots-clés compte). |

### `producer/` — Semaine 1

| Fichier | Rôle |
|---|---|
| `main.py` | Le chef d'orchestre : pour chaque ville → récupérer les crimes → envoyer → mettre à jour le marque-page → météo → attendre 5 min → recommencer. |
| `sources/base.py` | Le code commun aux sources. `SocrataSource` sert pour Chicago, LA et NYC (même plateforme, même langage de requête "SoQL", pagination par 1000 lignes). |
| `sources/chicago.py`, `new_york.py`, `los_angeles.py`, `uk_police.py` | Une classe par ville (héritage). Chacune dit seulement ce qui est différent : URL, colonne date, et comment lire une ligne (`normalize`). |
| `sources/weather.py` | Météo actuelle de chaque ville (Open-Meteo). |
| `checkpoint.py` | Le **marque-page** : la date du dernier crime envoyé, pour demander seulement les nouveaux au tour suivant. Fichier : `data/checkpoints/producer_state.json`. |
| `kafka_client.py` | Envoie vers Kafka (`acks=all`, idempotence). Avec `--dry-run`, affiche au lieu d'envoyer. |
| `replay.py` | Rejoue un fichier `.jsonl` comme si les crimes arrivaient maintenant (pour la démo). |
| `http_client.py` | Fait les appels HTTP et **réessaie 3 fois** (2 s, 4 s, 8 s) si l'API est en panne. |

### `streaming/` — Semaine 2

| Fichier | Rôle |
|---|---|
| `crime_stream_job.py` | Le job Spark. Il lance **3 queries** en même temps : Bronze crimes, Bronze météo, alertes. |
| `clustering.py` | DBSCAN en Python pur. Séparé de Spark pour pouvoir le **tester avec pytest** sans lancer Spark. |

### Autres

| Fichier | Rôle |
|---|---|
| `docker-compose.yml` | La liste des conteneurs : kafka, kafka-init (crée les topics), kafka-ui, producer, spark-streaming. |
| `scripts/generate_synthetic_sample.py` | Crée 440 crimes inventés (3 hotspots + 60 crimes isolés par ville) pour la démo. |
| `scripts/read_topic.py` | Un petit consumer pour lire un topic dans le terminal. |
| `tests/` | 16 tests pytest, sans internet ni Kafka. |

---

## 4. Les concepts clés (pour l'oral)

**Topic Kafka** : une "file" de messages avec un nom. On en a 3 : `crimes.raw`, `weather.raw`, `crimes.alerts`.
Un message lu **reste** dans Kafka : plusieurs programmes peuvent lire les mêmes messages.

**Clé Kafka = la ville** : Kafka met tous les messages d'une même clé dans la même
partition → l'ordre des crimes d'une ville est respecté.

**Structured Streaming** : Spark traite le flux par **micro-batchs** (petits paquets),
ici toutes les 30 secondes pour les alertes et toutes les minutes pour Bronze.

**Fenêtre (window) de 30 min** : on regroupe les crimes arrivés entre 18:00 et 18:30,
puis 18:30–19:00, etc. Le cahier des charges demande les "spikes en moins de 30 minutes".

**Watermark (10 min)** : "j'accepte les messages en retard de 10 minutes maximum".
Après, Spark ferme la fenêtre et libère la mémoire.

**Checkpoint Spark** (`data/checkpoints/spark/`) : Spark note où il en est dans Kafka.
Si on le redémarre, il reprend exactement là où il s'était arrêté.

**DBSCAN** : algorithme de clustering basé sur la densité. 2 paramètres :
`eps` = rayon (0,5 km) et `min_samples` = nombre minimum de crimes (5).
Avantages par rapport à K-Means : pas besoin de choisir le nombre de groupes,
et il ignore les points isolés (le "bruit").

**Effet de chaîne** : point faible de DBSCAN. Dans une ville dense, A est proche de B,
B proche de C… et DBSCAN relie tout un quartier en un seul groupe géant.
→ On ignore les groupes de plus de **1 km de rayon** (`HOTSPOT_MAX_RADIUS_KM`).

**Distance haversine** : la vraie distance sur la Terre (qui est ronde) entre deux points GPS.

**Bronze / Silver / Gold (architecture Médaillon)** :
Bronze = données brutes (on ne touche à rien) → Silver = nettoyées → Gold = prêtes pour l'analyse.
La phase 1 s'arrête à Bronze. Format **Parquet** (en colonnes, compressé), rangé par
`city=.../ingest_date=...` pour lire vite une seule ville ou un seul jour.

**Pourquoi `ingested_at` et pas `occurred_at` pour les fenêtres ?**
Les APIs publiques ont du retard (Chicago ~7 jours, New York ~3 mois, Londres ~1 mois).
Si on utilisait la date du crime, tout serait "trop vieux" pour du temps réel.
On fait donc le temps réel sur l'**heure d'arrivée**, et on garde `occurred_at`
pour les analyses historiques et le ML (phase 2).

---

## 5. Lancer le projet pas à pas (VS Code, Windows)

### Étape A — Préparer Python (une seule fois)
Ouvre le terminal de VS Code (Ctrl + `) :
```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements-dev.txt
copy .env.example .env
python scripts/generate_synthetic_sample.py
```

### Étape B — Tests (sans internet, sans Docker)
```powershell
pytest -v
```
Tu dois voir : `16 passed`.

### Étape C — Producer sans Kafka
```powershell
python -m producer.main --once --dry-run > $null
```
`> $null` cache les crimes et garde seulement le résumé par ville
(`X lignes reçues, Y valides, Z rejetées`).

### Étape D — Lancer Kafka + Spark
```powershell
docker compose up -d spark-streaming kafka-ui
docker compose logs -f spark-streaming
```
Attendre `3 queries démarrées` (le premier lancement prend 5 à 10 minutes), puis Ctrl+C.

### Étape E — La démo avec alertes
1. Dans `.env` : `PRODUCER_MODE=replay`
2. `docker compose up -d producer` → envoie 440 crimes en ~45 s, puis s'arrête tout seul.
3. Attendre ~1 minute, puis :
   ```powershell
   docker compose logs spark-streaming | Select-String "ALERTE"
   ```
   Résultat attendu : 3 hotspots par ville (25, 15, 10 crimes), rayon < 0,5 km.
4. Autres façons de voir les alertes :
   - `python scripts/read_topic.py crimes.alerts --from-beginning`
   - Kafka UI : http://localhost:8080 → Topics → `crimes.alerts` → Messages
5. Bronze : des fichiers `.parquet` dans `data/lake/bronze/crimes/city=.../`.
6. Spark UI : http://localhost:4040 → onglet Structured Streaming.

Pour **rejouer** la démo : `docker compose up -d producer`.

### Arrêter / remettre à zéro
```powershell
docker compose down        # arrête tout (les données Kafka sont gardées)
docker compose down -v     # arrête ET supprime les données Kafka
Remove-Item -Recurse -Force data\checkpoints\spark, data\lake\bronze -ErrorAction SilentlyContinue
```
Après un `down -v`, toujours supprimer **ensemble** les checkpoints Spark et le dossier Bronze.

---

## 6. Problèmes rencontrés (et solutions)

| Problème | Cause | Solution |
|---|---|---|
| `Connection refused localhost:9092` | Kafka n'est pas lancé | `docker compose up -d kafka-init` |
| Une ville donne "0 valides" (c'était LA) | L'API a d'autres noms de colonnes | Afficher 2 lignes brutes avec `get_json(config.LA_URL, params={"$limit": 2})`, puis ajouter le bon nom dans `LAT_KEYS`, `TYPE_KEYS`… de `los_angeles.py` |
| Le terminal ne montre que la fin | Trop de lignes `[DRY-RUN]` | Ajouter `> $null` à la commande |
| Spark "Up 2 seconds", redémarre en boucle **sans erreur** | Fichier `crime_stream_job.py` coupé à la fin (copier-coller) | Vérifier les dernières lignes : `Get-Content streaming/crime_stream_job.py -Tail 3` doit montrer `if __name__ == "__main__":` / `main()` |
| Alerte géante (1803 crimes dans 12,78 km) | Effet de chaîne de DBSCAN + données qui arrivent par paquets | `HOTSPOT_MAX_RADIUS_KM=1.0` |
| Des dizaines de fausses alertes en replay | Replay en boucle = mêmes coordonnées renvoyées toutes les 45 s | Le replay joue maintenant le fichier **une seule fois** |
| `Select-String "ALERTE"` ne montre rien | Trop tôt, ou conteneur recréé (anciens logs effacés) | Attendre ~1,5 min après le producer, vérifier `docker compose logs producer` |
| New York renvoie 0 ligne | Dataset mis à jour 1 fois par trimestre | Augmenter `LOOKBACK_DAYS` dans `.env` |
| Londres : "rien de nouveau" | 1 nouveau mois par mois | Normal. Supprimer `data/checkpoints/producer_state.json` pour tout renvoyer |
| Erreur 429 (Too Many Requests) | Trop d'appels | Augmenter `POLL_INTERVAL_SECONDS`, ou créer un token Socrata gratuit |
| Spark ne démarre pas, erreur de checkpoint | Checkpoints qui pointent vers d'anciens messages | Remise à zéro (section 5) |

---

## 7. Questions que le prof peut poser (et réponses courtes)

- **Pourquoi Kafka et pas directement Spark → API ?**
  Kafka découple les deux : si Spark plante, aucun message n'est perdu, et plusieurs
  consommateurs (Spark, dashboard...) peuvent lire les mêmes données.
- **Comment évitez-vous les doublons ?**
  Le producer a un marque-page (date du dernier crime + ids), mis à jour **après** l'envoi.
  Chaque crime a un `event_id` unique, utilisé pour la déduplication en Silver (phase 2).
  Côté Kafka : `enable.idempotence=True`.
- **Que se passe-t-il si une API tombe en panne ?**
  3 réessais automatiques, puis on passe à la ville suivante (`try/except` par ville).
- **Pourquoi DBSCAN et pas K-Means ?**
  Pas besoin de connaître le nombre de zones à l'avance, il ignore les crimes isolés,
  et il trouve des zones de toutes les formes.
- **Quelle limite de DBSCAN avez-vous rencontrée ?**
  L'effet de chaîne : avec 2000 vrais crimes de LA, il a créé un groupe de 12,78 km.
  On a ajouté un rayon maximum de 1 km.
- **Comment choisir eps = 0,5 km ?**
  C'est environ la taille d'un quartier. C'est un paramètre réglable dans `.env` (`DBSCAN_EPS_KM`).
- **Les données sont-elles vraiment en temps réel ?**
  Non, les APIs ont des jours de retard et livrent par paquets. On fait du "quasi temps réel"
  sur l'heure d'arrivée, et on a un mode replay pour simuler un vrai flux pendant la démo.
- **Pourquoi des fichiers Parquet ?**
  Format en colonnes et compressé : plus léger et plus rapide à lire que JSON ou CSV.

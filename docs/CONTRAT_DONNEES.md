# Contrat de données — Phase 1

Ce fichier décrit **la forme exacte** des données produites par la phase 1.
Les phases 2 et 3 s'appuient dessus. **Si tu changes un champ, mets à jour ce fichier
et augmente `schema_version`.**

Code source du format : [`common/schemas.py`](../common/schemas.py).

---

## 1. Topic Kafka `crimes.raw`

- **Clé** : la ville (`chicago`, `los_angeles`, `new_york`, `london`)
- **Valeur** : JSON, un crime par message

| Champ | Type | Exemple | Description |
|---|---|---|---|
| `schema_version` | int | `1` | Version du format |
| `event_id` | string | `chicago-13245678` | Id unique = ville + id source |
| `source_id` | string | `13245678` | Id dans l'API d'origine |
| `city` | string | `chicago` | Ville (clé courte) |
| `source` | string | `chicago_data_portal` | API d'origine (`synthetic` pour les données de test) |
| `crime_type` | string | `THEFT` | Libellé d'origine, en MAJUSCULES |
| `crime_category` | string | `theft` | Catégorie commune (voir liste ci-dessous) |
| `description` | string \| null | `RETAIL THEFT` | Détail (nom de rue pour UK, type de lieu pour LA) |
| `latitude` | float | `41.881` | Validée : à l'intérieur de la ville. LA : centre du "bloc de 100 numéros" (approximatif) |
| `longitude` | float | `-87.627` | Validée : à l'intérieur de la ville |
| `occurred_at` | string | `2026-10-28T14:30:00` | Date du crime, **heure locale de la ville**, sans fuseau. UK = 1er jour du mois à 00:00. LA : souvent 00:00 = heure inconnue |
| `ingested_at` | string | `2026-11-05T18:00:00Z` | Moment de l'envoi dans Kafka, **UTC** |
| `ingest_mode` | string | `api` | `api` = vraie donnée, `replay` = rejouée pour la démo |
| `raw` | object | `{...}` | Ligne brute de l'API, sans modification |

**Catégories communes** : `theft`, `vehicle_theft`, `burglary`, `robbery`, `assault`,
`sexual`, `drugs`, `weapons`, `vandalism`, `public_order`, `other`.

## 2. Topic Kafka `weather.raw`

- **Clé** : la ville

| Champ | Type | Exemple |
|---|---|---|
| `schema_version` | int | `1` |
| `city` | string | `chicago` |
| `source` | string | `open_meteo` |
| `latitude`, `longitude` | float | `41.88`, `-87.63` |
| `observed_at` | string | `2026-11-05T12:00:00` (heure locale) |
| `temperature_c` | float | `8.4` |
| `precipitation_mm` | float | `0.0` |
| `rain_mm` | float | `0.0` |
| `wind_speed_kmh` | float | `14.2` |
| `is_day` | int | `1` (jour) / `0` (nuit) |
| `ingested_at` | string | `2026-11-05T18:00:00Z` (UTC) |
| `raw` | object | réponse complète d'Open-Meteo |

## 3. Topic Kafka `crimes.alerts`

Produit par Spark quand DBSCAN trouve un hotspot dans une fenêtre de 30 minutes.

| Champ | Type | Exemple | Description |
|---|---|---|---|
| `alert_id` | string | `chicago\|2026-11-05T18:00:00Z\|41.89_-87.62` | ville + fenêtre + zone (~1 km) |
| `city` | string | `chicago` | |
| `window_start`, `window_end` | string | `2026-11-05T18:00:00Z` | Fenêtre de 30 min (UTC, basée sur `ingested_at`) |
| `rank` | int | `1` | 1 = plus gros hotspot de la fenêtre |
| `severity` | string | `high` | `high` si ≥ 3 × `min_samples`, sinon `medium` |
| `crime_count` | int | `25` | Nombre de crimes dans le hotspot |
| `center_latitude`, `center_longitude` | float | | Centre du hotspot |
| `radius_km` | float | `0.35` | Distance max entre le centre et un crime |
| `top_category` | string | `theft` | Catégorie la plus fréquente |
| `categories` | object | `{"theft": 12, "assault": 5}` | Détail par catégorie |
| `eps_km`, `min_samples` | float, int | `0.5`, `5` | Paramètres DBSCAN utilisés |
| `max_radius_km` | float | `1.0` | Les groupes plus larges sont ignorés (effet de chaîne) |
| `detected_at` | string | UTC | Moment du calcul |

⚠️ Une même alerte peut être **renvoyée** si le hotspot grandit (même `alert_id`,
`crime_count` plus grand). Le consommateur garde la dernière version par `alert_id`.

## 4. Bronze Parquet (Data Lake)

Écrit par Spark en continu (toutes les minutes).

| Dossier | Partitions |
|---|---|
| `data/lake/bronze/crimes/` | `city=.../ingest_date=YYYY-MM-DD/` |
| `data/lake/bronze/weather/` | `city=.../ingest_date=YYYY-MM-DD/` |

| Colonne | Type | Description |
|---|---|---|
| `raw_json` | string | Le message Kafka **complet** (JSON du §1 ou §2) |
| `kafka_key` | string | Clé Kafka (ville) |
| `kafka_topic`, `kafka_partition`, `kafka_offset` | string, int, long | Position dans Kafka (utile pour la dédup) |
| `kafka_timestamp` | timestamp | Heure d'arrivée dans Kafka |
| `city` | string | Extrait du JSON |
| `ingested_at` | timestamp | Extrait du JSON (UTC) |
| `ingest_date` | date | Partition |

**Pour la phase 2 (Silver)** : lire avec
`spark.read.parquet("data/lake/bronze/crimes")`, puis
`from_json(raw_json, schema)`, dédupliquer sur `event_id`, et filtrer
`ingest_mode = 'api'` si on ne veut pas les données de démo.

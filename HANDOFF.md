# HANDOFF — Passage de relais

> Ce fichier est mis à jour **à chaque changement de personne**.
> La personne qui arrive lit ce fichier EN PREMIER.

---

## Phase 1 (S1 + S2) — Ingestion + Streaming

**Responsable :** Mohamed Benheddane
**Statut :** ✅ Terminée — pipeline testé de bout en bout le 05/11/2026

### ✅ Ce qui est fait

- [x] Structure du repo, `docker-compose.yml` (Kafka KRaft, Kafka UI, producer, Spark)
- [x] 3 topics créés automatiquement : `crimes.raw`, `weather.raw`, `crimes.alerts`
- [x] Producer multi-villes : Chicago, Los Angeles, New York, Londres (UK Police) + météo Open-Meteo
- [x] Format commun des messages + 11 catégories de crimes communes → [`docs/CONTRAT_DONNEES.md`](docs/CONTRAT_DONNEES.md)
- [x] Checkpoint du producer (pas de doublons après redémarrage)
- [x] Mode `replay` + échantillon synthétique (`scripts/generate_synthetic_sample.py`) pour la démo
- [x] Job Spark Streaming : parsing, fenêtres 30 min, DBSCAN, alertes → `crimes.alerts`
- [x] Bronze Parquet : `data/lake/bronze/crimes` et `data/lake/bronze/weather`
- [x] 16 tests pytest (sans internet ni Kafka)

### 📊 Résultats des tests réels (05/11/2026)

| Ville | Lignes reçues | Valides |
|---|---|---|
| Chicago | 2000 | 1996 |
| Los Angeles | 2000 | 1998 (après correction des colonnes) |
| New York | 2000 | 2000 |
| Londres | 3777 | 3777 |

Démo replay : DBSCAN retrouve les 3 hotspots placés dans chaque ville
(25, 15 et 10 crimes, rayon entre 0,26 et 0,50 km) et ignore les crimes isolés.

### ▶️ Comment lancer

```bash
cp .env.example .env                       # Windows : copy .env.example .env
python scripts/generate_synthetic_sample.py
docker compose up -d spark-streaming kafka-ui
docker compose logs -f spark-streaming     # attendre "3 queries démarrées"
docker compose up -d producer              # avec PRODUCER_MODE=replay dans .env
docker compose logs spark-streaming | grep ALERTE    # PowerShell : | Select-String "ALERTE"
```
Détails et explications : [`docs/GUIDE_PHASE1.md`](docs/GUIDE_PHASE1.md).

### 🧪 Vérifié sur la machine de Mohamed

- [x] `pytest` → 16 passed
- [x] `python -m producer.main --once --dry-run` → des crimes pour chaque ville
- [x] `docker compose up` → tous les services démarrent
- [x] Messages visibles dans Kafka UI (http://localhost:8080)
- [x] Alertes visibles en mode `replay`
- [x] Fichiers Parquet dans `data/lake/bronze/`
- [x] Champs de l'API Los Angeles vérifiés

### 🔧 Problèmes rencontrés et corrigés

1. **Los Angeles : 0 crime valide.** Le nouveau dataset LAPD (NIBRS, `k7nn-b2ep`) appelle
   les coordonnées `hndrdth_lat` / `hndrdth_lon`. Ajouté dans `producer/sources/los_angeles.py`
   + un test avec une vraie ligne de l'API.
2. **« Grand Theft Auto » classé en `theft`** au lieu de `vehicle_theft` → mot-clé ajouté.
3. **Alerte géante (1803 crimes dans 12,78 km)** avec les vraies données : « effet de chaîne »
   de DBSCAN (les crimes se touchent de proche en proche dans une ville dense).
   → Nouveau paramètre `HOTSPOT_MAX_RADIUS_KM = 1.0` : un groupe plus large n'est pas un hotspot.
4. **Replay en boucle = fausses alertes** : les mêmes coordonnées étaient renvoyées toutes les
   45 s, donc même un crime isolé devenait un « groupe ». → Le replay joue le fichier **une seule
   fois** (`restart: on-failure` dans docker-compose).

### ⚠️ Limites connues

1. **Pas de vrai temps réel** : les APIs ont du retard (Chicago ~7 j, LA ~2 sem.,
   NYC ~3 mois, UK ~1 mois) et livrent les crimes **par paquets**. Les fenêtres et les alertes
   utilisent donc `ingested_at`. Avec les vraies données, un paquet ressemble à un « pic ».
   → Pour la démo, utiliser `PRODUCER_MODE=replay`.
2. **Coordonnées de LA approximatives** : « bloc de 100 numéros » de la rue (vie privée).
   Suffisant pour DBSCAN avec eps = 500 m.
3. **Heure inconnue à LA** : beaucoup de crimes ont `time_occ = "0000"` → `occurred_at` à 00:00.
   ⚠️ À filtrer ou traiter à part en phase 2 pour prédire l'heure des crimes.
4. **Catégories LA** : les libellés NIBRS (ex : « Intimidation », fraudes) tombent souvent
   dans `other`. On peut enrichir `CATEGORY_KEYWORDS` dans `common/schemas.py`.
5. **Alertes répétées** : une alerte est renvoyée si son hotspot grandit (même `alert_id`).
   Le consommateur doit garder la dernière version.
6. **Spark en mode `local[*]`** (un seul conteneur). Suffisant pour le projet.
7. **Données synthétiques** : `crimes_synthetic.jsonl` est inventé (`source = "synthetic"`).
   Pour la démo finale, on peut aussi rejouer de vraies données enregistrées avec `--save-sample`.

### 🔄 Remettre à zéro (si Kafka contient de vieilles données de démo)

```bash
docker compose down -v
# Windows : Remove-Item -Recurse -Force data\checkpoints\spark, data\lake\bronze
rm -rf data/checkpoints/spark data/lake/bronze
```
Toujours supprimer **ensemble** les checkpoints Spark et le dossier Bronze.

### ➡️ À faire en Phase 2 (S3 + S4)

1. **Silver** (Spark batch) : lire Bronze → `from_json(raw_json)` → dédup sur `event_id`
   → enlever `ingest_mode = 'replay'` → jointure avec la météo (même ville, heure la plus proche).
2. **Gold** : `gold_clusters` (réutiliser `streaming/clustering.find_hotspots` sur l'historique,
   avec `max_radius_km`), `gold_hotspots` (crimes par zone et par heure), `gold_predictions`.
3. **Airflow** : ajouter Airflow + Postgres dans `docker-compose.yml`, DAG quotidien Bronze → Silver → Gold.
4. **XGBoost + MLflow** : prédire `crime_category` et le créneau horaire à partir de
   `occurred_at` (heure, jour, mois), position, météo. Attention aux heures 00:00 de LA.
   Kaggle pour plus d'historique.
5. **Carte Streamlit simple** qui lit le topic `crimes.alerts`.

### 📞 Contact

Appel de passage de relais prévu le : *(date)*

---

## Phase 2 (S3 + S4) — Data Lake + Airflow + ML

**Responsable :** *(nom)*
**Statut :** pas commencé

---

## Phase 3 (S5 + S6) — Snowflake + dbt + Dashboard + CI

**Responsable :** *(nom)*
**Statut :** pas commencé

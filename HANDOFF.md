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
  *(passées à 16 catégories en phase 2, voir plus bas)*
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

**Responsable :** Mustapha Hamdaoui Alaoui
**Statut :** ✅ Terminée — DAG Airflow testé de bout en bout (6 tâches vertes) le 07/11/2026

### ✅ Ce qui est fait

- [x] **Silver** (`batch/bronze_to_silver.py`) : parsing JSON avec schéma explicite, déduplication sur
      `event_id` (on garde la version la plus récente), données de démo (`replay`) enlevées,
      colonnes de temps (`occurred_hour`, `day_of_week`, `month`, `is_weekend`), `hour_known`
- [x] **Catégories v2** (`common/schemas.py`) : 5 nouvelles catégories mesurées sur les vrais libellés
      (`harassment`, `fraud`, `trespass`, `traffic`, `justice`) + sous-type (`description`) pour Chicago/NYC.
      Recalculées en Silver à partir de `crime_type` → **pas besoin de re-télécharger**
- [x] **Météo historique** (`batch/weather_history.py`) : API d'archives Open-Meteo, heure par heure,
      seulement sur les dates des crimes de chaque ville → `silver/weather_hourly`
- [x] **Gold** (`batch/silver_to_gold.py`) :
      `crime_features` (1 ligne par crime + météo + zone), `zone_hourly` (crimes par zone et par heure),
      `zone_stats` (classement des zones). Zones = grille de 0,005° (~500 m)
- [x] **ML** (`ml/train_category.py`) : XGBoost qui prédit la catégorie du crime, split temporel,
      2 baselines, historique des zones en *target encoding* out-of-fold, suivi dans MLflow
- [x] **Contrôles qualité** (`batch/quality_checks.py`) : doublons, valeurs vides, crimes hors de leur ville,
      catégories inconnues, Londres sans heure, nombre de lignes Gold = Silver, météo manquante < 5 %
- [x] **Airflow** : `docker-compose.yml` (Airflow 2.10 + Postgres), image `docker/airflow.Dockerfile`
      (Java + Python séparé `/opt/jobs-venv` pour les jobs), DAG `airflow/dags/crimewatch_daily.py`
- [x] Producer : `MAX_RECORDS_PER_CITY` et `LOOKBACK_DAYS` transmis au conteneur
- [x] **28 tests** pytest (16 de la phase 1 + 12)

### 📊 Résultats (données réelles du 07/11/2026)

| Mesure | Résultat |
|---|---|
| Bronze → Silver | 201 785 → **176 627** crimes (~25 000 doublons venant des redémarrages du producer) |
| Par ville | Chicago 13 565 · LA 12 700 · **New York 146 585** · Londres 3 777 |
| Crimes à 00:00 pile (« heure inconnue ») | Chicago 2,7 % · LA 2,1 % · NYC 1,1 % · Londres 100 % |
| Catégorie `other` avant → après v2 | Chicago 15,5 → **3,3 %** · LA 23,5 → **8,6 %** · NYC 18,5 → **2,9 %** |
| Météo jointe | 100 % des crimes (96,6 % à l'heure près, le reste à la journée) |
| Zones | 7 652 · n°1 = Times Square, le Loop (Chicago), Soho (Londres), Downtown LA |

**Modèle « catégorie du crime »** (16 classes, test = 20 % derniers jours de chaque ville) :

| Run MLflow | Accuracy | Macro-F1 | Top-3 |
|---|---|---|---|
| Référence « catégorie la plus fréquente de la zone » | 0,299 | 0,127 | 0,581 |
| `xgb_v1` (sans historique des zones) | 0,297 | 0,097 | 0,608 |
| **`xgb_v2_zone_history`** (modèle retenu) | **0,313** | **0,129** | **0,620** |
| `xgb_v2_zone_history_balanced` | 0,200 | 0,149 | 0,425 |

Lecture : le lieu (historique de la zone) et la nuit comptent, la météo très peu.
Prédire la catégorie exacte reste difficile ; l'usage réaliste est le **top-3**
(« dans cette zone, ce soir, attendez-vous surtout à ces 3 types de crimes »).

### ▶️ Comment lancer

```bash
docker compose up -d --build spark-streaming kafka-ui producer   # remplit Bronze (mode api)
docker compose up -d --build airflow                             # http://localhost:8081  admin/admin
```
Dans Airflow : activer `crimewatch_daily` → ▶️ *Trigger DAG* → vue *Graph* (≈ 5 min).
Résultats ML : `mlflow ui --backend-store-uri sqlite:///mlflow.db` → http://localhost:5000.

Pour récupérer beaucoup d'historique au premier lancement, dans `.env` :
`POLL_INTERVAL_SECONDS=60` et `MAX_RECORDS_PER_CITY=20000` (remettre 300 / 2000 ensuite).

### 🔧 Problèmes rencontrés et corrigés

1. **Météo inutilisable pour la jointure** : le producer récupère la météo *actuelle*, mais les crimes
   ont 1 semaine à 3 mois de retard. → API d'**archives** Open-Meteo sur les dates des crimes.
2. **00:00 = « heure inconnue »** : mesuré en comparant 00:00 pile aux autres heures pile de la nuit
   (2,7x trop à Chicago, 1,6x à LA, normal à NYC). → colonne `hour_known`, exclue pour l'heure.
3. **Catégories incohérentes entre villes** : 22 474 `HARRASSMENT 2` comptés en `public_order` à NYC,
   15-24 % de `other`. Le modèle apprenait « New York → public_order ». → catégories v2.
4. **ML v1 ne battait pas la baseline zone** : le modèle ne « voyait » pas 7 652 zones avec seulement
   latitude/longitude. → historique des zones (target encoding *out-of-fold* + lissage, sans fuite :
   l'historique de la vraie classe vaut 0,356 en train et 0,355 en test).
5. **Airflow : `FileNotFoundException /app/...`** : le journal `_spark_metadata` du streaming contient
   des chemins absolus `/app/...`. → le projet est monté sur **`/app`** dans Airflow aussi.
6. **Airflow : `Permission denied` sur `data/lake/silver`** : dossiers créés par `spark-streaming` (root),
   Airflow tourne avec l'utilisateur 50000. → `docker exec -u root airflow chmod -R a+rwX /app/data/lake`
7. **Airflow : workers Spark avec un autre Python** (« different minor versions ») pour l'UDF des
   catégories. → `PYSPARK_PYTHON` = le Python des jobs dans le DAG.
8. **Tâche arrêtée par SIGTERM** : un *Clear* a été cliqué pendant qu'elle tournait. → utiliser
   *Trigger DAG* et ne pas toucher une tâche en cours.

### ⚠️ Limites connues

1. **New York pèse 83 % des données** (dataset annuel) : le modèle est surtout entraîné sur NYC.
   Les métriques par ville seraient un bon ajout.
2. **Londres est exclu du modèle** (pas d'heure) mais reste dans Silver/Gold et dans `zone_stats`.
3. **`month` n'est pas une feature** : chaque ville couvre des mois différents (biais de confusion).
4. **Lancer les jobs batch seulement via Airflow.** Si on les relance à la main dans `spark-streaming`
   (root), refaire le `chmod` du point 6.
5. **Bronze météo = beaucoup de petits fichiers** (un par minute) : la lecture prend ~40 s.
   Une compaction de Bronze serait utile.
6. **Mots de passe de développement** (`admin/admin`, Postgres `airflow/airflow`) : à changer hors du PC.
7. Le modèle « créneau horaire » prévu au départ n'est pas fait. ⚠️ Piège : `is_night` et la météo
   à l'heure exacte **révèlent l'heure** → fuite de données. N'utiliser que la météo de la journée.

### 🔄 Remettre à zéro le Data Lake batch

```bash
docker exec -u root airflow rm -rf /app/data/lake/silver /app/data/lake/gold
```
Puis relancer le DAG (Bronze n'est pas touché).

---

## Phase 3 (S5 + S6) — Snowflake + dbt + Dashboard + CI

**Responsable :** *(nom)*
**Statut :** pas commencé

### ➡️ À faire

1. **Snowflake + dbt** : charger les tables Gold (`crime_features`, `zone_stats`, `zone_hourly`)
   et construire un schéma en étoile : `fact_crime` + `dim_zone`, `dim_date`, `dim_category`, `dim_city`.
   Tests dbt : `unique` / `not_null` sur `event_id`, `relationships` vers les dimensions.
2. **Dashboard Streamlit** :
   - carte des zones (`zone_stats` : `zone_lat`, `zone_lon`, `crime_count`, `top_category`) ;
   - alertes temps réel (topic `crimes.alerts`, garder la dernière version par `alert_id`) ;
   - top-3 des catégories prédites pour une zone et une heure (modèle MLflow `xgb_v2_zone_history`).
3. **CI GitHub Actions** : `pip install -r requirements-dev.txt` puis `pytest` à chaque push.
4. *(Optionnel)* modèle « créneau horaire » (voir limite 7) et métriques ML par ville.

### 📞 Contact

Questions sur la phase 2 : Mustapha (Silver, Gold, météo, ML, Airflow).
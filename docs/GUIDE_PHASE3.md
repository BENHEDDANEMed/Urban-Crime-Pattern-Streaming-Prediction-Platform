# Guide Phase 3 — Comprendre le code (S5 + S6)

Ce guide explique **simplement** ce que fait chaque partie de la phase 3.
Lis-le dans l'ordre : chaque partie utilise la précédente. À la fin de chaque partie,
une petite **question pour vérifier** (réponses tout en bas).

---

## 1. L'idée en une image

Les phases 1 et 2 fabriquent des **fichiers** (Parquet). La phase 3 les transforme en
**tables bien rangées** et les **montre** :

```
 Gold (Parquet)          dbt                          Entrepôt                 Dashboard
 ┌──────────────┐   ┌─────────────────────┐   ┌────────────────────────┐   ┌───────────────┐
 │crime_features│──►│ staging : nettoyer  │──►│ fact_crime             │──►│ 🗺️ carte       │
 │zone_stats    │   │ marts   : ranger en │   │ dim_city, dim_zone ... │   │ 🔮 prédictions │
 │zone_hourly   │   │           étoile    │   │ mart_* (analyses)      │   │ 📊 analyses    │
 │zone_predict. │   │ tests   : vérifier  │   │ DuckDB  ou  Snowflake  │   └───────────────┘
 └──────────────┘   └─────────────────────┘   └────────────────────────┘          ▲
                                                                                   │
                                        Kafka crimes.alerts (temps réel) ──────────┘ 🚨 alertes
```

- **dbt** = le cuisinier : il prend les ingrédients (Gold), suit des recettes SQL et vérifie le goût (tests).
- **DuckDB / Snowflake** = le frigo où on range les plats prêts.
- **Streamlit** = la vitrine : ce que voit l'utilisateur.

---

## 2. dbt en 5 minutes

Sans dbt, on écrirait des scripts qui font `CREATE TABLE ... AS SELECT ...` dans un ordre précis,
à la main. Avec dbt :

- **1 fichier `.sql` = 1 table (ou vue).** On écrit seulement le `SELECT`.
- **`{{ ref('stg_crimes') }}`** au lieu du nom de la table. dbt en déduit que ce modèle
  *dépend* de `stg_crimes` → il construit tout **dans le bon ordre** et dessine le graphe (lineage).
  Il remplace aussi `ref()` par le bon nom complet (`analytics.staging.stg_crimes` dans DuckDB,
  `CRIMEWATCH.STAGING.STG_CRIMES` dans Snowflake) : **le même SQL marche partout**.
- **`{{ source('gold', 'crime_features') }}`** = une donnée qui vient de *l'extérieur* de dbt (le Gold).
- **Les tests** (`unique`, `not_null`, `relationships`...) s'écrivent en YAML à côté des modèles.
- **`dbt build`** = tout faire d'un coup : seeds → modèles → tests, dans l'ordre du graphe.
  Si un test échoue, les modèles qui en dépendent ne sont pas construits.

> ❓ **Question 1** : pourquoi `ref()` plutôt que le nom de la table écrit en dur ? (2 raisons)

---

## 3. Le projet `crimewatch_dbt/` fichier par fichier

```
crimewatch_dbt/
├── dbt_project.yml        la "carte d'identité" : nom, profil, dossiers, matérialisation par dossier
├── profiles.yml           les connexions (DuckDB / Snowflake) — que des variables d'environnement
├── requirements.txt       dbt-core + dbt-duckdb
├── macros/
│   └── generate_schema_name.sql   des schémas propres : staging, reference, marts
├── seeds/                 petits CSV chargés tels quels (tables de référence)
│   ├── cities.csv              les 4 villes (fuseau, source, heure fiable ?)
│   ├── crime_categories.csv    les 16 catégories + libellé français + famille
│   └── hours.csv               0-23 h + créneau (nuit / matin / après-midi / soir)
├── models/
│   ├── staging/           1 vue par table du lake : renommer, typer, rien de plus
│   │   ├── _sources.yml        OÙ sont les données (Parquet du lake, ou schéma RAW de Snowflake)
│   │   └── stg_*.sql
│   └── marts/             ce que lit le dashboard (de vraies tables)
│       ├── dim_*.sql           les dimensions
│       ├── fact_*.sql          les faits
│       └── mart_*.sql          les tables d'analyse
└── tests/                 tests "sur mesure" en SQL
```

**`dbt_project.yml`** dit à dbt : « je m'appelle `crimewatch`, utilise le profil `crimewatch`,
les modèles de `staging/` sont des **vues**, ceux de `marts/` des **tables** ».
Pourquoi cette différence ? Le staging ne fait que renommer (une vue ne coûte rien à créer) ;
les marts sont lus souvent par le dashboard (une table est plus rapide à lire).

**`profiles.yml`** est *dans* le projet (normalement il est dans `~/.dbt/`). C'est possible parce qu'il
ne contient **aucun mot de passe** : seulement `{{ env_var('SNOWFLAKE_PASSWORD', ...) }}`.
Avantage : la CI, Airflow et tes coéquipiers ont la même config sans rien copier.

**La macro `generate_schema_name`** : par défaut, dbt nommerait le schéma `analytics_marts`
(schéma du profil + schéma du dossier). La macro garde juste `marts`.

**`_sources.yml` et `external_location`** : avec DuckDB, dbt ne copie pas les Parquet, il les lit
directement avec `read_parquet('../data/lake/gold/crime_features/*/*.parquet', hive_partitioning = true)`.
`hive_partitioning` transforme les dossiers `city=chicago/` en colonne `city`.
Avec Snowflake, ce réglage est ignoré : dbt lit la table `RAW.CRIME_FEATURES`.

> ❓ **Question 2** : pourquoi le fichier `crimewatch.duckdb` ne doit **pas** aller sur GitHub ?

---

## 4. Le schéma en étoile

```
                    dim_date              dim_hour
                  (1 ligne/jour)       (0-23 + créneau)
                         │                    │
       dim_city ─────────┼──── fact_crime ────┼───────── dim_category
     (4 villes)          │  (1 ligne/crime)   │        (16 catégories)
                         │                    │
                     dim_zone
                (case de ~500 m)
```

- **Fait** = ce qu'on compte (un crime). Beaucoup de lignes, surtout des **clés** + des mesures.
- **Dimension** = ce qui décrit (une ville, un jour, une catégorie). Peu de lignes, beaucoup de texte.
- Pourquoi ? Pour répondre à « combien de crimes violents le samedi soir à Chicago ? »,
  on part de `fact_crime` et on joint `dim_category` (violent ?), `dim_date` (samedi ?),
  `dim_hour` (soir ?). Toutes les questions se posent de la même façon.

**Le piège de l'heure** : `fact_crime.hour_of_day` est **NULL** quand l'heure n'est pas fiable
(Londres : seulement le mois ; crimes à 00:00 pile, voir HANDOFF phase 2). Toute analyse « par heure »
ignore donc automatiquement ces crimes. Le test `relationships` vers `dim_hour` ignore les NULL.

**Les tables d'analyse (`mart_*`)** préparent ce dont le dashboard a besoin :

| Table | Question |
|---|---|
| `mart_city_comparison` | Chicago vs LA vs NYC vs Londres : volume, nuit, week-end, violents |
| `mart_city_daily` | Comment évolue le nombre de crimes jour après jour ? |
| `mart_hourly_profile` | À quelle heure y a-t-il le plus de crimes, en semaine / le week-end ? |
| `mart_weather_impact` | Y a-t-il plus de crimes quand il pleut / fait chaud ? |
| `mart_zone_predictions` | Que prédit le modèle pour cette zone à cette heure ? |

**`mart_weather_impact`, le modèle le plus subtil.** Compter « 300 crimes sous la pluie, 3 000 au sec »
ne prouve rien : il pleut juste moins souvent. On compte donc aussi les **heures** de chaque condition
(météo Silver) et on calcule des crimes **par heure**. Deuxième piège : il fait plus chaud l'après-midi,
et il y a aussi plus de crimes l'après-midi. `index_vs_same_hour` compare donc aux crimes *attendus
à ces mêmes heures de la journée*. Index = 1 → la météo ne change rien.

> ❓ **Question 3** : dans `mart_hourly_profile`, pourquoi diviser par le nombre de jours de
> semaine / de week-end au lieu de comparer les totaux ?

---

## 5. Les tests de données

`dbt build` construit 3 seeds + 17 modèles et lance **50 tests de données**. Trois familles :

1. **Tests génériques** (YAML) :
   - `unique` + `not_null` sur chaque clé (`event_id`, `zone_id`, `city`...)
   - `relationships` : chaque `fact_crime.zone_id` existe dans `dim_zone`, etc.
     → c'est la définition d'une **clé étrangère**, vérifiée sur les données.
   - `accepted_values` : `weather_level` vaut `hourly`, `daily` ou `none`.
2. **Tests sur mesure** (`tests/*.sql`) : une requête qui renvoie les lignes **en erreur**.
   0 ligne = test vert.
   - `assert_zone_hourly_matches_fact` : Gold `zone_hourly` compte bien les mêmes crimes que `fact_crime`.
   - `assert_crime_inside_its_zone` : chaque crime est bien dans sa case de la grille.
   - `assert_predictions_are_top3` : 3 prédictions par scénario, probabilités décroissantes.
3. **Test pytest** (`tests/test_dbt_project.py`) : les 16 catégories du seed = les 16 du code Python.
   Si quelqu'un ajoute une catégorie dans `common/schemas.py` et oublie le seed, la CI le voit.

> ❓ **Question 4** : un test sur mesure dbt « passe » quand la requête renvoie combien de lignes ?

---

## 6. Les prédictions (`ml/predict_zones.py`)

Le modèle de la phase 2 sait dire, **pour un crime donné**, de quelle catégorie il est probablement.
Le dashboard veut autre chose : « **dans cette zone, ce samedi à 21 h**, à quoi s'attendre ? ».

Le script :
1. récupère le **dernier run MLflow** `xgb_v2_zone_history` (modèle + liste des classes + liste des features) ;
2. fabrique des **scénarios** : 100 zones les plus touchées × 24 heures × {mercredi, samedi},
   avec la météo *habituelle* de la ville à cette heure, sans pluie ;
3. recalcule l'**historique des zones** (`te_*`) sur tout l'historique ;
4. garde le **top-3** → `gold/zone_predictions` (3 lignes par scénario).

`train_category.py` a aussi gagné des **métriques par ville** (`accuracy_chicago`, `top3_accuracy_new_york`...)
dans MLflow : New York pèse 83 % des données, la moyenne globale cachait les autres villes.

---

## 7. Snowflake

DuckDB tourne sur ton PC, gratuit et sans compte : parfait pour développer et pour la CI.
Snowflake = le même rôle, **dans le cloud**, partagé par toute une équipe.

Pour passer à Snowflake (essai gratuit 30 jours) :
1. remplir `SNOWFLAKE_*` dans `.env` ;
2. `python -m warehouse.load_snowflake` → copie Gold + météo dans le schéma `RAW` ;
3. `cd crimewatch_dbt` puis `dbt build --target snowflake` → **le même projet dbt**, sans changer une ligne de SQL.

Le SQL des modèles évite les fonctions propres à une seule base (`extract(...)`, `date_trunc('hour', ...)`,
`count(distinct ...)` existent dans les deux). C'est ce qui rend la bascule possible.

---

## 8. Airflow : le DAG complet

```
bronze_to_silver → check_silver → weather_history → silver_to_gold → check_gold
    → train_category → predict_zones → load_snowflake → dbt_build
```

- `predict_zones` : section 6.
- `load_snowflake` : ne fait **rien** si `SNOWFLAKE_ACCOUNT` est vide (tâche verte quand même).
- `dbt_build` : construit `data/warehouse/crimewatch.duckdb` (ou Snowflake si `DBT_TARGET=snowflake`).
  Si un test dbt échoue → tâche **rouge** : on ne publie pas des données fausses au dashboard.

dbt a **son propre environnement Python** dans l'image Airflow (`/opt/dbt-venv`), comme les jobs
(`/opt/jobs-venv`) : dbt, MLflow et Airflow ont des dépendances qui se disputent les versions.

---

## 9. Le dashboard (`dashboard/`)

- `data.py` : **toutes** les lectures.
  - DuckDB ouvert **en lecture seule** et refermé à chaque requête. Pourquoi ? DuckDB est un fichier :
    tant qu'un programme le garde ouvert, dbt (Airflow) ne peut pas le réécrire.
  - `@st.cache_data(ttl=300)` : un résultat est gardé 5 min → le dashboard reste rapide.
  - Alertes : lecture de **tout** le topic `crimes.alerts` et on garde la **dernière version de chaque
    `alert_id`** (une alerte est renvoyée quand son hotspot grandit, voir le contrat de données).
- `app.py` : l'affichage (4 onglets). Il ne contient **aucun calcul lourd** : tout est déjà prêt
  dans les tables `mart_*`. C'est le rôle de dbt.

---

## 10. La CI (`.github/workflows/ci.yml`)

À chaque push / Pull Request, GitHub lance 2 jobs sur une machine neuve :

1. **tests** : `pytest` (33 tests, sans internet ni Kafka).
2. **dbt** : le batch **de bout en bout** sur des données synthétiques :
   `generate_gold_fixture.py` → `train_category` → `predict_zones` → `dbt build`.

Pourquoi des données synthétiques ? Les vraies données ne sont pas sur GitHub (trop lourdes).
`scripts/generate_gold_fixture.py` écrit un petit Gold avec **exactement** les mêmes colonnes,
types et dossiers `city=` que Spark. Si quelqu'un casse un modèle dbt ou le script de prédiction,
la Pull Request passe au rouge avant d'arriver sur `main`.

---

## 11. Commandes utiles

```powershell
# dbt (depuis crimewatch_dbt/, venv activé)
dbt debug                        # la connexion marche ?
dbt build                        # tout : seeds + modèles + tests
dbt build -s fact_crime+         # fact_crime et tout ce qui en dépend
dbt test -s dim_zone             # seulement les tests de dim_zone
dbt docs generate; dbt docs serve   # documentation + graphe de lineage (http://localhost:8080)

# Tout tester sans Docker, sur des données synthétiques (depuis la racine du projet)
python scripts/generate_gold_fixture.py
$env:LAKE_PATH="data/fixture"; $env:MLFLOW_TRACKING_URI="sqlite:///data/fixture/mlflow.db"
python -m ml.train_category; python -m ml.predict_zones --top-zones 20
cd crimewatch_dbt; $env:CRIMEWATCH_LAKE="../data/fixture"; $env:DUCKDB_PATH="../data/fixture/crimewatch.duckdb"; dbt build; cd ..
$env:DUCKDB_PATH="data/fixture/crimewatch.duckdb"; streamlit run dashboard/app.py
```

⚠️ `dbt docs serve` utilise le port 8080, comme Kafka UI : arrête Kafka UI ou ajoute `--port 8090`.

---

## Réponses

1. dbt **connaît l'ordre** de construction (et dessine le lineage) ; le **nom complet** de la table
   change selon la base (DuckDB / Snowflake) et dbt l'écrit pour nous.
2. C'est une **donnée générée** (reconstruite par `dbt build`), elle peut être lourde, et elle change à
   chaque exécution → conflits Git permanents. Dans Git on met le **code** qui la fabrique.
3. Il y a 5 jours de semaine pour 2 de week-end : les totaux de semaine seraient ~2,5 fois plus grands
   même si chaque jour se ressemble. La moyenne **par jour** rend les deux comparables.
4. **0 ligne.** La requête cherche les erreurs.

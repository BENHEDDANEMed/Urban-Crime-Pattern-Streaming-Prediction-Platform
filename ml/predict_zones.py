"""
Phase 3 : prédictions du modèle pour le dashboard  ->  data/lake/gold/zone_predictions

Question à laquelle on répond :
  « Dans cette zone, tel type de jour, à telle heure : quels sont les 3 types de crimes
    les plus probables ? »  (c'est l'usage réaliste du modèle : top-3 = 0,62)

Comment :
  1. On prend le DERNIER run MLflow `xgb_v2_zone_history` (le modèle retenu en phase 2)
     et sa liste de classes (classes.json) et de features (paramètre "features").
  2. On construit des "scénarios" : pour les N zones les plus touchées de chaque ville,
     chaque heure 0-23, un jour de semaine (mercredi) et un jour de week-end (samedi).
     Météo du scénario = météo habituelle de la ville à cette heure (médiane), sans pluie.
  3. Les features "historique de la zone" (zone_n, te_*) sont recalculées sur TOUT
     l'historique (comme le ferait le modèle en production).
  4. On garde les 3 catégories les plus probables -> 3 lignes par scénario.

Colonnes écrites (partition city=) :
  zone_id, day_type ('semaine' | 'week-end'), hour_of_day, prediction_rank (1-3),
  crime_category, probability, model_run_id, scored_at (UTC)

Lancement (après ml/train_category.py) :
  python -m ml.predict_zones
"""
import argparse
import logging
import os
import shutil
from datetime import datetime, timezone

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd

from ml.train_category import CITIES, EXPERIMENT, LAKE, TRACKING_URI, load_data, prepare_x, zone_shares

log = logging.getLogger("predict_zones")

RUN_NAME = os.getenv("PREDICT_RUN_NAME", "xgb_v2_zone_history")
TOP_ZONES = int(os.getenv("PREDICT_TOP_ZONES", "100"))
TOP_K = 3
# type de jour -> (day_of_week au format Spark : 1 = dimanche ... 7 = samedi, is_weekend)
DAY_TYPES = {"semaine": (4, 0), "week-end": (7, 1)}   # mercredi, samedi


def latest_run(run_name: str = RUN_NAME) -> pd.Series:
    """Le dernier run terminé qui porte ce nom dans l'expérience MLflow."""
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT],
        filter_string=f"tags.mlflow.runName = '{run_name}' and attributes.status = 'FINISHED'",
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )
    if runs.empty:
        raise SystemExit(f"Aucun run MLflow '{run_name}' : lancer d'abord python -m ml.train_category")
    return runs.iloc[0]


def typical_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Météo habituelle par (ville, heure) : médiane température / vent, pas de pluie."""
    by_hour = (df.groupby(["city", "occurred_hour"])
               .agg(temperature_c=("temperature_c", "median"),
                    wind_speed_kmh=("wind_speed_kmh", "median"),
                    night_share=("is_night", "mean"))
               .reset_index())
    grid = pd.MultiIndex.from_product([sorted(df["city"].unique()), range(24)],
                                      names=["city", "occurred_hour"]).to_frame(index=False)
    weather = grid.merge(by_hour, on=["city", "occurred_hour"], how="left")
    # heure jamais vue dans une ville : médiane de la ville
    for col in ["temperature_c", "wind_speed_kmh"]:
        weather[col] = weather[col].fillna(weather.groupby("city")[col].transform("median"))
    default_night = ((weather["occurred_hour"] < 7) | (weather["occurred_hour"] >= 19)).astype(float)
    weather["is_night"] = (weather["night_share"].fillna(default_night) >= 0.5).astype(int)
    weather["precipitation_mm"] = 0.0
    weather["is_raining"] = 0
    return weather.drop(columns="night_share")


def build_scenarios(df: pd.DataFrame, top_zones: int = TOP_ZONES) -> pd.DataFrame:
    """Zones les plus touchées x 24 heures x {semaine, week-end}."""
    zones = (df.groupby(["city", "zone_id", "zone_lat", "zone_lon"]).size().rename("n").reset_index()
             .sort_values(["city", "n", "zone_id"], ascending=[True, False, True])
             .groupby("city").head(top_zones)
             .drop(columns="n"))
    hours = pd.DataFrame({"occurred_hour": range(24)})
    days = pd.DataFrame([{"day_type": k, "day_of_week": v[0], "is_weekend": v[1]} for k, v in DAY_TYPES.items()])
    scen = zones.merge(hours, how="cross").merge(days, how="cross")
    return scen.merge(typical_weather(df), on=["city", "occurred_hour"], how="left")


def top_k_long(scen: pd.DataFrame, proba: np.ndarray, classes: list, k: int = TOP_K) -> pd.DataFrame:
    """Probabilités (n_scénarios x n_classes) -> k lignes par scénario, rang 1 = la plus probable."""
    order = np.argsort(-proba, axis=1)[:, :k]
    rows = []
    for rank in range(k):
        idx = order[:, rank]
        rows.append(pd.DataFrame({
            "city": scen["city"].to_numpy(),
            "zone_id": scen["zone_id"].to_numpy(),
            "day_type": scen["day_type"].to_numpy(),
            "hour_of_day": scen["occurred_hour"].to_numpy().astype("int32"),
            "prediction_rank": np.int32(rank + 1),
            "crime_category": np.asarray(classes)[idx],
            "probability": np.round(proba[np.arange(len(scen)), idx], 4),
        }))
    return pd.concat(rows, ignore_index=True)


def predict(df: pd.DataFrame, model, classes: list, features: list, top_zones: int = TOP_ZONES) -> pd.DataFrame:
    scen = build_scenarios(df, top_zones)
    x = prepare_x(scen)
    if any(f.startswith("te_") for f in features):            # modèle avec historique des zones
        x = x.join(scen[["zone_id"]].join(zone_shares(df, classes), on="zone_id").drop(columns="zone_id"))
    proba = model.predict_proba(x[features])
    return top_k_long(scen, proba, classes)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-zones", type=int, default=TOP_ZONES, help="zones par ville (les plus touchées)")
    args = parser.parse_args()

    mlflow.set_tracking_uri(TRACKING_URI)
    run = latest_run()
    run_id = run["run_id"]
    model = mlflow.xgboost.load_model(f"runs:/{run_id}/model")
    classes = mlflow.artifacts.load_dict(f"runs:/{run_id}/classes.json")["classes"]
    features = run["params.features"].split(",")
    log.info("Modèle : run %s (%s), %d classes, %d features", run_id, RUN_NAME, len(classes), len(features))

    df = load_data()
    out = predict(df, model, classes, features, args.top_zones)
    out["model_run_id"] = run_id
    out["scored_at"] = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))

    path = f"{LAKE}/gold/zone_predictions"
    shutil.rmtree(path, ignore_errors=True)        # on réécrit tout : relancer = même résultat
    out.to_parquet(path, partition_cols=["city"], index=False)
    log.info("Gold zone_predictions : %d lignes (%d scénarios) -> %s", len(out), len(out) // TOP_K, path)
    for city in CITIES:
        part = out[(out["city"] == city) & (out["prediction_rank"] == 1)]
        if len(part):
            log.info("%-12s | n°1 le plus souvent prédit : %s", city, part["crime_category"].value_counts().idxmax())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    main()

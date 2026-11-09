"""
Phase 2 (S4) : Machine Learning -> prédire la CATÉGORIE d'un crime  (XGBoost + MLflow)

Question : "Sachant OÙ, QUAND et PAR QUEL TEMPS un crime a lieu, de quel type est-il ?"
           (vol, agression, cambriolage, vol de voiture...)

Données : data/lake/gold/crime_features
  - on garde Chicago, Los Angeles et New York avec hour_known = 1
    (Londres et les crimes à 00:00 n'ont pas d'heure fiable : la feature "heure" serait fausse)

Features (toutes connues au moment du crime, aucune ne dépend de la réponse) :
  ville, heure, jour de la semaine, week-end, nuit, position (zone), température, pluie, vent
  -> "month" n'est PAS utilisé : chaque ville couvre des mois différents
     (New York = juin-septembre, Chicago = octobre). Le modèle apprendrait
     "octobre = Chicago" au lieu d'un vrai effet du mois.

Découpage temporel (jamais aléatoire) : pour chaque ville, les 80 % premiers jours
servent à l'entraînement, les 20 % derniers au test. On prédit donc le futur à partir du passé.

Version 2 : historique des zones ("target encoding")
  Pour chaque zone, on calcule la part de chaque catégorie dans le PASSÉ
  ("dans cette zone : 45 % de vols, 20 % d'agressions...") et on la donne au modèle.
  Sans fuite de données :
    - test  : parts calculées sur TOUT l'entraînement (le passé)
    - train : parts calculées SANS les jours de la ligne (5 groupes de jours, "out-of-fold"),
              sinon le modèle verrait sa propre réponse dans ses features
  Lissage : une zone avec peu de crimes est ramenée vers la moyenne de sa ville.

Le modèle doit battre deux références simples :
  - baseline_city : "toujours la catégorie la plus fréquente de la ville"
  - baseline_zone : "la catégorie la plus fréquente de la zone dans le passé"

Lancement (venv activé, depuis la racine du projet) :
  python -m ml.train_category
  python -m ml.train_category --balanced        # donne plus de poids aux catégories rares
  python -m ml.train_category --no-zone-history # version 1 (pour comparer)
Résultats : mlflow ui --backend-store-uri sqlite:///mlflow.db   -> http://localhost:5000
"""
from __future__ import annotations

import argparse
import logging
import os

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, top_k_accuracy_score
from sklearn.utils.class_weight import compute_sample_weight
from xgboost import XGBClassifier

log = logging.getLogger("train_category")

LAKE = os.getenv("LAKE_PATH", "data/lake")
TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
EXPERIMENT = "crimewatch-category"
CITIES = ["chicago", "los_angeles", "new_york"]
TARGET = "crime_category"
FEATURES = [
    "city", "occurred_hour", "day_of_week", "is_weekend", "is_night",
    "zone_lat", "zone_lon", "temperature_c", "precipitation_mm", "wind_speed_kmh", "is_raining",
]
TEST_SHARE = 0.2
N_FOLDS = 5          # out-of-fold : 5 groupes de jours
SMOOTHING = 20       # une zone "vaut" au moins 20 crimes de la moyenne de sa ville


def load_data(lake: str = LAKE) -> pd.DataFrame:
    df = pd.read_parquet(f"{lake}/gold/crime_features")
    df["city"] = df["city"].astype(str)
    df = df[df["city"].isin(CITIES) & (df["hour_known"] == 1)].copy()
    df["occurred_date"] = pd.to_datetime(df["occurred_date"])
    return df.dropna(subset=[TARGET])


def temporal_split(df: pd.DataFrame, test_share: float = TEST_SHARE):
    """Pour chaque ville : les derniers jours vont dans le test."""
    is_test = pd.Series(False, index=df.index)
    for city, part in df.groupby("city"):
        days = np.sort(part["occurred_date"].unique())
        cutoff = days[int(len(days) * (1 - test_share))]
        is_test[part.index] = part["occurred_date"] >= cutoff
        log.info("%-12s | train < %s <= test | %d jours", city, pd.Timestamp(cutoff).date(), len(days))
    return df[~is_test], df[is_test]


def prepare_x(df: pd.DataFrame) -> pd.DataFrame:
    x = df[FEATURES].copy()
    x["city"] = pd.Categorical(x["city"], categories=CITIES)   # XGBoost gère les catégories
    return x


def zone_shares(history: pd.DataFrame, classes: list, m: float = SMOOTHING) -> pd.DataFrame:
    """
    Pour chaque zone : part lissée de chaque catégorie, calculée sur `history`.
        part = (crimes de la catégorie dans la zone + m * part dans la ville) / (crimes de la zone + m)
    Renvoie un tableau indexé par zone_id : zone_n, te_theft, te_assault, ...
    """
    city_prior = pd.crosstab(history["city"], history[TARGET], normalize="index") \
        .reindex(columns=classes, fill_value=0.0)
    counts = pd.crosstab(history["zone_id"], history[TARGET]).reindex(columns=classes, fill_value=0)
    n = counts.sum(axis=1)
    zone_city = history.groupby("zone_id")["city"].first()
    prior = city_prior.loc[zone_city.loc[counts.index]].set_axis(counts.index)
    shares = (counts + m * prior).div(n + m, axis=0)
    shares.columns = [f"te_{c}" for c in classes]
    shares.insert(0, "zone_n", n)
    return shares


def add_zone_history(train: pd.DataFrame, test: pd.DataFrame, classes: list):
    """Ajoute les colonnes zone_n + te_* sans fuite (voir le haut du fichier)."""
    days = np.sort(train["occurred_date"].unique())
    fold_of_day = pd.Series(np.arange(len(days)) % N_FOLDS, index=days)
    fold = train["occurred_date"].map(fold_of_day).to_numpy()

    parts = []
    for k in range(N_FOLDS):
        past = train[fold != k]          # les autres groupes de jours
        target = train[fold == k]
        enc = zone_shares(past, classes)
        parts.append(target[["zone_id"]].join(enc, on="zone_id"))
    train_enc = pd.concat(parts).loc[train.index]
    test_enc = test[["zone_id"]].join(zone_shares(train, classes), on="zone_id")

    def fill(enc: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
        """Zone jamais vue : 0 crime et la moyenne de la ville."""
        prior = pd.crosstab(train["city"], train[TARGET], normalize="index") \
            .reindex(columns=classes, fill_value=0.0)
        prior.columns = [f"te_{c}" for c in classes]
        city_prior = prior.loc[df["city"]].set_axis(df.index)
        enc = enc.drop(columns="zone_id")
        enc["zone_n"] = enc["zone_n"].fillna(0)
        return enc.fillna(city_prior)

    return fill(train_enc, train), fill(test_enc, test)


def baselines(train: pd.DataFrame, test: pd.DataFrame) -> dict:
    city_top = train.groupby("city")[TARGET].agg(lambda s: s.value_counts().idxmax())
    zone_top = train.groupby("zone_id")[TARGET].agg(lambda s: s.value_counts().idxmax())
    pred_city = test["city"].map(city_top)
    pred_zone = test["zone_id"].map(zone_top).fillna(pred_city)   # zone jamais vue -> ville
    zone_top3 = train.groupby("zone_id")[TARGET].agg(lambda s: list(s.value_counts().index[:3]))
    city_top3 = train.groupby("city")[TARGET].agg(lambda s: list(s.value_counts().index[:3]))
    top3 = [z if isinstance(z, list) else c
            for z, c in zip(test["zone_id"].map(zone_top3), test["city"].map(city_top3))]
    return {
        "baseline_zone_top3_accuracy": float(np.mean([y in t for y, t in zip(test[TARGET], top3)])),
        "baseline_city_accuracy": accuracy_score(test[TARGET], pred_city),
        "baseline_zone_accuracy": accuracy_score(test[TARGET], pred_zone),
        "baseline_zone_macro_f1": f1_score(test[TARGET], pred_zone, average="macro"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--balanced", action="store_true",
                        help="pondérer les catégories rares (améliore le macro-F1, baisse l'accuracy)")
    parser.add_argument("--no-zone-history", action="store_true",
                        help="version 1 : sans l'historique des zones (pour comparer)")
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--learning-rate", type=float, default=0.1)
    args = parser.parse_args()

    df = load_data()
    train, test = temporal_split(df)
    log.info("Train : %d crimes | Test : %d crimes", len(train), len(test))

    classes = sorted(train[TARGET].unique())
    code = {c: i for i, c in enumerate(classes)}
    test = test[test[TARGET].isin(classes)]       # catégorie jamais vue à l'entraînement : ignorée
    y_train = train[TARGET].map(code).to_numpy()
    y_test = test[TARGET].map(code).to_numpy()

    x_train, x_test = prepare_x(train), prepare_x(test)
    if not args.no_zone_history:
        enc_train, enc_test = add_zone_history(train, test, classes)
        x_train = pd.concat([x_train, enc_train], axis=1)
        x_test = pd.concat([x_test, enc_test], axis=1)
    features = list(x_train.columns)

    params = {
        "n_estimators": args.n_estimators,
        "max_depth": args.max_depth,
        "learning_rate": args.learning_rate,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "tree_method": "hist",
        "enable_categorical": True,
        "objective": "multi:softprob",
        "eval_metric": "mlogloss",
    }

    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT)
    run_name = ("xgb_v1" if args.no_zone_history else "xgb_v2_zone_history") + \
        ("_balanced" if args.balanced else "")
    with mlflow.start_run(run_name=run_name):
        weights = compute_sample_weight("balanced", y_train) if args.balanced else None
        model = XGBClassifier(**params)
        model.fit(x_train, y_train, sample_weight=weights)

        proba = model.predict_proba(x_test)
        pred = proba.argmax(axis=1)
        metrics = {
            "accuracy": accuracy_score(y_test, pred),
            "macro_f1": f1_score(y_test, pred, average="macro"),
            "top3_accuracy": top_k_accuracy_score(y_test, proba, k=3, labels=range(len(classes))),
            **baselines(train, test),
        }
        # Métriques par ville (New York pèse ~83 % des données : la moyenne globale le cache)
        for city in CITIES:
            in_city = (test["city"] == city).to_numpy()
            if in_city.sum() > 0:
                metrics[f"accuracy_{city}"] = accuracy_score(y_test[in_city], pred[in_city])
                metrics[f"top3_accuracy_{city}"] = top_k_accuracy_score(
                    y_test[in_city], proba[in_city], k=3, labels=range(len(classes)))

        mlflow.log_params({**params, "balanced": args.balanced, "zone_history": not args.no_zone_history,
                           "features": ",".join(features),
                           "cities": ",".join(CITIES), "test_share": TEST_SHARE,
                           "n_train": len(train), "n_test": len(test), "n_classes": len(classes)})
        mlflow.log_metrics(metrics)
        importance = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
        mlflow.log_dict(importance.round(4).to_dict(), "feature_importance.json")
        mlflow.log_dict({"classes": classes}, "classes.json")
        mlflow.xgboost.log_model(model, name="model")

        print("\n=== Résultats sur le test (derniers jours de chaque ville) ===")
        for k, v in metrics.items():
            print(f"{k:25s} {v:.3f}")
        print("\n=== Importance des features ===")
        print(importance.head(15).round(3).to_string())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    main()
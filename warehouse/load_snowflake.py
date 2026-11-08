"""
Phase 3 : charger les tables Gold (+ météo Silver) du Data Lake dans Snowflake (schéma RAW).

Ensuite dbt construit le schéma en étoile dans Snowflake, exactement comme avec DuckDB :
  python -m warehouse.load_snowflake
  cd crimewatch_dbt && dbt build --target snowflake

Configuration (fichier .env, jamais sur GitHub) :
  SNOWFLAKE_ACCOUNT    ex : abc12345.eu-west-1   (Snowsight -> menu du compte -> "Account identifier")
  SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
  SNOWFLAKE_ROLE       (défaut ACCOUNTADMIN), SNOWFLAKE_WAREHOUSE (défaut COMPUTE_WH)
  SNOWFLAKE_DATABASE   (défaut CRIMEWATCH, créée si besoin)

Si SNOWFLAKE_ACCOUNT est vide, le script ne fait rien (exit 0) : la tâche Airflow
reste verte tant que Snowflake n'est pas configuré.

Chaque table est remplacée en entier (overwrite) : relancer = même résultat.
Dépendance : pip install "snowflake-connector-python[pandas]"
"""
import logging
import os
import sys

import pandas as pd

log = logging.getLogger("load_snowflake")

LAKE = os.getenv("LAKE_PATH", "data/lake")
TABLES = {
    "CRIME_FEATURES": "gold/crime_features",
    "ZONE_STATS": "gold/zone_stats",
    "ZONE_HOURLY": "gold/zone_hourly",
    "ZONE_PREDICTIONS": "gold/zone_predictions",
    "WEATHER_HOURLY": "silver/weather_hourly",
}


def read_table(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if "city" in df.columns:                       # colonne de partition -> texte normal
        df["city"] = df["city"].astype(str)
    df.columns = [c.upper() for c in df.columns]   # Snowflake : identifiants en majuscules
    return df


def main() -> None:
    account = os.getenv("SNOWFLAKE_ACCOUNT", "").strip()
    if not account:
        log.warning("SNOWFLAKE_ACCOUNT vide : chargement Snowflake ignoré (DuckDB seulement).")
        return

    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    database = os.getenv("SNOWFLAKE_DATABASE", "CRIMEWATCH")
    conn = snowflake.connector.connect(
        account=account,
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    try:
        cur = conn.cursor()
        cur.execute(f"create database if not exists {database}")
        cur.execute(f"create schema if not exists {database}.RAW")
        cur.execute(f"use schema {database}.RAW")
        for table, rel_path in TABLES.items():
            path = f"{LAKE}/{rel_path}"
            if not os.path.exists(path):
                log.warning("%s absent (%s) : ignoré", table, path)
                continue
            df = read_table(path)
            ok, _, nrows, _ = write_pandas(conn, df, table, database=database, schema="RAW",
                                           auto_create_table=True, overwrite=True,
                                           quote_identifiers=False, use_logical_type=True)
            if not ok:
                sys.exit(f"Échec du chargement de {table}")
            log.info("RAW.%-17s %8d lignes", table, nrows)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    main()

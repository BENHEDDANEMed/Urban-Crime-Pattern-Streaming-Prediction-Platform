"""
Lecture des données du dashboard.

- Historique + prédictions : la base DuckDB construite par dbt (schéma `marts`).
  Une connexion EN LECTURE SEULE est ouverte puis fermée à chaque requête : la base reste
  libre pour que dbt (Airflow) puisse la réécrire. Les résultats sont gardés 5 min en cache.
- Alertes temps réel : le topic Kafka `crimes.alerts` (dernière version de chaque alert_id).
"""
import json
import os
import uuid
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", ROOT / "data" / "warehouse" / "crimewatch.duckdb"))
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALERTS_TOPIC = os.getenv("TOPIC_CRIMES_ALERTS", "crimes.alerts")


class WarehouseMissing(Exception):
    pass


@st.cache_data(ttl=300, show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    if not DUCKDB_PATH.exists():
        raise WarehouseMissing(str(DUCKDB_PATH))
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        df = con.execute(sql, list(params)).df()
    finally:
        con.close()
    # sum() DuckDB -> int128 : on repasse en types pandas simples
    for col in df.columns:
        if str(df[col].dtype) == "object" and len(df) and isinstance(df[col].iloc[0], int):
            df[col] = df[col].astype("int64")
    return df


@st.cache_data(ttl=20, show_spinner=False)
def load_alerts(max_messages: int = 5000, idle_seconds: float = 1.5) -> pd.DataFrame:
    """
    Lit tout le topic crimes.alerts depuis le début (nouveau groupe à chaque lecture,
    rien n'est commité) et garde la DERNIÈRE version de chaque alert_id :
    une alerte est renvoyée quand son hotspot grandit (voir docs/CONTRAT_DONNEES.md § 3).
    Renvoie un DataFrame vide si Kafka ne répond pas.
    """
    from confluent_kafka import Consumer, KafkaException

    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": f"dashboard-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "socket.timeout.ms": 3000,
        "session.timeout.ms": 6000,
    })
    alerts: dict[str, dict] = {}
    try:
        consumer.list_topics(ALERTS_TOPIC, timeout=3)     # lève une erreur si Kafka est éteint
        consumer.subscribe([ALERTS_TOPIC])
        empty_polls, received = 0, 0
        while received < max_messages and empty_polls * 0.5 < idle_seconds + 3:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                empty_polls += 1
                # on attend un peu plus au début (le temps que le groupe reçoive ses partitions)
                if received and empty_polls * 0.5 >= idle_seconds:
                    break
                continue
            empty_polls = 0
            received += 1
            try:
                alert = json.loads(msg.value())
            except (TypeError, ValueError):
                continue
            alerts[alert.get("alert_id", str(received))] = alert
    except KafkaException:
        return pd.DataFrame()
    finally:
        consumer.close()

    if not alerts:
        return pd.DataFrame()
    df = pd.DataFrame(alerts.values())
    for col in ["window_start", "window_end", "detected_at"]:
        if col in df:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df.sort_values(["window_start", "crime_count"], ascending=[False, False])

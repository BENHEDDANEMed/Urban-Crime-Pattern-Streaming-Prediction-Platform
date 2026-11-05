"""
Job Spark Structured Streaming (Semaine 2).

Ce job lance 3 "queries" qui tournent en même temps :

  1) crimes.raw   -> Bronze Parquet   data/lake/bronze/crimes   (copie brute)
  2) weather.raw  -> Bronze Parquet   data/lake/bronze/weather  (copie brute)
  3) crimes.raw   -> fenêtres de 30 min par ville -> DBSCAN -> topic crimes.alerts

Schéma :
    Kafka (crimes.raw) ──► parsing JSON ──► fenêtre 30 min ──► DBSCAN ──► Kafka (crimes.alerts)
            │
            └──────────► Bronze Parquet (pour la phase 2 : Silver / Gold avec Airflow)

Lancement : automatique avec docker compose (service "spark-streaming").
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

from streaming.clustering import build_alerts

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("crime_stream_job")

# ---------------------------------------------------------------------------
# Paramètres (modifiables avec des variables d'environnement)
# ---------------------------------------------------------------------------
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_CRIMES = os.getenv("TOPIC_CRIMES_RAW", "crimes.raw")
TOPIC_WEATHER = os.getenv("TOPIC_WEATHER_RAW", "weather.raw")
TOPIC_ALERTS = os.getenv("TOPIC_CRIMES_ALERTS", "crimes.alerts")
LAKE = os.getenv("LAKE_PATH", "data/lake")
CHECKPOINTS = os.getenv("SPARK_CHECKPOINT_PATH", "data/checkpoints/spark")

WINDOW_DURATION = os.getenv("ALERT_WINDOW", "30 minutes")  # taille de la fenêtre
WATERMARK = os.getenv("WATERMARK", "10 minutes")           # retard toléré
EPS_KM = float(os.getenv("DBSCAN_EPS_KM", "0.5"))          # rayon DBSCAN
MIN_SAMPLES = int(os.getenv("DBSCAN_MIN_SAMPLES", "5"))    # crimes min. pour un hotspot
MAX_RADIUS_KM = float(os.getenv("HOTSPOT_MAX_RADIUS_KM", "1.0"))  # au-delà : pas un hotspot
ALERT_TRIGGER = os.getenv("ALERT_TRIGGER", "30 seconds")   # fréquence de calcul des alertes
BRONZE_TRIGGER = os.getenv("BRONZE_TRIGGER", "1 minute")   # fréquence d'écriture Bronze

# Forme d'un message "crime" (voir common/schemas.py et docs/CONTRAT_DONNEES.md).
# On ne lit pas le champ "raw" ici : il est gardé tel quel dans Bronze.
CRIME_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_id", StringType()),
    StructField("source_id", StringType()),
    StructField("city", StringType()),
    StructField("source", StringType()),
    StructField("crime_type", StringType()),
    StructField("crime_category", StringType()),
    StructField("description", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("occurred_at", StringType()),
    StructField("ingested_at", StringType()),
    StructField("ingest_mode", StringType()),
])
ISO_UTC = "yyyy-MM-dd'T'HH:mm:ssXXX"  # ex: 2026-11-05T18:00:00Z


def create_spark() -> SparkSession:
    spark = (SparkSession.builder
             .appName("CrimeWatch-Streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")  # moins de messages inutiles dans les logs
    return spark


def read_topic(spark: SparkSession, topic: str) -> DataFrame:
    """Lit un topic Kafka en continu. Chaque ligne = un message (key, value, offset...)."""
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")  # 1er lancement : on lit tout depuis le début
            .option("failOnDataLoss", "false")
            .load())


# ---------------------------------------------------------------------------
# 1 + 2) Bronze : on garde le message JSON COMPLET, sans rien modifier
# ---------------------------------------------------------------------------
def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """
    Bronze = données brutes + quelques colonnes techniques.
    Si un jour on se trompe dans le nettoyage (Silver), on peut tout refaire
    à partir de Bronze. C'est le principe de l'architecture "Médaillon".
    """
    raw_json = F.col("value").cast("string")
    return (kafka_df.select(
                raw_json.alias("raw_json"),
                F.col("key").cast("string").alias("kafka_key"),
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"),
            )
            .withColumn("city", F.get_json_object("raw_json", "$.city"))
            .withColumn("ingested_at", F.to_timestamp(F.get_json_object("raw_json", "$.ingested_at"), ISO_UTC))
            .withColumn("ingest_date", F.to_date("kafka_timestamp")))


def start_bronze_query(kafka_df: DataFrame, name: str):
    path = f"{LAKE}/bronze/{name}"
    log.info("Bronze %s -> %s", name, path)
    return (to_bronze(kafka_df).writeStream
            .queryName(f"bronze_{name}")
            .format("parquet")
            .option("path", path)
            .option("checkpointLocation", f"{CHECKPOINTS}/bronze_{name}")
            .partitionBy("city", "ingest_date")  # un dossier par ville et par jour
            .outputMode("append")
            .trigger(processingTime=BRONZE_TRIGGER)
            .start())


# ---------------------------------------------------------------------------
# 3) Alertes : parsing -> fenêtres de 30 min -> DBSCAN -> crimes.alerts
# ---------------------------------------------------------------------------
def parse_crimes(kafka_df: DataFrame) -> DataFrame:
    """Transforme le texte JSON en vraies colonnes, et enlève les lignes inutilisables."""
    return (kafka_df
            .select(F.from_json(F.col("value").cast("string"), CRIME_SCHEMA).alias("e"))
            .select("e.*")
            .withColumn("ingested_ts", F.to_timestamp("ingested_at", ISO_UTC))
            .filter(F.col("latitude").isNotNull()
                    & F.col("longitude").isNotNull()
                    & F.col("ingested_ts").isNotNull()))


def windowed_crimes(crimes: DataFrame) -> DataFrame:
    """
    Regroupe les crimes par ville et par fenêtre de 30 minutes.
    On garde la liste des points (lat, lon, catégorie) pour lancer DBSCAN ensuite.

    Pourquoi "ingested_ts" (heure d'arrivée) et pas "occurred_at" (heure du crime) ?
    Parce que les APIs publiques ont des jours de retard : si on utilisait
    l'heure du crime, toutes les données seraient "trop vieilles" pour le temps réel.

    Watermark = "j'accepte les messages en retard de 10 min max, après je ferme
    la fenêtre et j'oublie". Sans watermark, Spark garderait tout en mémoire.
    """
    return (crimes
            .withWatermark("ingested_ts", WATERMARK)
            .groupBy(F.window("ingested_ts", WINDOW_DURATION), F.col("city"))
            .agg(F.count("*").alias("crime_count"),
                 F.collect_list(F.struct("event_id", "latitude", "longitude",
                                         "crime_category")).alias("points")))


class AlertProcessor:
    """
    Appelé par Spark à chaque micro-batch (toutes les 30 s) avec les fenêtres
    qui ont changé. Pour chaque fenêtre : DBSCAN -> alertes -> Kafka.

    En mode "update", Spark renvoie une fenêtre à chaque fois qu'elle reçoit de
    nouveaux crimes. Pour ne pas envoyer 10 fois la même alerte, on se souvient
    des alertes déjà envoyées et on renvoie seulement si le hotspot a GRANDI.
    """

    def __init__(self):
        self.sent: dict[str, int] = {}  # alert_id -> crime_count déjà envoyé

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.collect()  # peu de lignes : (nb villes) x (nb fenêtres actives)
        if not rows:
            return

        alerts = []
        for row in rows:
            log.info("Batch %d | %-12s | fenêtre %s -> %s | %d crimes",
                     batch_id, row["city"], row["window"]["start"], row["window"]["end"],
                     row["crime_count"])
            points = [p.asDict() for p in row["points"]]
            for alert in build_alerts(row["city"], row["window"]["start"], row["window"]["end"],
                                      points, EPS_KM, MIN_SAMPLES, MAX_RADIUS_KM):
                if alert["crime_count"] > self.sent.get(alert["alert_id"], 0):
                    self.sent[alert["alert_id"]] = alert["crime_count"]
                    alerts.append(alert)

        self._forget_old_alerts()
        if not alerts:
            return

        for a in alerts:
            log.warning("ALERTE %s | %s | %d crimes dans %.2f km autour de (%.4f, %.4f) | surtout: %s",
                        a["severity"].upper(), a["city"], a["crime_count"], a["radius_km"],
                        a["center_latitude"], a["center_longitude"], a["top_category"])

        spark = batch_df.sparkSession
        alerts_df = spark.createDataFrame(
            [(a["city"], json.dumps(a)) for a in alerts], ["key", "value"])
        (alerts_df.write
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA)
         .option("topic", TOPIC_ALERTS)
         .save())

    def _forget_old_alerts(self) -> None:
        """On oublie les alertes des fenêtres de plus de 2 heures (sinon la mémoire grossit)."""
        limit = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.sent = {k: v for k, v in self.sent.items() if k.split("|")[1] >= limit}


def start_alert_query(kafka_df: DataFrame):
    windows = windowed_crimes(parse_crimes(kafka_df))
    return (windows.writeStream
            .queryName("hotspot_alerts")
            .outputMode("update")
            .foreachBatch(AlertProcessor())
            .option("checkpointLocation", f"{CHECKPOINTS}/alerts")
            .trigger(processingTime=ALERT_TRIGGER)
            .start())


def main() -> None:
    spark = create_spark()
    log.info("Kafka=%s | fenêtre=%s | DBSCAN eps=%.2f km, min_samples=%d, rayon max=%.1f km",
             KAFKA, WINDOW_DURATION, EPS_KM, MIN_SAMPLES, MAX_RADIUS_KM)

    crimes_kafka = read_topic(spark, TOPIC_CRIMES)
    weather_kafka = read_topic(spark, TOPIC_WEATHER)

    start_bronze_query(crimes_kafka, "crimes")
    start_bronze_query(weather_kafka, "weather")
    start_alert_query(crimes_kafka)
    
    
    

    log.info("3 queries démarrées. Spark UI : http://localhost:4040")
    spark.streams.awaitAnyTermination()  # le job tourne jusqu'à ce qu'on l'arrête


if __name__ == "__main__":
    main()    
"""
Phase 2 (S3) : Bronze -> Silver  (Spark batch)

Bronze = messages Kafka bruts (colonne raw_json), avec doublons et données de démo.
Silver = une ligne propre par crime / par relevé météo, avec les bons types.

Ce que fait ce job :
  Crimes
    1. parse le JSON avec un schéma explicite (contrat : docs/CONTRAT_DONNEES.md)
    2. garde seulement les vraies données (ingest_mode = 'api')
    3. déduplique sur event_id (on garde la version arrivée en dernier dans Kafka)
    4. ajoute les colonnes de temps (date, heure, jour de la semaine, mois)
    5. ajoute hour_known : 0 si l'heure du crime n'est pas fiable
       - Londres : l'API ne donne que le mois -> heure toujours inconnue
       - toutes villes : 00:00 pile = souvent "heure inconnue"
         (mesuré : 2,7x plus que prévu à Chicago, 1,6x à LA, normal à New York)
  Météo
    1. parse le JSON, 2. garde les vraies données, 3. une ligne par (ville, heure observée)

Lancement (Spark tourne déjà dans le conteneur spark-streaming) :
  docker exec spark-streaming /opt/spark/bin/spark-submit --master local[*] batch/bronze_to_silver.py
"""
import logging
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("bronze_to_silver")

LAKE = os.getenv("LAKE_PATH", "data/lake")
ISO_UTC = "yyyy-MM-dd'T'HH:mm:ssXXX"   # ingested_at : 2026-11-05T18:00:00Z

# On ne lit pas le champ "raw" (ligne brute de l'API) : il reste disponible dans Bronze.
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

WEATHER_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("city", StringType()),
    StructField("source", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("observed_at", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("precipitation_mm", DoubleType()),
    StructField("rain_mm", DoubleType()),
    StructField("wind_speed_kmh", DoubleType()),
    StructField("is_day", IntegerType()),
    StructField("ingested_at", StringType()),
])


def create_spark() -> SparkSession:
    spark = (SparkSession.builder
             .appName("CrimeWatch-BronzeToSilver")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.port", "4041")   # 4040 est déjà pris par le job de streaming
             .config("spark.sql.shuffle.partitions", "4")   # petites données : 4 suffisent (200 par défaut)
             .config("spark.ui.showConsoleProgress", "false")  # pas de barres de progression dans les logs
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_bronze(bronze: DataFrame, schema: StructType) -> DataFrame:
    """raw_json (texte) -> vraies colonnes. On garde kafka_timestamp pour la déduplication."""
    return (bronze
            .select(F.from_json("raw_json", schema).alias("e"), "kafka_timestamp")
            .select("e.*", "kafka_timestamp"))


def keep_latest(df: DataFrame, keys: list) -> DataFrame:
    """Une seule ligne par clé : celle arrivée en dernier dans Kafka."""
    w = Window.partitionBy(*keys).orderBy(F.col("kafka_timestamp").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


# ---------------------------------------------------------------------------
# Crimes
# ---------------------------------------------------------------------------
def crimes_to_silver(bronze: DataFrame) -> DataFrame:
    crimes = parse_bronze(bronze, CRIME_SCHEMA)

    crimes = crimes.filter(
        (F.col("ingest_mode") == "api")              # pas de données de démo (replay)
        & F.col("event_id").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull())

    crimes = keep_latest(crimes, ["event_id"])

    occurred = F.to_timestamp("occurred_at")         # heure LOCALE de la ville
    crimes = (crimes
              .withColumn("occurred_ts", occurred)
              .filter(F.col("occurred_ts").isNotNull())
              .withColumn("ingested_ts", F.to_timestamp("ingested_at", ISO_UTC))
              .withColumn("occurred_date", F.to_date("occurred_ts"))
              .withColumn("occurred_hour", F.hour("occurred_ts"))
              .withColumn("day_of_week", F.dayofweek("occurred_ts"))   # 1 = dimanche ... 7 = samedi
              .withColumn("month", F.month("occurred_ts"))
              .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int")))

    exactly_midnight = (F.col("occurred_hour") == 0) & (F.minute("occurred_ts") == 0)
    crimes = crimes.withColumn(
        "hour_known",
        F.when(F.col("city") == "london", 0)
         .when(exactly_midnight, 0)
         .otherwise(1))

    return crimes.select(
        "event_id", "city", "source", "crime_type", "crime_category", "description",
        "latitude", "longitude",
        "occurred_ts", "occurred_date", "occurred_hour", "day_of_week", "month", "is_weekend",
        "hour_known", "ingested_ts", "schema_version")


def log_hour_quality(silver: DataFrame) -> None:
    """Contrôle qualité : part des crimes à 00:00 pile, par ville.
    À comparer aux autres heures pile de la nuit (23:00, 01:00) : un 00:00 nettement
    plus haut révèle une valeur par défaut "heure inconnue"."""
    stats = (silver.groupBy("city")
             .agg(F.count("*").alias("crimes"),
                  F.round(100 * F.avg(1 - F.col("hour_known")), 1).alias("pct_hour_unknown"))
             .orderBy("city")
             .collect())
    for row in stats:
        log.info("Qualité | %-12s | %6d crimes | heure inconnue : %5.1f %%",
                 row["city"], row["crimes"], row["pct_hour_unknown"])


# ---------------------------------------------------------------------------
# Météo
# ---------------------------------------------------------------------------
def weather_to_silver(bronze: DataFrame) -> DataFrame:
    weather = parse_bronze(bronze, WEATHER_SCHEMA)
    weather = (weather
               .withColumn("observed_ts", F.to_timestamp("observed_at"))   # heure locale
               .filter(F.col("city").isNotNull() & F.col("observed_ts").isNotNull())
               .filter(F.col("source") != "synthetic"))
    weather = keep_latest(weather, ["city", "observed_ts"])
    return weather.select(
        "city", "observed_ts", "temperature_c", "precipitation_mm", "rain_mm",
        "wind_speed_kmh", "is_day", "latitude", "longitude",
        F.to_timestamp("ingested_at", ISO_UTC).alias("ingested_ts"))


def run(spark: SparkSession, lake: str = LAKE) -> None:
    bronze_crimes = spark.read.parquet(f"{lake}/bronze/crimes")
    log.info("Bronze crimes : %d lignes", bronze_crimes.count())
    silver_crimes = crimes_to_silver(bronze_crimes).cache()
    (silver_crimes.write.mode("overwrite")
        .partitionBy("city")
        .parquet(f"{lake}/silver/crimes"))
    log.info("Silver crimes : %d lignes (doublons et démo enlevés)", silver_crimes.count())
    log_hour_quality(silver_crimes)

    weather_path = f"{lake}/bronze/weather"
    try:
        bronze_weather = spark.read.parquet(weather_path)
    except Exception:  # noqa: BLE001  (pas encore de météo dans Bronze)
        log.warning("Pas de météo dans %s : étape ignorée", weather_path)
        return
    silver_weather = weather_to_silver(bronze_weather)
    (silver_weather.write.mode("overwrite")
        .partitionBy("city")
        .parquet(f"{lake}/silver/weather"))
    log.info("Silver météo : %d relevés", silver_weather.count())


if __name__ == "__main__":
    spark = create_spark()
    run(spark)
    log.info("Silver OK")
    spark.stop()
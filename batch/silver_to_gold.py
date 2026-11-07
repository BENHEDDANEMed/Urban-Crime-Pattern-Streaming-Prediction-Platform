"""
Phase 2 (S3) : Silver -> Gold  (Spark batch)

Gold = des tables prêtes pour le Machine Learning et pour le dashboard.

  1. gold/crime_features   une ligne par crime, avec :
       - le temps    : heure, jour de la semaine, mois, week-end
       - le lieu     : zone de ~500 m (grille), latitude, longitude
       - la météo    : au moment du crime (heure exacte si hour_known = 1,
                       sinon moyenne de la journée), depuis silver/weather_hourly
     -> sert à l'étape ML (prédire la catégorie et le créneau horaire).

  2. gold/zone_hourly      nombre de crimes par (ville, zone, date, heure)
     -> la "carte de chaleur" heure par heure (seulement les crimes avec hour_known = 1).

  3. gold/zone_stats       une ligne par (ville, zone) : total, catégorie principale,
                           part de la ville, rang -> les zones les plus touchées.

La zone : on découpe chaque ville en cases de 0,005° (~550 m nord-sud, ~420 m est-ouest).
C'est simple, stable dans le temps et facile à afficher sur une carte.

Lancement :
  docker exec spark-streaming /opt/spark/bin/spark-submit --master local[*] batch/silver_to_gold.py
"""
import logging
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("silver_to_gold")

LAKE = os.getenv("LAKE_PATH", "data/lake")
GRID_DEG = float(os.getenv("ZONE_GRID_DEG", "0.005"))   # taille d'une case de la grille


def create_spark() -> SparkSession:
    spark = (SparkSession.builder
             .appName("CrimeWatch-SilverToGold")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.port", "4041")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Zones (grille)
# ---------------------------------------------------------------------------
def add_zone(df: DataFrame, grid: float = GRID_DEG) -> DataFrame:
    """Ajoute la case de la grille : zone_id + le centre de la case (pour la carte)."""
    row = F.floor(F.col("latitude") / grid).cast("long")
    col = F.floor(F.col("longitude") / grid).cast("long")
    return (df
            .withColumn("zone_id", F.concat_ws("_", F.col("city"), row, col))
            .withColumn("zone_lat", F.round((row + 0.5) * grid, 6))
            .withColumn("zone_lon", F.round((col + 0.5) * grid, 6)))


# ---------------------------------------------------------------------------
# Météo
# ---------------------------------------------------------------------------
def hourly_weather(weather: DataFrame) -> DataFrame:
    """Une ligne par (ville, heure). La clé est un texte 'yyyy-MM-dd HH' :
    pas de problème de fuseau horaire, les deux côtés sont en heure locale."""
    return weather.select(
        "city",
        F.date_format("weather_ts", "yyyy-MM-dd HH").alias("hour_key"),
        F.col("temperature_c").alias("h_temperature_c"),
        F.col("precipitation_mm").alias("h_precipitation_mm"),
        F.col("wind_speed_kmh").alias("h_wind_speed_kmh"),
        F.col("is_day").alias("h_is_day"))


def daily_weather(weather: DataFrame) -> DataFrame:
    """Une ligne par (ville, jour) : utilisée quand l'heure du crime est inconnue."""
    return (weather
            .groupBy("city", F.date_format("weather_ts", "yyyy-MM-dd").alias("day_key"))
            .agg(F.round(F.avg("temperature_c"), 2).alias("d_temperature_c"),
                 F.round(F.sum("precipitation_mm"), 2).alias("d_precipitation_mm"),
                 F.round(F.avg("wind_speed_kmh"), 2).alias("d_wind_speed_kmh")))


def add_weather(crimes: DataFrame, weather: DataFrame) -> DataFrame:
    crimes = (crimes
              .withColumn("hour_key", F.date_format("occurred_ts", "yyyy-MM-dd HH"))
              .withColumn("day_key", F.date_format("occurred_ts", "yyyy-MM-dd")))
    joined = (crimes
              .join(hourly_weather(weather), ["city", "hour_key"], "left")
              .join(daily_weather(weather), ["city", "day_key"], "left"))

    use_hourly = (F.col("hour_known") == 1) & F.col("h_temperature_c").isNotNull()
    return (joined
            .withColumn("temperature_c", F.when(use_hourly, F.col("h_temperature_c")).otherwise(F.col("d_temperature_c")))
            .withColumn("precipitation_mm", F.when(use_hourly, F.col("h_precipitation_mm")).otherwise(F.col("d_precipitation_mm")))
            .withColumn("wind_speed_kmh", F.when(use_hourly, F.col("h_wind_speed_kmh")).otherwise(F.col("d_wind_speed_kmh")))
            .withColumn("is_raining", (F.col("precipitation_mm") > 0).cast("int"))
            .withColumn("is_night", F.when(use_hourly, 1 - F.col("h_is_day")))   # inconnu si l'heure est inconnue
            .withColumn("weather_level",
                        F.when(use_hourly, "hourly")
                         .when(F.col("d_temperature_c").isNotNull(), "daily")
                         .otherwise("none"))
            .drop("hour_key", "day_key", "h_temperature_c", "h_precipitation_mm", "h_wind_speed_kmh",
                  "h_is_day", "d_temperature_c", "d_precipitation_mm", "d_wind_speed_kmh"))


# ---------------------------------------------------------------------------
# Tables Gold
# ---------------------------------------------------------------------------
def build_crime_features(crimes: DataFrame, weather: DataFrame) -> DataFrame:
    df = add_weather(add_zone(crimes), weather)
    return df.select(
        "event_id", "city", "crime_category", "crime_type",
        "occurred_ts", "occurred_date", "occurred_hour", "hour_known",
        "day_of_week", "month", "is_weekend",
        "latitude", "longitude", "zone_id", "zone_lat", "zone_lon",
        "temperature_c", "precipitation_mm", "wind_speed_kmh", "is_raining", "is_night",
        "weather_level")


def build_zone_hourly(features: DataFrame) -> DataFrame:
    return (features
            .filter(F.col("hour_known") == 1)
            .groupBy("city", "zone_id", "zone_lat", "zone_lon", "occurred_date", "occurred_hour")
            .agg(F.count("*").alias("crime_count")))


def build_zone_stats(features: DataFrame) -> DataFrame:
    per_category = (features.groupBy("city", "zone_id", "crime_category")
                    .agg(F.count("*").alias("n")))
    w_cat = Window.partitionBy("city", "zone_id").orderBy(F.col("n").desc(), "crime_category")
    top_category = (per_category
                    .withColumn("_rn", F.row_number().over(w_cat))
                    .filter("_rn = 1")
                    .select("city", "zone_id", F.col("crime_category").alias("top_category")))

    zones = (features.groupBy("city", "zone_id", "zone_lat", "zone_lon")
             .agg(F.count("*").alias("crime_count"),
                  F.countDistinct("occurred_date").alias("active_days"),
                  F.min("occurred_date").alias("first_date"),
                  F.max("occurred_date").alias("last_date")))
    w_city = Window.partitionBy("city")
    w_rank = Window.partitionBy("city").orderBy(F.col("crime_count").desc(), "zone_id")
    return (zones.join(top_category, ["city", "zone_id"])
            .withColumn("share_of_city_pct",
                        F.round(100 * F.col("crime_count") / F.sum("crime_count").over(w_city), 3))
            .withColumn("rank_in_city", F.row_number().over(w_rank)))


def run(spark: SparkSession, lake: str = LAKE) -> None:
    crimes = spark.read.parquet(f"{lake}/silver/crimes")
    weather = spark.read.parquet(f"{lake}/silver/weather_hourly")

    features = build_crime_features(crimes, weather).cache()
    features.write.mode("overwrite").partitionBy("city").parquet(f"{lake}/gold/crime_features")
    log.info("Gold crime_features : %d crimes", features.count())
    for row in (features.groupBy("city", "weather_level").count()
                .orderBy("city", "weather_level").collect()):
        log.info("Météo | %-12s | %-6s | %6d crimes", row["city"], row["weather_level"], row["count"])

    zone_hourly = build_zone_hourly(features)
    zone_hourly.write.mode("overwrite").partitionBy("city").parquet(f"{lake}/gold/zone_hourly")
    log.info("Gold zone_hourly : %d lignes", zone_hourly.count())

    zone_stats = build_zone_stats(features).cache()
    zone_stats.write.mode("overwrite").partitionBy("city").parquet(f"{lake}/gold/zone_stats")
    log.info("Gold zone_stats : %d zones", zone_stats.count())
    for row in zone_stats.filter("rank_in_city = 1").orderBy("city").collect():
        log.info("Zone n°1 | %-12s | %5d crimes (%.2f %% de la ville) | surtout : %s | (%.4f, %.4f)",
                 row["city"], row["crime_count"], row["share_of_city_pct"], row["top_category"],
                 row["zone_lat"], row["zone_lon"])


if __name__ == "__main__":
    spark = create_spark()
    run(spark)
    log.info("Gold OK")
    spark.stop()
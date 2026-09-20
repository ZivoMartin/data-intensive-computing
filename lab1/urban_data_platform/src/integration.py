"""
Task 5 — integration pipeline.

Builds gold/integrated_taxi_trips: every taxi trip enriched with pickup/
dropoff zone + borough, pickup-hour weather, and pickup-hour citywide
air-quality pollutant means.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from io_utils import write_delta


def build_integrated_taxi_trips(spark: SparkSession, silver_root: str, gold_root: str) -> DataFrame:
    taxi = spark.read.format("delta").load(f"{silver_root}/taxi_trips")
    weather = spark.read.format("delta").load(f"{silver_root}/weather")
    air_quality = spark.read.format("delta").load(f"{silver_root}/air_quality")
    zones = spark.read.format("delta").load(f"{silver_root}/taxi_zones")

    pickup_zone = (
        F.broadcast(zones)
        .select(
            F.col("location_id").alias("pu_zone_location_id"),
            F.col("zone").alias("pickup_zone"),
            F.col("borough").alias("pickup_borough"),
        )
    )
    dropoff_zone = (
        F.broadcast(zones)
        .select(
            F.col("location_id").alias("do_zone_location_id"),
            F.col("zone").alias("dropoff_zone"),
            F.col("borough").alias("dropoff_borough"),
        )
    )

    enriched = (
        taxi.join(pickup_zone, taxi["pulocation_id"] == pickup_zone["pu_zone_location_id"], "left")
        .join(dropoff_zone, taxi["dolocation_id"] == dropoff_zone["do_zone_location_id"], "left")
        .drop("pu_zone_location_id", "do_zone_location_id")
        .withColumn(
            "zone_available",
            F.col("pickup_zone").isNotNull() & F.col("dropoff_zone").isNotNull(),
        )
    )

    weather_hourly = weather.select(
        F.col("observation_hour_utc").alias("weather_hour_utc"),
        *[c for c in weather.columns if c not in ("observation_hour_utc", "observation_timestamp_utc", "observation_timestamp_local")],
    )
    enriched = enriched.join(
        weather_hourly, enriched["pickup_hour_utc"] == weather_hourly["weather_hour_utc"], "left"
    )
    enriched = enriched.withColumn("weather_available", F.col("weather_hour_utc").isNotNull())
    enriched = enriched.drop("weather_hour_utc")

    aq_hourly = (
        air_quality.groupBy("observation_hour_utc", "parameter_name")
        .agg(
            F.avg("sample_measurement").alias("mean_measurement"),
            F.countDistinct("site_num").alias("station_count"),
        )
    )
    aq_maps = aq_hourly.groupBy("observation_hour_utc").agg(
        F.map_from_entries(F.collect_list(F.struct("parameter_name", "mean_measurement"))).alias(
            "air_quality_measurements"
        ),
        F.map_from_entries(
            F.collect_list(F.struct("parameter_name", F.col("station_count").cast("double")))
        ).alias("air_quality_station_counts"),
    )

    enriched = enriched.join(
        aq_maps, enriched["pickup_hour_utc"] == aq_maps["observation_hour_utc"], "left"
    ).drop(aq_maps["observation_hour_utc"])
    enriched = enriched.withColumn(
        "air_quality_available", F.col("air_quality_measurements").isNotNull()
    )

    write_delta(enriched, f"{gold_root}/integrated_taxi_trips", partition_cols=["pickup_year", "pickup_month"])
    return enriched

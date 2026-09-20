"""
Week 2 — Task 4: reusable analytical data products.

Each data product is a pre-aggregated summary derived automatically from
``gold/integrated_taxi_trips`` and written as its own partitioned/ordered
Delta table under ``<output_root>/gold/products/<name>``. Analysts query the
product directly instead of re-running the heavy source aggregation.

Every product row carries provenance/metadata columns:

    _data_source        the source table it was derived from
    _created_at         first-build timestamp (UTC)
    _refreshed_at       timestamp of the run that produced the current rows
    _schema_version     product schema version string

A companion metadata log is appended to
``<output_root>/metadata/data_products`` on every build so the platform keeps
an auditable history of when each product was refreshed, how many rows it has,
and its on-disk size.

The five products
-----------------
    daily_mobility_summary     per-day citywide mobility KPIs
    taxi_zone_statistics       per-zone lifetime statistics
    weather_impact_summary     demand & trip metrics per weather condition
    air_quality_impact_summary demand per air-quality band
    borough_mobility_summary   per-borough monthly mobility KPIs

Rationale for materialization (report Task 4): these summaries collapse tens of
millions of trip rows into at most a few thousand rows, are read far more often
than the underlying data changes, and back dashboards/reports where interactive
latency matters. Recomputing them on demand would repeat the same full scan for
every viewer; materializing pays that cost once per refresh.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import queries

SOURCE_TABLE = "gold/integrated_taxi_trips"


# --------------------------------------------------------------------------
# Product definition
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class DataProductSpec:
    name: str
    title: str
    audience: str
    why_useful: str
    why_materialize: str
    schema_version: str
    partition_cols: List[str]
    builder: Callable[[SparkSession], DataFrame]


# --------------------------------------------------------------------------
# Builders — each returns a plain (un-decorated) aggregate DataFrame.
# They read the canonical `integrated_taxi_trips` view registered by
# queries.register_views(), so they compose with the rest of Week 2.
# --------------------------------------------------------------------------
def _build_daily_mobility_summary(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT
            pickup_date,
            pickup_year,
            pickup_month,
            COUNT(*)                                   AS trip_count,
            COUNT(DISTINCT pulocation_id)              AS active_pickup_zones,
            SUM(fare_amount)                           AS total_fare,
            AVG(fare_amount)                           AS avg_fare,
            AVG(trip_distance)                         AS avg_trip_distance,
            AVG(trip_duration_seconds) / 60.0          AS avg_trip_duration_minutes
        FROM integrated_taxi_trips
        WHERE pickup_date IS NOT NULL
        GROUP BY pickup_date, pickup_year, pickup_month
        """
    )


def _build_taxi_zone_statistics(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT
            pulocation_id,
            pickup_zone,
            pickup_borough,
            COUNT(*)                          AS total_trips,
            AVG(trip_distance)                AS avg_trip_distance,
            AVG(fare_amount)                  AS avg_fare,
            AVG(trip_duration_seconds) / 60.0 AS avg_trip_duration_minutes,
            MIN(pickup_date)                  AS first_seen_date,
            MAX(pickup_date)                  AS last_seen_date
        FROM integrated_taxi_trips
        WHERE pickup_zone IS NOT NULL
        GROUP BY pulocation_id, pickup_zone, pickup_borough
        """
    )


def _build_weather_impact_summary(spark: SparkSession) -> DataFrame:
    return spark.sql(
        f"""
        SELECT
            {queries.WEATHER_CONDITION_EXPR}  AS weather_condition,
            COUNT(*)                          AS trip_count,
            AVG(trip_distance)                AS avg_trip_distance,
            AVG(trip_duration_seconds) / 60.0 AS avg_trip_duration_minutes,
            AVG(fare_amount)                  AS avg_fare,
            AVG(temp)                         AS avg_temp,
            AVG(prcp)                         AS avg_precipitation
        FROM integrated_taxi_trips
        WHERE weather_available = true
        GROUP BY {queries.WEATHER_CONDITION_EXPR}
        """
    )


def _build_air_quality_impact_summary(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        WITH hourly AS (
            SELECT
                pickup_hour_utc,
                COUNT(*) AS trip_count,
                AVG(
                    COALESCE(
                        air_quality_measurements['PM2.5 - Local Conditions'],
                        air_quality_measurements['PM2.5']
                    )
                ) AS pm25
            FROM integrated_taxi_trips
            WHERE air_quality_available = true
            GROUP BY pickup_hour_utc
        )
        SELECT
            CASE
                WHEN pm25 IS NULL THEN 'unknown'
                WHEN pm25 <= 12.0 THEN 'good'
                WHEN pm25 <= 35.4 THEN 'moderate'
                WHEN pm25 <= 55.4 THEN 'unhealthy_sensitive'
                ELSE 'unhealthy'
            END                        AS air_quality_band,
            COUNT(*)                   AS observed_hours,
            AVG(trip_count)            AS avg_trips_per_hour,
            SUM(trip_count)            AS total_trips,
            AVG(pm25)                  AS avg_pm25
        FROM hourly
        GROUP BY 1
        """
    )


def _build_borough_mobility_summary(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT
            pickup_borough,
            pickup_year,
            pickup_month,
            COUNT(*)                          AS trip_count,
            SUM(fare_amount)                  AS total_fare,
            AVG(fare_amount)                  AS avg_fare,
            AVG(trip_distance)                AS avg_trip_distance,
            COUNT(DISTINCT pulocation_id)     AS active_zones
        FROM integrated_taxi_trips
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough, pickup_year, pickup_month
        """
    )


# --------------------------------------------------------------------------
# Registry
# --------------------------------------------------------------------------
PRODUCTS: Dict[str, DataProductSpec] = {
    "daily_mobility_summary": DataProductSpec(
        name="daily_mobility_summary",
        title="Daily Mobility Summary",
        audience="City mobility analysts and operations dashboards.",
        why_useful="One row per day of citywide demand, revenue and trip-quality KPIs; backs time-series dashboards and daily reporting.",
        why_materialize="Read every day by many viewers but only refreshed once per ingestion cycle; recomputing a full-table daily rollup per view is wasteful.",
        schema_version="1.0",
        partition_cols=["pickup_year"],
        builder=_build_daily_mobility_summary,
    ),
    "taxi_zone_statistics": DataProductSpec(
        name="taxi_zone_statistics",
        title="Taxi Zone Statistics",
        audience="Urban planners and zone-level demand analysts.",
        why_useful="Lifetime per-zone demand and trip metrics for ranking, mapping and capacity planning.",
        why_materialize="Small (≈one row per zone) but derived from a full scan; materializing gives instant lookups for maps/joins.",
        schema_version="1.0",
        partition_cols=[],
        builder=_build_taxi_zone_statistics,
    ),
    "weather_impact_summary": DataProductSpec(
        name="weather_impact_summary",
        title="Weather Impact Summary",
        audience="Demand forecasting and policy analysts.",
        why_useful="Quantifies how demand and trip characteristics shift across weather conditions.",
        why_materialize="Tiny output but expensive to compute (full scan + bucketing); ideal materialization candidate.",
        schema_version="1.0",
        partition_cols=[],
        builder=_build_weather_impact_summary,
    ),
    "air_quality_impact_summary": DataProductSpec(
        name="air_quality_impact_summary",
        title="Air Quality Impact Summary",
        audience="Environmental and public-health analysts.",
        why_useful="Relates citywide air-quality bands to hourly taxi demand for environmental studies.",
        why_materialize="Requires an hourly pre-aggregation over the full table before bucketing; cheap to store, costly to recompute.",
        schema_version="1.0",
        partition_cols=[],
        builder=_build_air_quality_impact_summary,
    ),
    "borough_mobility_summary": DataProductSpec(
        name="borough_mobility_summary",
        title="Borough Mobility Summary",
        audience="Borough-level government stakeholders and executive dashboards.",
        why_useful="Monthly mobility and revenue KPIs aggregated to the five boroughs.",
        why_materialize="Powers recurring monthly executive reports read far more often than the data changes.",
        schema_version="1.0",
        partition_cols=["pickup_year"],
        builder=_build_borough_mobility_summary,
    ),
}


# --------------------------------------------------------------------------
# Build / refresh
# --------------------------------------------------------------------------
def _decorate_with_metadata(df: DataFrame, spec: DataProductSpec, created_at: datetime,
                            refreshed_at: datetime) -> DataFrame:
    return (
        df.withColumn("_data_source", F.lit(SOURCE_TABLE))
        .withColumn("_created_at", F.lit(created_at))
        .withColumn("_refreshed_at", F.lit(refreshed_at))
        .withColumn("_schema_version", F.lit(spec.schema_version))
    )


def _product_path(gold_root: str, name: str) -> str:
    return f"{gold_root}/products/{name}"


def _existing_created_at(spark: SparkSession, path: str) -> Optional[datetime]:
    """Preserve the original _created_at across refreshes if the table exists."""
    try:
        row = spark.read.format("delta").load(path).select("_created_at").limit(1).collect()
        return row[0]["_created_at"] if row else None
    except Exception:
        return None


def _dir_size_bytes(spark: SparkSession, path: str) -> int:
    hconf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    total = 0
    try:
        it = fs.listFiles(p, True)
        while it.hasNext():
            total += it.next().getLen()
    except Exception:
        return -1
    return total


def build_product(spark: SparkSession, spec: DataProductSpec, gold_root: str,
                  metadata_root: str) -> dict:
    """Build/refresh a single data product and append a metadata-log record."""
    path = _product_path(gold_root, spec.name)
    refreshed_at = datetime.now(timezone.utc)
    created_at = _existing_created_at(spark, path) or refreshed_at

    df = _decorate_with_metadata(spec.builder(spark), spec, created_at, refreshed_at)

    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if spec.partition_cols:
        writer = writer.partitionBy(*spec.partition_cols)
    writer.save(path)

    row_count = spark.read.format("delta").load(path).count()
    size_bytes = _dir_size_bytes(spark, path)

    record = {
        "product_name": spec.name,
        "product_title": spec.title,
        "data_source": SOURCE_TABLE,
        "schema_version": spec.schema_version,
        "created_at": created_at,
        "refreshed_at": refreshed_at,
        "row_count": row_count,
        "storage_bytes": size_bytes,
        "path": path,
    }
    _append_metadata(spark, metadata_root, record)
    return record


def _append_metadata(spark: SparkSession, metadata_root: str, record: dict) -> None:
    row_df = spark.createDataFrame([record])
    (
        row_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(f"{metadata_root}/data_products")
    )


def build_all_products(spark: SparkSession, gold_root: str, metadata_root: str) -> List[dict]:
    """Build/refresh every registered data product."""
    records = []
    for spec in PRODUCTS.values():
        print(f"[product] building {spec.name}")
        records.append(build_product(spark, spec, gold_root, metadata_root))
    return records

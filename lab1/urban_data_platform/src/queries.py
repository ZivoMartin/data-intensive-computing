"""
Week 2 — Task 1 & 2: reusable analytical query library.

Every analysis required by the assignment is expressed here as a pure Spark
SQL string plus a thin Python wrapper that returns a DataFrame. Keeping the
queries as named SQL text (rather than DataFrame chains) means they can be:

  * executed directly against the Week 1 Delta tables,
  * fed verbatim to ``EXPLAIN FORMATTED`` for the optimization experiments,
  * materialized into the Week 2 data products, and
  * pasted into the report unchanged.

Design boundary
---------------
This module knows *nothing* about file paths. The caller is responsible for
registering the underlying Delta tables as temp views with the canonical
names below (see :func:`register_views`). That keeps the query text portable
across the integrated gold table and the raw silver tables, and lets the
optimization module swap a cached / re-partitioned view under the same name
without touching a single query.

Canonical view names
---------------------
    integrated_taxi_trips   gold/integrated_taxi_trips  (enriched trips)
    silver_taxi_trips       silver/taxi_trips
    silver_weather          silver/weather
    silver_air_quality      silver/air_quality
    silver_taxi_zones       silver/taxi_zones

The six analyses (assignment Task 1)
------------------------------------
    Q1  monthly_demand_by_zone          Monthly taxi demand for each taxi zone.
    Q2  avg_distance_by_weather         Average trip distance under different weather conditions.
    Q3  air_quality_vs_demand           Relationship between air quality and taxi demand.
    Q4  zone_demand_weather_variation   Zones with the largest demand variation across weather.
    Q5  peak_hours_by_weekday           Peak travel hours for each day of the week.
    Q6  monthly_demand_trend            Monthly trends in taxi demand.
"""
from dataclasses import dataclass
from typing import Callable, Dict, List

from pyspark.sql import DataFrame, SparkSession


# --------------------------------------------------------------------------
# View registration
# --------------------------------------------------------------------------
CANONICAL_VIEWS = {
    "integrated_taxi_trips": "gold/integrated_taxi_trips",
    "silver_taxi_trips": "silver/taxi_trips",
    "silver_weather": "silver/weather",
    "silver_air_quality": "silver/air_quality",
    "silver_taxi_zones": "silver/taxi_zones",
}


def register_views(spark: SparkSession, silver_root: str, gold_root: str) -> None:
    """Register the Week 1 Delta tables under their canonical view names.

    Idempotent: uses createOrReplaceTempView so the optimization module can
    later re-register a cached/re-partitioned DataFrame under the same name.
    """
    spark.read.format("delta").load(f"{gold_root}/integrated_taxi_trips").createOrReplaceTempView(
        "integrated_taxi_trips"
    )
    spark.read.format("delta").load(f"{silver_root}/taxi_trips").createOrReplaceTempView(
        "silver_taxi_trips"
    )
    spark.read.format("delta").load(f"{silver_root}/weather").createOrReplaceTempView(
        "silver_weather"
    )
    spark.read.format("delta").load(f"{silver_root}/air_quality").createOrReplaceTempView(
        "silver_air_quality"
    )
    spark.read.format("delta").load(f"{silver_root}/taxi_zones").createOrReplaceTempView(
        "silver_taxi_zones"
    )


# --------------------------------------------------------------------------
# Weather-condition bucketing
# --------------------------------------------------------------------------
# The raw weather feed exposes a numeric `coco` (Meteostat weather-condition
# code) plus continuous measurements. For "different weather conditions" we
# derive a small, stable set of human-readable categories from precipitation,
# snow depth and temperature. Defined once as a SQL expression so Q2 and Q4
# bucket identically.
WEATHER_CONDITION_EXPR = """
    CASE
        WHEN prcp IS NULL AND snwd IS NULL AND temp IS NULL THEN 'unknown'
        WHEN snwd > 0                    THEN 'snow'
        WHEN prcp >= 7.6                 THEN 'heavy_rain'
        WHEN prcp > 0                    THEN 'light_rain'
        WHEN temp >= 30                  THEN 'hot_dry'
        WHEN temp <= 0                   THEN 'freezing_dry'
        ELSE 'clear'
    END
"""


# --------------------------------------------------------------------------
# The six analytical queries
# --------------------------------------------------------------------------
# Q1 — Monthly taxi demand for each taxi zone.
Q1_MONTHLY_DEMAND_BY_ZONE = """
    SELECT
        pickup_year,
        pickup_month,
        pulocation_id,
        pickup_zone,
        pickup_borough,
        COUNT(*)                       AS trip_count,
        SUM(fare_amount)               AS total_fare,
        AVG(trip_distance)             AS avg_trip_distance
    FROM integrated_taxi_trips
    WHERE pickup_zone IS NOT NULL
    GROUP BY pickup_year, pickup_month, pulocation_id, pickup_zone, pickup_borough
    ORDER BY pickup_year, pickup_month, trip_count DESC
"""

# Q2 — Average trip distance under different weather conditions.
Q2_AVG_DISTANCE_BY_WEATHER = f"""
    SELECT
        {WEATHER_CONDITION_EXPR}       AS weather_condition,
        COUNT(*)                       AS trip_count,
        AVG(trip_distance)             AS avg_trip_distance,
        AVG(trip_duration_seconds)     AS avg_trip_duration_seconds,
        AVG(fare_amount)               AS avg_fare_amount
    FROM integrated_taxi_trips
    WHERE weather_available = true
    GROUP BY {WEATHER_CONDITION_EXPR}
    ORDER BY trip_count DESC
"""

# Q3 — Relationship between air quality and taxi demand.
# Air-quality context is a citywide hourly mean per pollutant, stored as a map
# in the integrated table (Week 1, report 5.4). We probe PM2.5, bucket it into
# EPA-style bands, and correlate with hourly trip demand.
Q3_AIR_QUALITY_VS_DEMAND = """
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
            WHEN pm25 IS NULL      THEN 'unknown'
            WHEN pm25 <= 12.0      THEN 'good'
            WHEN pm25 <= 35.4      THEN 'moderate'
            WHEN pm25 <= 55.4      THEN 'unhealthy_sensitive'
            ELSE 'unhealthy'
        END                                AS air_quality_band,
        COUNT(*)                           AS observed_hours,
        AVG(trip_count)                    AS avg_trips_per_hour,
        MIN(trip_count)                    AS min_trips_per_hour,
        MAX(trip_count)                    AS max_trips_per_hour,
        AVG(pm25)                          AS avg_pm25
    FROM hourly
    GROUP BY 1
    ORDER BY avg_pm25
"""

# Q4 — Taxi zones with the largest variation in demand under different weather.
# For each zone, compute trips-per-day under each weather condition, then the
# spread (stddev / max-min) of those per-condition daily averages.
Q4_ZONE_DEMAND_WEATHER_VARIATION = f"""
    WITH per_zone_condition AS (
        SELECT
            pulocation_id,
            pickup_zone,
            pickup_borough,
            {WEATHER_CONDITION_EXPR}       AS weather_condition,
            pickup_date,
            COUNT(*)                       AS trips_in_day
        FROM integrated_taxi_trips
        WHERE weather_available = true AND pickup_zone IS NOT NULL
        GROUP BY pulocation_id, pickup_zone, pickup_borough,
                 {WEATHER_CONDITION_EXPR}, pickup_date
    ),
    zone_condition_avg AS (
        SELECT
            pulocation_id,
            pickup_zone,
            pickup_borough,
            weather_condition,
            AVG(trips_in_day)              AS avg_daily_trips
        FROM per_zone_condition
        GROUP BY pulocation_id, pickup_zone, pickup_borough, weather_condition
    )
    SELECT
        pulocation_id,
        pickup_zone,
        pickup_borough,
        COUNT(*)                                       AS weather_conditions_seen,
        AVG(avg_daily_trips)                           AS mean_daily_trips,
        STDDEV_SAMP(avg_daily_trips)                   AS stddev_daily_trips,
        MAX(avg_daily_trips) - MIN(avg_daily_trips)    AS range_daily_trips
    FROM zone_condition_avg
    GROUP BY pulocation_id, pickup_zone, pickup_borough
    HAVING COUNT(*) >= 2
    ORDER BY stddev_daily_trips DESC
"""

# Q5 — Peak travel hours for each day of the week.
# Rank hours within each weekday by average trips-per-day, so the result is
# directly the "peak hours" ordering.
Q5_PEAK_HOURS_BY_WEEKDAY = """
    WITH per_day_hour AS (
        SELECT
            DATE_FORMAT(pickup_timestamp_utc, 'E')          AS weekday,
            (DAYOFWEEK(pickup_timestamp_utc) + 5) % 7       AS weekday_index, -- Mon=0..Sun=6
            HOUR(pickup_timestamp_utc)                      AS pickup_hour,
            pickup_date,
            COUNT(*)                                        AS trips
        FROM integrated_taxi_trips
        GROUP BY 1, 2, 3, pickup_date
    ),
    hour_avg AS (
        SELECT
            weekday,
            weekday_index,
            pickup_hour,
            AVG(trips) AS avg_trips
        FROM per_day_hour
        GROUP BY weekday, weekday_index, pickup_hour
    )
    SELECT
        weekday,
        pickup_hour,
        avg_trips,
        RANK() OVER (PARTITION BY weekday_index ORDER BY avg_trips DESC) AS hour_rank
    FROM hour_avg
    ORDER BY weekday_index, hour_rank
"""

# Q6 — Monthly trends in taxi demand (citywide), with month-over-month change.
Q6_MONTHLY_DEMAND_TREND = """
    WITH monthly AS (
        SELECT
            pickup_year,
            pickup_month,
            COUNT(*)               AS trip_count,
            SUM(fare_amount)       AS total_fare,
            AVG(trip_distance)     AS avg_trip_distance
        FROM integrated_taxi_trips
        GROUP BY pickup_year, pickup_month
    )
    SELECT
        pickup_year,
        pickup_month,
        trip_count,
        total_fare,
        avg_trip_distance,
        LAG(trip_count) OVER (ORDER BY pickup_year, pickup_month) AS prev_month_trip_count,
        trip_count - LAG(trip_count) OVER (ORDER BY pickup_year, pickup_month) AS mom_change,
        ROUND(
            100.0 * (trip_count - LAG(trip_count) OVER (ORDER BY pickup_year, pickup_month))
            / NULLIF(LAG(trip_count) OVER (ORDER BY pickup_year, pickup_month), 0),
            2
        ) AS mom_change_pct
    FROM monthly
    ORDER BY pickup_year, pickup_month
"""


@dataclass(frozen=True)
class AnalyticalQuery:
    """A named analytical query plus a short human description."""

    key: str
    title: str
    sql: str


# Registry — single source of truth used by the runner, optimizer and benchmark.
QUERIES: Dict[str, AnalyticalQuery] = {
    "q1_monthly_demand_by_zone": AnalyticalQuery(
        "q1_monthly_demand_by_zone",
        "Monthly taxi demand for each taxi zone",
        Q1_MONTHLY_DEMAND_BY_ZONE,
    ),
    "q2_avg_distance_by_weather": AnalyticalQuery(
        "q2_avg_distance_by_weather",
        "Average trip distance under different weather conditions",
        Q2_AVG_DISTANCE_BY_WEATHER,
    ),
    "q3_air_quality_vs_demand": AnalyticalQuery(
        "q3_air_quality_vs_demand",
        "Relationship between air quality and taxi demand",
        Q3_AIR_QUALITY_VS_DEMAND,
    ),
    "q4_zone_demand_weather_variation": AnalyticalQuery(
        "q4_zone_demand_weather_variation",
        "Taxi zones with the largest variation in demand across weather conditions",
        Q4_ZONE_DEMAND_WEATHER_VARIATION,
    ),
    "q5_peak_hours_by_weekday": AnalyticalQuery(
        "q5_peak_hours_by_weekday",
        "Peak travel hours for each day of the week",
        Q5_PEAK_HOURS_BY_WEEKDAY,
    ),
    "q6_monthly_demand_trend": AnalyticalQuery(
        "q6_monthly_demand_trend",
        "Monthly trends in taxi demand",
        Q6_MONTHLY_DEMAND_TREND,
    ),
}


def run_query(spark: SparkSession, key: str) -> DataFrame:
    """Execute one analytical query by key and return its DataFrame."""
    if key not in QUERIES:
        raise KeyError(f"Unknown query '{key}'. Known: {sorted(QUERIES)}")
    return spark.sql(QUERIES[key].sql)


def run_all(spark: SparkSession) -> Dict[str, DataFrame]:
    """Execute every analytical query; returns {key: DataFrame} (lazy)."""
    return {key: spark.sql(q.sql) for key, q in QUERIES.items()}


def iter_queries() -> List[AnalyticalQuery]:
    return list(QUERIES.values())

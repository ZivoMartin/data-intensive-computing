from dataclasses import dataclass
from typing import Callable, Dict, List

from pyspark.sql import DataFrame, SparkSession


CANONICAL_VIEWS = {
    "integrated_taxi_trips": "gold/integrated_taxi_trips",
    "silver_taxi_trips": "silver/taxi_trips",
    "silver_weather": "silver/weather",
    "silver_air_quality": "silver/air_quality",
    "silver_taxi_zones": "silver/taxi_zones",
}


def register_views(spark: SparkSession, silver_root: str, gold_root: str) -> None:
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

    key: str
    title: str
    sql: str


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
    if key not in QUERIES:
        raise KeyError(f"Unknown query '{key}'. Known: {sorted(QUERIES)}")
    return spark.sql(QUERIES[key].sql)


def run_all(spark: SparkSession) -> Dict[str, DataFrame]:
    return {key: spark.sql(q.sql) for key, q in QUERIES.items()}


def iter_queries() -> List[AnalyticalQuery]:
    return list(QUERIES.values())

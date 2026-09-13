"""
Task 6 — benchmark two Taxi Trips storage strategies:

  Strategy A: partitioned by (pickup_year, pickup_month)  -- the Task 2 design
  Strategy B: partitioned by (pulocation_id)               -- introduced for this benchmark

For each strategy this module writes the table, then times the three
required queries, and reports storage size + file counts. Results should be
copied into report section 6.6.
"""
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _dir_stats(spark: SparkSession, path: str):
    hconf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    total_bytes, file_count = 0, 0
    it = fs.listFiles(p, True)
    while it.hasNext():
        status = it.next()
        name = status.getPath().getName()
        if name.endswith(".parquet"):
            total_bytes += status.getLen()
            file_count += 1
    return total_bytes, file_count


def _write_strategy(taxi: DataFrame, path: str, partition_cols) -> float:
    start = time.time()
    writer_df = taxi.repartition(*partition_cols).sortWithinPartitions(*partition_cols) if partition_cols else taxi
    writer = writer_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    return time.time() - start


def _run_benchmark_queries(spark: SparkSession, table_path: str, zones_path: str) -> dict:
    taxi = spark.read.format("delta").load(table_path)
    taxi.createOrReplaceTempView("bench_taxi_trips")
    zones = spark.read.format("delta").load(zones_path)
    zones.createOrReplaceTempView("bench_taxi_zones")

    timings = {}

    start = time.time()
    spark.sql(
        """
        SELECT z.borough, COUNT(*) AS trip_count
        FROM bench_taxi_trips t
        JOIN bench_taxi_zones z ON t.pulocation_id = z.location_id
        GROUP BY z.borough
        """
    ).collect()
    timings["trips_per_borough_seconds"] = time.time() - start

    start = time.time()
    spark.sql(
        """
        SELECT pickup_date, AVG(trip_duration_seconds) AS avg_duration_seconds
        FROM bench_taxi_trips
        GROUP BY pickup_date
        """
    ).collect()
    timings["avg_duration_per_day_seconds"] = time.time() - start

    start = time.time()
    spark.sql(
        """
        SELECT z.borough, AVG(t.fare_amount) AS avg_fare
        FROM bench_taxi_trips t
        JOIN bench_taxi_zones z ON t.pulocation_id = z.location_id
        GROUP BY z.borough
        """
    ).collect()
    timings["avg_fare_per_borough_seconds"] = time.time() - start

    return timings


def run_task6_benchmark(spark: SparkSession, silver_root: str, bench_root: str) -> dict:
    taxi = spark.read.format("delta").load(f"{silver_root}/taxi_trips")
    zones_path = f"{silver_root}/taxi_zones"

    strategy_a_path = f"{bench_root}/taxi_trips_by_month"
    strategy_b_path = f"{bench_root}/taxi_trips_by_pulocation"

    results = {}

    ingest_time_a = _write_strategy(taxi, strategy_a_path, ["pickup_year", "pickup_month"])
    bytes_a, files_a = _dir_stats(spark, strategy_a_path)
    query_times_a = _run_benchmark_queries(spark, strategy_a_path, zones_path)
    results["strategy_a_by_month"] = {
        "ingestion_time_seconds": ingest_time_a,
        "storage_bytes": bytes_a,
        "file_count": files_a,
        **query_times_a,
    }

    ingest_time_b = _write_strategy(taxi, strategy_b_path, ["pulocation_id"])
    bytes_b, files_b = _dir_stats(spark, strategy_b_path)
    query_times_b = _run_benchmark_queries(spark, strategy_b_path, zones_path)
    results["strategy_b_by_pulocation"] = {
        "ingestion_time_seconds": ingest_time_b,
        "storage_bytes": bytes_b,
        "file_count": files_b,
        **query_times_b,
    }

    return results


def print_results_markdown(results: dict) -> None:
    """Emit a markdown table ready to paste into report section 6.6."""
    a, b = results["strategy_a_by_month"], results["strategy_b_by_pulocation"]
    print("| Metric | Strategy A (by month) | Strategy B (by pulocation_id) |")
    print("|---|---|---|")
    for key, label in [
        ("ingestion_time_seconds", "Ingestion time (s)"),
        ("storage_bytes", "Storage size (bytes)"),
        ("file_count", "File count"),
        ("trips_per_borough_seconds", "Query: trips per borough (s)"),
        ("avg_duration_per_day_seconds", "Query: avg duration per day (s)"),
        ("avg_fare_per_borough_seconds", "Query: avg fare per borough (s)"),
    ]:
        print(f"| {label} | {a[key]:.3f} | {b[key]:.3f} |" if isinstance(a[key], float) else f"| {label} | {a[key]} | {b[key]} |")

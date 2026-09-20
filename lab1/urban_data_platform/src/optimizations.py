import statistics
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import queries


def _materialize(df: DataFrame) -> int:
    df.write.format("noop").mode("overwrite").save()
    return 1


def time_query(spark: SparkSession, sql: str, runs: int = 3, warmup: bool = True) -> Dict:
    if warmup:
        _materialize(spark.sql(sql))
    samples: List[float] = []
    for _ in range(runs):
        start = time.time()
        _materialize(spark.sql(sql))
        samples.append(time.time() - start)
    return {
        "runs": runs,
        "median_seconds": statistics.median(samples),
        "min_seconds": min(samples),
        "samples_seconds": samples,
    }


def explain_formatted(spark: SparkSession, sql: str) -> str:
    return spark.sql(f"EXPLAIN FORMATTED {sql}").collect()[0][0]


def results_match(left: DataFrame, right: DataFrame) -> bool:
    if left.columns != right.columns:
        common = sorted(left.columns)
        if sorted(left.columns) != sorted(right.columns):
            return False
        left = left.select(*common)
        right = right.select(*common)
    lc, rc = left.count(), right.count()
    if lc != rc:
        return False
    return left.exceptAll(right).count() == 0 and right.exceptAll(left).count() == 0


@dataclass
class OptimizationResult:
    technique: str
    query_key: str
    baseline_seconds: float
    optimized_seconds: float
    results_identical: bool
    baseline_plan: str = ""
    optimized_plan: str = ""
    notes: str = ""
    extra: Dict = field(default_factory=dict)

    @property
    def speedup(self) -> float:
        if self.optimized_seconds <= 0:
            return float("nan")
        return self.baseline_seconds / self.optimized_seconds

    def summary(self) -> Dict:
        return {
            "technique": self.technique,
            "query_key": self.query_key,
            "baseline_seconds": round(self.baseline_seconds, 4),
            "optimized_seconds": round(self.optimized_seconds, 4),
            "speedup_x": round(self.speedup, 3),
            "results_identical": self.results_identical,
            "notes": self.notes,
            **{k: v for k, v in self.extra.items()},
        }


class _ConfGuard:

    def __init__(self, spark: SparkSession, overrides: Dict[str, str]):
        self.spark = spark
        self.overrides = overrides
        self._previous: Dict[str, Optional[str]] = {}

    def __enter__(self):
        for k, v in self.overrides.items():
            try:
                self._previous[k] = self.spark.conf.get(k)
            except Exception:
                self._previous[k] = None
            self.spark.conf.set(k, v)
        return self

    def __exit__(self, *exc):
        for k, prev in self._previous.items():
            if prev is None:
                self.spark.conf.unset(k)
            else:
                self.spark.conf.set(k, prev)
        return False


def experiment_caching(
    spark: SparkSession, query_key: str = "q5_peak_hours_by_weekday", runs: int = 3
) -> OptimizationResult:
    sql = queries.QUERIES[query_key].sql

    spark.catalog.clearCache()
    baseline = time_query(spark, sql, runs=runs)
    baseline_plan = explain_formatted(spark, sql)
    baseline_df = spark.sql(sql)

    cached = spark.table("integrated_taxi_trips").cache()
    cached.createOrReplaceTempView("integrated_taxi_trips")
    cached.count()
    optimized = time_query(spark, sql, runs=runs)
    optimized_plan = explain_formatted(spark, sql)
    optimized_df = spark.sql(sql)

    identical = results_match(baseline_df, optimized_df)

    spark.catalog.clearCache()
    spark.table("integrated_taxi_trips").unpersist(blocking=False)

    return OptimizationResult(
        technique="caching",
        query_key=query_key,
        baseline_seconds=baseline["median_seconds"],
        optimized_seconds=optimized["median_seconds"],
        results_identical=identical,
        baseline_plan=baseline_plan,
        optimized_plan=optimized_plan,
        notes=(
            "Cached integrated_taxi_trips in memory before re-running a full-scan "
            "query. Look for InMemoryTableScan / InMemoryRelation in the optimized "
            "plan. Caveat for the report: the baseline is preceded by a warm-up run, "
            "so the OS page cache is already hot and the baseline is NOT a cold-disk "
            "read. The measured gain therefore reflects skipping deserialization and "
            "column decoding, not disk I/O — expect it to be modest on a single node "
            "and larger on a cluster where the data does not fit in page cache."
        ),
        extra={
            "baseline_samples": baseline["samples_seconds"],
            "optimized_samples": optimized["samples_seconds"],
        },
    )


def experiment_partition_pruning(
    spark: SparkSession, runs: int = 3
) -> OptimizationResult:
    yr_mo = (
        spark.table("integrated_taxi_trips")
        .select("pickup_year", "pickup_month")
        .distinct()
        .orderBy("pickup_year", "pickup_month")
        .limit(1)
        .collect()
    )
    if not yr_mo:
        raise RuntimeError("integrated_taxi_trips has no rows — cannot run pruning experiment.")
    year, month = yr_mo[0]["pickup_year"], yr_mo[0]["pickup_month"]

    baseline_sql = f"""
        SELECT pulocation_id, COUNT(*) AS trip_count, AVG(trip_distance) AS avg_dist
        FROM integrated_taxi_trips
        WHERE YEAR(pickup_date) = {year} AND MONTH(pickup_date) = {month}
        GROUP BY pulocation_id
    """
    optimized_sql = f"""
        SELECT pulocation_id, COUNT(*) AS trip_count, AVG(trip_distance) AS avg_dist
        FROM integrated_taxi_trips
        WHERE pickup_year = {year} AND pickup_month = {month}
        GROUP BY pulocation_id
    """

    spark.catalog.clearCache()
    baseline = time_query(spark, baseline_sql, runs=runs)
    optimized = time_query(spark, optimized_sql, runs=runs)

    baseline_plan = explain_formatted(spark, baseline_sql)
    optimized_plan = explain_formatted(spark, optimized_sql)

    identical = results_match(spark.sql(baseline_sql), spark.sql(optimized_sql))

    return OptimizationResult(
        technique="partition_pruning",
        query_key="monthly_zone_agg_same_scope",
        baseline_seconds=baseline["median_seconds"],
        optimized_seconds=optimized["median_seconds"],
        results_identical=identical,
        baseline_plan=baseline_plan,
        optimized_plan=optimized_plan,
        notes=(
            f"Both queries return the same month ({year}-{month:02d}). Baseline filters "
            "on the data column pickup_date (no pruning); optimized filters on the "
            "partition columns pickup_year/pickup_month. In the plans, compare the "
            "FileScan node's PartitionFilters entry and the 'number of files read' / "
            "'size of files read' metrics."
        ),
        extra={
            "partition_year": year,
            "partition_month": month,
            "baseline_samples": baseline["samples_seconds"],
            "optimized_samples": optimized["samples_seconds"],
        },
    )


def experiment_broadcast_join(spark: SparkSession, runs: int = 3) -> OptimizationResult:
    baseline_sql = """
        SELECT z.borough, COUNT(*) AS trip_count, AVG(t.fare_amount) AS avg_fare
        FROM silver_taxi_trips t
        JOIN silver_taxi_zones z ON t.pulocation_id = z.location_id
        GROUP BY z.borough
    """
    optimized_sql = """
        SELECT /*+ BROADCAST(z) */ z.borough, COUNT(*) AS trip_count, AVG(t.fare_amount) AS avg_fare
        FROM silver_taxi_trips t
        JOIN silver_taxi_zones z ON t.pulocation_id = z.location_id
        GROUP BY z.borough
    """

    spark.catalog.clearCache()

    with _ConfGuard(spark, {"spark.sql.autoBroadcastJoinThreshold": "-1"}):
        baseline = time_query(spark, baseline_sql, runs=runs)
        baseline_plan = explain_formatted(spark, baseline_sql)
        baseline_df = spark.sql(baseline_sql)

    optimized = time_query(spark, optimized_sql, runs=runs)
    optimized_plan = explain_formatted(spark, optimized_sql)
    optimized_df = spark.sql(optimized_sql)

    identical = results_match(baseline_df, optimized_df)

    return OptimizationResult(
        technique="broadcast_join",
        query_key="trips_per_borough_join",
        baseline_seconds=baseline["median_seconds"],
        optimized_seconds=optimized["median_seconds"],
        results_identical=identical,
        baseline_plan=baseline_plan,
        optimized_plan=optimized_plan,
        notes=(
            "Baseline forces SortMergeJoin (autoBroadcastJoinThreshold=-1); optimized "
            "uses a BROADCAST hint. Expect BroadcastHashJoin + BroadcastExchange in "
            "the optimized plan and no shuffle of the large fact table."
        ),
        extra={
            "baseline_samples": baseline["samples_seconds"],
            "optimized_samples": optimized["samples_seconds"],
        },
    )


def experiment_aqe(
    spark: SparkSession, query_key: str = "q4_zone_demand_weather_variation", runs: int = 3
) -> OptimizationResult:
    sql = queries.QUERIES[query_key].sql

    spark.catalog.clearCache()
    with _ConfGuard(spark, {"spark.sql.adaptive.enabled": "false"}):
        baseline = time_query(spark, sql, runs=runs)
        baseline_plan = explain_formatted(spark, sql)
        baseline_df = spark.sql(sql)

    with _ConfGuard(spark, {"spark.sql.adaptive.enabled": "true"}):
        optimized = time_query(spark, sql, runs=runs)
        optimized_plan = explain_formatted(spark, sql)
        optimized_df = spark.sql(sql)

    identical = results_match(baseline_df, optimized_df)

    return OptimizationResult(
        technique="aqe",
        query_key=query_key,
        baseline_seconds=baseline["median_seconds"],
        optimized_seconds=optimized["median_seconds"],
        results_identical=identical,
        baseline_plan=baseline_plan,
        optimized_plan=optimized_plan,
        notes=(
            "Same query with spark.sql.adaptive.enabled false vs true. With AQE the "
            "plan shows AdaptiveSparkPlan isFinalPlan=true and coalesced shuffle "
            "partitions; effect is largest on skewed / over-partitioned shuffles."
        ),
        extra={
            "baseline_samples": baseline["samples_seconds"],
            "optimized_samples": optimized["samples_seconds"],
        },
    )


EXPERIMENTS: Dict[str, Callable[..., OptimizationResult]] = {
    "caching": experiment_caching,
    "partition_pruning": experiment_partition_pruning,
    "broadcast_join": experiment_broadcast_join,
    "aqe": experiment_aqe,
}


def run_all_experiments(spark: SparkSession, runs: int = 3) -> List[OptimizationResult]:
    results: List[OptimizationResult] = []
    for name, fn in EXPERIMENTS.items():
        print(f"[optimize] running experiment: {name}")
        results.append(fn(spark, runs=runs))
    return results

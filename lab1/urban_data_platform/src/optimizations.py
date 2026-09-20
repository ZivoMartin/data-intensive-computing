"""
Week 2 — Task 3: query-optimization experiments.

This module runs controlled before/after experiments for the four required
optimization techniques and produces, for each experiment:

  * a wall-clock timing (median of N timed runs after a warm-up run),
  * the ``EXPLAIN FORMATTED`` physical plan of both the baseline and the
    optimized query,
  * a result-equality check proving the optimization did not change output,
  * a short machine-readable record the benchmark/report modules consume.

The four techniques (assignment Task 3)
---------------------------------------
    caching             Cache a frequently accessed table / intermediate result.
    partition_pruning   Express the same time window as a partition predicate.
    broadcast_join      Force a broadcast of the small dimension tables.
    aqe                 Compare the same query with AQE enabled vs disabled.

Everything is expressed against the canonical temp views registered by
``queries.register_views`` so the baseline query text stays identical to the
library version.
"""
import statistics
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import queries


# --------------------------------------------------------------------------
# Timing / plan / equality primitives
# --------------------------------------------------------------------------
def _materialize(df: DataFrame) -> int:
    """Force full execution of a DataFrame.

    ``count()`` is not a safe timing action: Spark can prune projections and
    even whole aggregation branches when only the row count is needed, so it
    measures less work than the real query. Writing to the ``noop`` sink runs
    the complete physical plan and discards the rows, which isolates compute
    cost from driver-side collection.
    """
    df.write.format("noop").mode("overwrite").save()
    return 1


def time_query(spark: SparkSession, sql: str, runs: int = 3, warmup: bool = True) -> Dict:
    """Time a SQL query. Returns median/min/all timings in seconds."""
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
    """Return the EXPLAIN FORMATTED physical plan text for a query.

    Uses the supported SQL surface (``EXPLAIN FORMATTED <query>``) rather than
    reaching into the private ``_jdf.queryExecution()`` JVM API, so this does
    not break across Spark minor versions.
    """
    return spark.sql(f"EXPLAIN FORMATTED {sql}").collect()[0][0]


def results_match(left: DataFrame, right: DataFrame) -> bool:
    """True iff both queries produce the same multiset of rows.

    Order-independent: compares counts and the symmetric difference. Used to
    verify an optimization preserved semantics.
    """
    if left.columns != right.columns:
        # Column order can differ harmlessly; align by name before comparing.
        common = sorted(left.columns)
        if sorted(left.columns) != sorted(right.columns):
            return False
        left = left.select(*common)
        right = right.select(*common)
    lc, rc = left.count(), right.count()
    if lc != rc:
        return False
    # exceptAll both directions == identical multisets
    return left.exceptAll(right).count() == 0 and right.exceptAll(left).count() == 0


# --------------------------------------------------------------------------
# Experiment result record
# --------------------------------------------------------------------------
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


# --------------------------------------------------------------------------
# Config helpers (save / restore session flags so experiments don't leak)
# --------------------------------------------------------------------------
class _ConfGuard:
    """Context manager that sets Spark confs and restores them on exit."""

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


# --------------------------------------------------------------------------
# Experiment 1 — Caching
# --------------------------------------------------------------------------
def experiment_caching(
    spark: SparkSession, query_key: str = "q5_peak_hours_by_weekday", runs: int = 3
) -> OptimizationResult:
    """Cache the integrated table, then re-time a query that scans it fully.

    Q5 (peak hours by weekday) scans the whole integrated table and does no
    partition-friendly filtering, so it benefits from having the table pinned
    in memory across repeated analyst queries.
    """
    sql = queries.QUERIES[query_key].sql

    # Baseline: uncached.
    spark.catalog.clearCache()
    baseline = time_query(spark, sql, runs=runs)
    baseline_plan = explain_formatted(spark, sql)
    baseline_df = spark.sql(sql)

    # Optimized: cache + eager-materialize the source view, then re-time.
    cached = spark.table("integrated_taxi_trips").cache()
    cached.createOrReplaceTempView("integrated_taxi_trips")
    cached.count()  # force the cache to populate
    optimized = time_query(spark, sql, runs=runs)
    optimized_plan = explain_formatted(spark, sql)
    optimized_df = spark.sql(sql)

    identical = results_match(baseline_df, optimized_df)

    # Restore an uncached view so later experiments start from a clean state.
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


# --------------------------------------------------------------------------
# Experiment 2 — Partition pruning
# --------------------------------------------------------------------------
def experiment_partition_pruning(
    spark: SparkSession, runs: int = 3
) -> OptimizationResult:
    """Compare two queries that return the SAME rows but differ in prunability.

    The integrated table is partitioned by (pickup_year, pickup_month). The
    baseline expresses its time filter on ``pickup_date`` — a *data* column,
    so Spark must open every partition directory and filter row groups. The
    optimized query expresses the identical time window as an equality
    predicate on the two *partition* columns, so the file listing itself is
    pruned and only one directory is read.

    Holding the answer set fixed is what makes this a measurement of pruning
    rather than a measurement of "reading less data", and it lets
    ``results_match`` act as a real correctness check instead of comparing a
    query against itself.
    """
    # Discover an actual (year, month) present in the data.
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

    # Same calendar month, expressed two ways.
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

    # Genuine equality check: the two queries must agree row for row.
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


# --------------------------------------------------------------------------
# Experiment 3 — Broadcast join
# --------------------------------------------------------------------------
def experiment_broadcast_join(spark: SparkSession, runs: int = 3) -> OptimizationResult:
    """Join the large silver taxi-trips table to the small zones lookup.

    Baseline disables auto-broadcast (forcing a shuffle/sort-merge join);
    optimized re-enables it (or uses an explicit BROADCAST hint) so the tiny
    zones table is broadcast to every executor. This is the canonical
    large-fact / small-dimension join.
    """
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

    # Baseline: disable auto broadcast so we get a sort-merge join.
    with _ConfGuard(spark, {"spark.sql.autoBroadcastJoinThreshold": "-1"}):
        baseline = time_query(spark, baseline_sql, runs=runs)
        baseline_plan = explain_formatted(spark, baseline_sql)
        baseline_df = spark.sql(baseline_sql)

    # Optimized: explicit broadcast hint (independent of the threshold).
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


# --------------------------------------------------------------------------
# Experiment 4 — Adaptive Query Execution (AQE)
# --------------------------------------------------------------------------
def experiment_aqe(
    spark: SparkSession, query_key: str = "q4_zone_demand_weather_variation", runs: int = 3
) -> OptimizationResult:
    """Run a shuffle-heavy, multi-stage query with AQE off then on.

    Q4 has several aggregation stages (per-zone/condition/day -> per-zone/
    condition -> per-zone), so it produces skew-prone shuffles that AQE can
    coalesce / re-plan. We compare the same query text under both settings.
    """
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


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
EXPERIMENTS: Dict[str, Callable[..., OptimizationResult]] = {
    "caching": experiment_caching,
    "partition_pruning": experiment_partition_pruning,
    "broadcast_join": experiment_broadcast_join,
    "aqe": experiment_aqe,
}


def run_all_experiments(spark: SparkSession, runs: int = 3) -> List[OptimizationResult]:
    """Run every optimization experiment and return the result records."""
    results: List[OptimizationResult] = []
    for name, fn in EXPERIMENTS.items():
        print(f"[optimize] running experiment: {name}")
        results.append(fn(spark, runs=runs))
    return results

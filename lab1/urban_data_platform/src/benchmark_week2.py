import json
import os
from datetime import datetime, timezone
from typing import Dict, List

from pyspark.sql import SparkSession

import data_products
import optimizations
import queries


PRODUCT_FOR_QUERY = {
    "q2_avg_distance_by_weather": (
        "weather_impact_summary",
        "SELECT * FROM _product_under_test",
    ),
    "q3_air_quality_vs_demand": (
        "air_quality_impact_summary",
        "SELECT * FROM _product_under_test",
    ),
    "q6_monthly_demand_trend": (
        "daily_mobility_summary",
        """
        SELECT
            pickup_year,
            pickup_month,
            SUM(trip_count)                                        AS trip_count,
            SUM(total_fare)                                        AS total_fare,
            SUM(avg_trip_distance * trip_count) / SUM(trip_count)  AS avg_trip_distance
        FROM _product_under_test
        GROUP BY pickup_year, pickup_month
        ORDER BY pickup_year, pickup_month
        """,
    ),
}


def _write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def benchmark_queries(spark: SparkSession, runs: int = 3) -> Dict[str, Dict]:
    spark.catalog.clearCache()
    results = {}
    for key, q in queries.QUERIES.items():
        print(f"[bench] timing {key}")
        timing = optimizations.time_query(spark, q.sql, runs=runs)
        results[key] = {
            "title": q.title,
            "median_seconds": timing["median_seconds"],
            "min_seconds": timing["min_seconds"],
            "samples_seconds": timing["samples_seconds"],
        }
    return results


def collect_product_storage(spark: SparkSession, metadata_root: str) -> List[Dict]:
    path = f"{metadata_root}/data_products"
    try:
        df = spark.read.format("delta").load(path)
    except Exception:
        return []
    latest = (
        df.orderBy("refreshed_at", ascending=False)
        .dropDuplicates(["product_name"])
        .select("product_name", "row_count", "storage_bytes", "schema_version", "refreshed_at")
        .orderBy("product_name")
    )
    return [r.asDict() for r in latest.collect()]


def benchmark_materialization(spark: SparkSession, gold_root: str, runs: int = 3) -> List[Dict]:
    out = []
    for query_key, (product_name, product_sql) in PRODUCT_FOR_QUERY.items():
        product_path = f"{gold_root}/products/{product_name}"
        try:
            spark.read.format("delta").load(product_path).createOrReplaceTempView("_product_under_test")
        except Exception:
            continue
        source_timing = optimizations.time_query(spark, queries.QUERIES[query_key].sql, runs=runs)
        product_timing = optimizations.time_query(spark, product_sql, runs=runs)
        src = source_timing["median_seconds"]
        prod = product_timing["median_seconds"]
        out.append(
            {
                "query_key": query_key,
                "product_name": product_name,
                "source_query_seconds": src,
                "product_read_seconds": prod,
                "speedup_x": round(src / prod, 3) if prod > 0 else None,
            }
        )
    return out


def benchmark_optimizations(spark: SparkSession, plans_dir: str, runs: int = 3) -> List[Dict]:
    results = optimizations.run_all_experiments(spark, runs=runs)
    summaries = []
    for r in results:
        _write_text(
            os.path.join(plans_dir, f"{r.technique}_{r.query_key}_baseline.txt"),
            r.baseline_plan,
        )
        _write_text(
            os.path.join(plans_dir, f"{r.technique}_{r.query_key}_optimized.txt"),
            r.optimized_plan,
        )
        summaries.append(r.summary())
    return summaries


def render_markdown(all_results: Dict) -> str:
    lines: List[str] = []
    lines.append("# Week 2 Benchmark Report\n")
    lines.append(f"_Generated: {all_results['generated_at']}_\n")
    lines.append(f"Timing method: median of {all_results['runs']} runs after one warm-up "
                 "run; each run fully materializes the query via a `noop` write.\n")

    lines.append("\n## 1. Baseline analytical query execution time\n")
    lines.append("| Query | Title | Median (s) | Min (s) |")
    lines.append("|---|---|---|---|")
    for key, v in all_results["query_timings"].items():
        lines.append(f"| {key} | {v['title']} | {v['median_seconds']:.3f} | {v['min_seconds']:.3f} |")

    lines.append("\n## 2. Data product storage overhead\n")
    lines.append("| Product | Rows | Storage (bytes) | Schema version |")
    lines.append("|---|---|---|---|")
    for p in all_results["product_storage"]:
        lines.append(
            f"| {p['product_name']} | {p['row_count']} | {p['storage_bytes']} | {p['schema_version']} |"
        )

    lines.append("\n## 3. Materialization speedup (source query vs product read)\n")
    lines.append("| Query | Product | Source query (s) | Product read (s) | Speedup |")
    lines.append("|---|---|---|---|---|")
    for m in all_results["materialization"]:
        lines.append(
            f"| {m['query_key']} | {m['product_name']} | {m['source_query_seconds']:.3f} | "
            f"{m['product_read_seconds']:.3f} | {m['speedup_x']}x |"
        )

    lines.append("\n## 4. Optimization experiments\n")
    lines.append("| Technique | Query | Baseline (s) | Optimized (s) | Speedup | Results identical |")
    lines.append("|---|---|---|---|---|---|")
    for o in all_results["optimizations"]:
        lines.append(
            f"| {o['technique']} | {o['query_key']} | {o['baseline_seconds']:.3f} | "
            f"{o['optimized_seconds']:.3f} | {o['speedup_x']}x | {o['results_identical']} |"
        )

    lines.append("\nEXPLAIN FORMATTED plans for each experiment are saved under `plans/`.\n")
    lines.append("\n### Notes per experiment\n")
    for o in all_results["optimizations"]:
        lines.append(f"- **{o['technique']}** ({o['query_key']}): {o['notes']}")

    return "\n".join(lines) + "\n"


def run_week2_benchmark(spark: SparkSession, silver_root: str, gold_root: str,
                        metadata_root: str, bench_root: str, runs: int = 3) -> Dict:
    queries.register_views(spark, silver_root, gold_root)

    query_timings = benchmark_queries(spark, runs=runs)
    product_storage = collect_product_storage(spark, metadata_root)
    materialization = benchmark_materialization(spark, gold_root, runs=runs)

    plans_dir = os.path.join(bench_root, "plans")
    optimization_summaries = benchmark_optimizations(spark, plans_dir, runs=runs)

    all_results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runs": runs,
        "query_timings": query_timings,
        "product_storage": product_storage,
        "materialization": materialization,
        "optimizations": optimization_summaries,
    }

    _write_text(os.path.join(bench_root, "results.json"), json.dumps(all_results, indent=2, default=str))
    _write_text(os.path.join(bench_root, "benchmark_report.md"), render_markdown(all_results))
    print(f"[bench] wrote results.json + benchmark_report.md to {bench_root}")
    return all_results

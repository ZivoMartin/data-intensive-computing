import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if not SRC_DIR.is_dir():
    raise RuntimeError(f"Expected src/ directory at {SRC_DIR}, but it does not exist.")
sys.path.insert(0, str(SRC_DIR))

from pyspark.sql import SparkSession

from config import default_output_root
import queries
import data_products
import benchmark_week2


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("urban-data-platform-week2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def cmd_run_queries(spark, roots, args):
    queries.register_views(spark, roots["silver"], roots["gold"])
    if args.query:
        df = queries.run_query(spark, args.query)
        df.show(args.limit, truncate=False)
    else:
        for key, df in queries.run_all(spark).items():
            print(f"\n=== {key}: {queries.QUERIES[key].title} ===")
            df.show(args.limit, truncate=False)


def cmd_build_products(spark, roots, args):
    queries.register_views(spark, roots["silver"], roots["gold"])
    records = data_products.build_all_products(spark, roots["gold"], roots["metadata"])
    for r in records:
        print(f"[product] {r['product_name']}: {r['row_count']} rows, {r['storage_bytes']} bytes")


def cmd_run_benchmark(spark, roots, args):
    benchmark_week2.run_week2_benchmark(
        spark,
        silver_root=roots["silver"],
        gold_root=roots["gold"],
        metadata_root=roots["metadata"],
        bench_root=f"{args.output_root}/bench_week2",
        runs=args.runs,
    )


def cmd_all(spark, roots, args):
    cmd_build_products(spark, roots, args)
    cmd_run_benchmark(spark, roots, args)


def main():
    parser = argparse.ArgumentParser(description="Urban Data Platform — Week 2")
    parser.add_argument("command", choices=["run-queries", "build-products", "run-benchmark", "all"])
    parser.add_argument("--output-root", required=True,
                        help="Lakehouse root produced by the Week 1 main.py")
    parser.add_argument("--query", help="(run-queries) single query key to run; omit to run all")
    parser.add_argument("--limit", type=int, default=20, help="(run-queries) rows to show")
    parser.add_argument("--runs", type=int, default=3, help="(run-benchmark) timed runs per query")
    args = parser.parse_args()

    roots = default_output_root(args.output_root)
    spark = build_spark()
    try:
        {
            "run-queries": cmd_run_queries,
            "build-products": cmd_build_products,
            "run-benchmark": cmd_run_benchmark,
            "all": cmd_all,
        }[args.command](spark, roots, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

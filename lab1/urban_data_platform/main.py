import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if not SRC_DIR.is_dir():
    raise RuntimeError(
        f"Expected src/ directory at {SRC_DIR}, but it does not exist. "
        f"main.py resolved its own location to {PROJECT_ROOT} — "
        f"confirm config.py/io_utils.py/etc. actually live in a 'src' folder next to main.py."
    )
sys.path.insert(0, str(SRC_DIR))

from pyspark.sql import SparkSession

from config import DatasetSpec, default_output_root
from ingestion import ingest_dataset
from integration import build_integrated_taxi_trips
from benchmark import run_task6_benchmark, print_results_markdown
import transforms


def build_specs(input_root: str):
    return [
        DatasetSpec(
            name="taxi_trips",
            file_format="parquet",
            input_path=f"{input_root}/taxi_trips/*.parquet",
            required_columns=[
                "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                "PULocationID", "DOLocationID", "fare_amount",
            ],
            primary_key=["trip_id"],
            partition_cols=["pickup_year", "pickup_month"],
            transform=transforms.transform_taxi_trips,
            numeric_range_checks={"fare_amount": (0, 1000), "trip_distance": (0, 500)},
        ),
        DatasetSpec(
            name="weather",
            file_format="csv",
            input_path=f"{input_root}/weather/*.csv",
            required_columns=["year", "month", "day", "hour"],
            primary_key=["observation_hour_utc"],
            partition_cols=[],
            transform=transforms.transform_weather,
        ),
        DatasetSpec(
            name="air_quality",
            file_format="csv",
            input_path=f"{input_root}/air_quality/*.csv",
            required_columns=[
                "State Code", "County Code", "Site Num", "Parameter Code", "POC",
                "Date Local", "Time Local",
            ],
            primary_key=[
                "state_code", "county_code", "site_num", "parameter_code", "poc",
                "date_local", "time_local",
            ],
            partition_cols=["observation_year", "observation_month"],
            transform=transforms.transform_air_quality,
        ),
        DatasetSpec(
            name="taxi_zones",
            file_format="csv",
            input_path=f"{input_root}/taxi_zones/*.csv",
            required_columns=["LocationID", "Borough", "Zone", "service_zone"],
            primary_key=["location_id"],
            partition_cols=[],
            transform=transforms.transform_taxi_zones,
        ),
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--run-benchmark", action="store_true")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("urban-data-platform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    roots = default_output_root(args.output_root)
    for spec in build_specs(args.input_root):
        record = ingest_dataset(spark, spec, roots)
        print(f"[ingest] {spec.name}: {record}")

    build_integrated_taxi_trips(spark, roots["silver"], roots["gold"])
    print("[integrate] gold/integrated_taxi_trips written")

    if args.run_benchmark:
        results = run_task6_benchmark(spark, roots["silver"], f"{args.output_root}/bench")
        print_results_markdown(results)

    spark.stop()


if __name__ == "__main__":
    main()

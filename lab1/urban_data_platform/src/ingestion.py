"""
Generic ingestion pipeline.

ingest_dataset() is the one function used for every dataset in the platform.
Everything dataset-specific is pulled from the DatasetSpec passed in, or from
the transform function it references.
"""
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import DatasetSpec
from io_utils import (
    load_raw,
    normalize_nulls,
    standardize_column_names,
    write_bronze_copy,
    write_delta,
)
from quality import (
    check_duplicate_primary_key,
    check_numeric_ranges,
    check_primary_key_nulls,
    check_timestamp_columns,
    split_valid_rejected,
    validate_required_schema,
)

TIMESTAMP_CHECK_COLUMNS = {
    "taxi_trips": ["pickup_timestamp_utc", "dropoff_timestamp_utc"],
    "weather": ["observation_timestamp_utc"],
    "air_quality": ["observation_timestamp_utc"],
    "taxi_zones": [],
}


def ingest_dataset(spark: SparkSession, spec: DatasetSpec, output_roots: dict) -> dict:
    """
    Run the full generic ingestion sequence for one dataset and return an
    ingestion-metadata record (also appended to metadata/ingestion_runs).
    """
    start = time.time()

    raw_df = load_raw(spark, spec)
    validate_required_schema(standardize_column_names(raw_df), spec)  # fail fast, pre-transform

    df = standardize_column_names(raw_df)
    write_bronze_copy(df, output_roots["bronze"], spec.name)

    df = normalize_nulls(df)

    if spec.transform is not None:
        df = spec.transform(df)

    df = check_primary_key_nulls(df, spec.primary_key)
    df = check_duplicate_primary_key(df, spec.primary_key)
    df = check_timestamp_columns(df, TIMESTAMP_CHECK_COLUMNS.get(spec.name, []))
    df = check_numeric_ranges(df, spec.numeric_range_checks)

    # Taxi-specific quality rule lives here rather than in generic quality.py
    # because "dropoff before pickup" only makes sense for a trip dataset —
    # generic checks stay dataset-agnostic, this one stays local to the caller.
    if spec.name == "taxi_trips":
        from quality import _append_reason  # noqa: PLC0415 (kept local to this branch)

        df = _append_reason(
            df,
            (F.col("dropoff_timestamp_utc") < F.col("pickup_timestamp_utc"))
            | (F.col("trip_duration_seconds") <= 0),
            "dropoff_before_or_equal_pickup",
        )

    valid_df, rejected_df = split_valid_rejected(df)

    silver_path = f"{output_roots['silver']}/{spec.name}"
    write_delta(valid_df, silver_path, partition_cols=spec.partition_cols)

    rejected_path = f"{output_roots['metadata']}/rejected_records/{spec.name}"
    write_delta(rejected_df, rejected_path)

    valid_count = valid_df.count()
    rejected_count = rejected_df.count()
    elapsed = time.time() - start

    run_record = {
        "dataset_name": spec.name,
        "source_path": spec.input_path,
        "valid_records": valid_count,
        "rejected_records": rejected_count,
        "execution_time_seconds": elapsed,
        "schema_version": spec.schema_version,
        "ingestion_timestamp": datetime.now(timezone.utc),
    }
    _append_ingestion_metadata(spark, output_roots["metadata"], run_record)
    return run_record


def _append_ingestion_metadata(spark: SparkSession, metadata_root: str, record: dict) -> None:
    row_df = spark.createDataFrame([record])
    (
        row_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(f"{metadata_root}/ingestion_runs")
    )

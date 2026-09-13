"""
Generic, dataset-agnostic I/O helpers.

Nothing in this module knows about "taxi trips" or "weather" — it only knows
about file formats, naming conventions, and null handling. All dataset
semantics live in transforms.py.
"""
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import DatasetSpec, NULL_MARKERS


def load_raw(spark: SparkSession, spec: DatasetSpec) -> DataFrame:
    """Load a dataset from its declared format. Generic across all datasets."""
    if spec.file_format == "csv":
        return (
            spark.read.options(**spec.csv_options).csv(spec.input_path)
        )
    if spec.file_format == "parquet":
        return spark.read.parquet(spec.input_path)
    raise ValueError(f"Unsupported file_format '{spec.file_format}' for dataset '{spec.name}'")


def write_bronze_copy(df: DataFrame, bronze_root: str, dataset_name: str) -> None:
    """
    Land an unmodified copy of the raw load as Delta before any
    transformation is applied. This is the actual bronze layer promised by
    the Task 2 directory design: a reproducible, queryable record of exactly
    what was ingested, independent of whether the original source file is
    later moved, rotated, or deleted upstream.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{bronze_root}/{dataset_name}")
    )


def _to_snake_case(col_name: str) -> str:
    name = col_name.strip()
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)          # spaces/punctuation -> _
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)  # camelCase -> camel_Case
    name = re.sub(r"_+", "_", name).strip("_")
    return name.lower()


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Convert every column name to lowercase snake_case (Task 4.3)."""
    renamed = [F.col(c).alias(_to_snake_case(c)) for c in df.columns]
    return df.select(*renamed)


def normalize_nulls(df: DataFrame) -> DataFrame:
    """
    Convert common textual null markers to real SQL NULL for every string
    column (Task 4.4). Non-string columns are left untouched.
    """
    string_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == "string"]
    for c in string_cols:
        df = df.withColumn(
            c, F.when(F.trim(F.col(c)).isin(list(NULL_MARKERS)), None).otherwise(F.col(c))
        )
    return df


def write_delta(df: DataFrame, path: str, partition_cols=None, mode: str = "overwrite") -> None:
    writer_df = df
    if partition_cols:
        # Repartition by the partition columns first (not just sort within
        # each task's existing rows) so rows sharing a partition value are
        # concentrated into a small number of Spark tasks. Without this, a
        # single task's rows can span several output partitions, forcing
        # several partition-file writers open at once and multiplying
        # Parquet's per-writer row-group memory (see the "Scaling row group
        # sizes for N writers" warnings this produces under memory pressure).
        writer_df = df.repartition(*partition_cols).sortWithinPartitions(*partition_cols)
    writer = writer_df.write.format("delta").mode(mode).option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)

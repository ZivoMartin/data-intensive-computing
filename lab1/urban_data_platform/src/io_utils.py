import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import DatasetSpec, NULL_MARKERS


def load_raw(spark: SparkSession, spec: DatasetSpec) -> DataFrame:
    if spec.file_format == "csv":
        return (
            spark.read.options(**spec.csv_options).csv(spec.input_path)
        )
    if spec.file_format == "parquet":
        return spark.read.parquet(spec.input_path)
    raise ValueError(f"Unsupported file_format '{spec.file_format}' for dataset '{spec.name}'")


def write_bronze_copy(df: DataFrame, bronze_root: str, dataset_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{bronze_root}/{dataset_name}")
    )


def _to_snake_case(col_name: str) -> str:
    name = col_name.strip()
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)          
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name) 
    name = re.sub(r"_+", "_", name).strip("_")
    return name.lower()


def standardize_column_names(df: DataFrame) -> DataFrame:
    renamed = [F.col(c).alias(_to_snake_case(c)) for c in df.columns]
    return df.select(*renamed)


def normalize_nulls(df: DataFrame) -> DataFrame:
    string_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == "string"]
    for c in string_cols:
        df = df.withColumn(
            c, F.when(F.trim(F.col(c)).isin(list(NULL_MARKERS)), None).otherwise(F.col(c))
        )
    return df


def write_delta(df: DataFrame, path: str, partition_cols=None, mode: str = "overwrite") -> None:
    writer_df = df
    if partition_cols:
        writer_df = df.repartition(*partition_cols).sortWithinPartitions(*partition_cols)
    writer = writer_df.write.format("delta").mode(mode).option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)

"""
Generic data-quality framework.

Every check here operates on column names and PK/range definitions passed in
via DatasetSpec — nothing here hardcodes a dataset name.
"""
from typing import List, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config import DatasetSpec

REJECTION_COL = "rejection_reasons"


def validate_required_schema(df: DataFrame, spec: DatasetSpec) -> None:
    """
    Fail fast if the raw source is missing columns the platform depends on.
    Standardized (snake_case) names are compared, so this must run AFTER
    standardize_column_names.
    """
    from io_utils import _to_snake_case  # local import avoids a circular dep at module load

    present = set(df.columns)
    expected = {_to_snake_case(c) for c in spec.required_columns}
    missing = expected - present
    if missing:
        raise ValueError(
            f"[{spec.name}] missing required columns after standardization: {sorted(missing)}. "
            f"Upstream schema has changed — refusing to silently produce null columns."
        )


def _append_reason(df: DataFrame, condition, message: str) -> DataFrame:
    """Append `message` to the rejection_reasons array whenever `condition` is true."""
    if REJECTION_COL not in df.columns:
        df = df.withColumn(REJECTION_COL, F.array().cast("array<string>"))
    return df.withColumn(
        REJECTION_COL,
        F.when(condition, F.array_union(F.col(REJECTION_COL), F.array(F.lit(message))))
        .otherwise(F.col(REJECTION_COL)),
    )


def check_primary_key_nulls(df: DataFrame, primary_key: List[str]) -> DataFrame:
    if not primary_key:
        return df
    cond = F.lit(False)
    for c in primary_key:
        cond = cond | F.col(c).isNull()
    return _append_reason(df, cond, f"null_primary_key:{','.join(primary_key)}")


def check_duplicate_primary_key(df: DataFrame, primary_key: List[str]) -> DataFrame:
    if not primary_key:
        return df
    w = Window.partitionBy(*primary_key)
    df = df.withColumn("_pk_count", F.count(F.lit(1)).over(w))
    df = _append_reason(df, F.col("_pk_count") > 1, f"duplicate_primary_key:{','.join(primary_key)}")
    return df.drop("_pk_count")


def check_timestamp_columns(df: DataFrame, timestamp_cols: List[str]) -> DataFrame:
    for c in timestamp_cols:
        if c in df.columns:
            df = _append_reason(df, F.col(c).isNull(), f"invalid_timestamp:{c}")
    return df


def check_numeric_ranges(df: DataFrame, ranges: dict) -> DataFrame:
    for col_name, (lo, hi) in ranges.items():
        if col_name in df.columns:
            cond = F.col(col_name).isNotNull() & ((F.col(col_name) < lo) | (F.col(col_name) > hi))
            df = _append_reason(df, cond, f"out_of_range:{col_name}[{lo},{hi}]")
    return df


def split_valid_rejected(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    if REJECTION_COL not in df.columns:
        df = df.withColumn(REJECTION_COL, F.array().cast("array<string>"))
    rejected = df.filter(F.size(F.col(REJECTION_COL)) > 0)
    valid = df.filter(F.size(F.col(REJECTION_COL)) == 0).drop(REJECTION_COL)
    return valid, rejected

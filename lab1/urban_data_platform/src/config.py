"""
Dataset configuration model.

Every dataset the platform ingests is described declaratively by a
DatasetSpec. The generic ingestion framework (ingestion.py) never contains
dataset-name-specific logic — anything dataset-specific lives either in this
spec or in a small transform function registered on it (see transforms.py).
"""
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame

# Textual markers that should be normalized to real SQL NULL before any
# dataset-specific transformation runs.
NULL_MARKERS = {"", "NA", "N/A", "null", "NULL", "None", "NaN", "-"}


@dataclass
class DatasetSpec:
    name: str
    file_format: str  # "csv" | "parquet"
    input_path: str

    # Raw source column names required to exist before transformation.
    required_columns: List[str]

    # Column names AFTER standardization + transform, used for PK checks.
    primary_key: List[str]

    # Partition columns for the Delta write (post-transform column names).
    partition_cols: List[str] = field(default_factory=list)

    # Dataset-specific transformation: raw-standardized DF -> common-model DF.
    transform: Optional[Callable[[DataFrame], DataFrame]] = None

    # {column_name: (min, max)} numeric domain checks, applied post-transform.
    numeric_range_checks: Dict[str, Tuple[float, float]] = field(default_factory=dict)

    # Options passed to the CSV reader. Ignored for parquet sources.
    # inferSchema is deliberately OFF: Spark's inference can misclassify a
    # bare time-only value (e.g. "19:00") as a full TIMESTAMP, silently
    # filling in the current system date as the implicit date part. Every
    # column loads as a plain string instead; each transform function casts
    # explicitly wherever it actually needs a numeric or temporal type.
    csv_options: Dict[str, str] = field(
        default_factory=lambda: {"header": "true", "inferSchema": "false"}
    )

    schema_version: str = "1.0"


def default_output_root(base: str) -> Dict[str, str]:
    """Standard directory layout, matching the Task 2 storage architecture."""
    return {
        "bronze": f"{base}/bronze",
        "silver": f"{base}/silver",
        "gold": f"{base}/gold",
        "metadata": f"{base}/metadata",
    }

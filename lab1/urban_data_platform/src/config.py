from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame

NULL_MARKERS = {"", "NA", "N/A", "null", "NULL", "None", "NaN", "-"}

@dataclass
class DatasetSpec:
    name: str
    file_format: str
    input_path: str

    required_columns: List[str]

    primary_key: List[str]

    partition_cols: List[str] = field(default_factory=list)

    transform: Optional[Callable[[DataFrame], DataFrame]] = None

    numeric_range_checks: Dict[str, Tuple[float, float]] = field(default_factory=dict)

    csv_options: Dict[str, str] = field(
        default_factory=lambda: {"header": "true", "inferSchema": "false"}
    )

    schema_version: str = "1.0"


def default_output_root(base: str) -> Dict[str, str]:
    return {
        "bronze": f"{base}/bronze",
        "silver": f"{base}/silver",
        "gold": f"{base}/gold",
        "metadata": f"{base}/metadata",
    }

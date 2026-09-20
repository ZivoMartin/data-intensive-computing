# Urban Data Platform — Week 2

Analytical query library, reusable data products, and query-optimization
benchmarks built on top of the Week 1 Delta lakehouse.

## Prerequisites

- Java 8, 11, or 17 (Spark 3.5 does not support Java 21+)
- Python 3.8–3.11 (PySpark 3.5.1 is untested on 3.12+)
- A Week 1 lakehouse already built — this project reads
  `<output-root>/silver/*` and `<output-root>/gold/integrated_taxi_trips`,
  it does not create them

```bash
pip install -r requirements.txt
```

## 1. Run the analytical queries (Task 1 & 2)

Run all six:

```bash
spark-submit \
  --driver-memory 8g --master local[4] \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  run_week2.py run-queries \
  --output-root ./data/lakehouse
```

Run one query with more rows shown:

```bash
spark-submit \
  --driver-memory 8g --master local[4] \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  run_week2.py run-queries \
  --output-root ./data/lakehouse \
  --query q5_peak_hours_by_weekday --limit 30
```

Valid query keys: `q1_monthly_demand_by_zone`, `q2_avg_distance_by_weather`,
`q3_air_quality_vs_demand`, `q4_zone_demand_weather_variation`,
`q5_peak_hours_by_weekday`, `q6_monthly_demand_trend`.

## 2. Generate the reusable data products (Task 4)

```bash
spark-submit \
  --driver-memory 8g --master local[4] \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  run_week2.py build-products \
  --output-root ./data/lakehouse
```

Writes five Delta tables under `./data/lakehouse/gold/products/` —
`daily_mobility_summary`, `taxi_zone_statistics`, `weather_impact_summary`,
`air_quality_impact_summary`, `borough_mobility_summary` — and appends a
build record to `./data/lakehouse/metadata/data_products` on every run.

## 3. Reproduce the benchmark experiments (Task 3 & 5)

```bash
spark-submit \
  --driver-memory 8g --master local[4] \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  run_week2.py run-benchmark \
  --output-root ./data/lakehouse \
  --runs 3
```

This times all six queries, times each query against its matching data
product, runs the four optimization experiments (caching, partition
pruning, broadcast join, AQE), and writes:

```
./data/lakehouse/bench_week2/results.json          machine-readable results
./data/lakehouse/bench_week2/benchmark_report.md    tables for the report
./data/lakehouse/bench_week2/plans/*.txt            EXPLAIN FORMATTED plans
```

## Shortcut — steps 2 + 3 in one command

```bash
spark-submit \
  --driver-memory 8g --master local[4] \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  run_week2.py all \
  --output-root ./data/lakehouse \
  --runs 3
```

## Sanity check before trusting any of the above

Confirm the Week 1 integration join actually matched rows before running
anything in this project — an empty join silently produces valid-looking
zero-row results downstream (weather_available in particular has a history
of being computed against the wrong column name; see integration.py):

```bash
cat > /tmp/check_integration.py <<'EOF'
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
df = spark.read.format("delta").load("./data/lakehouse/gold/integrated_taxi_trips")
df.selectExpr(
    "count(*) AS total_rows",
    "sum(cast(zone_available as int))        AS with_zone",
    "sum(cast(weather_available as int))     AS with_weather",
    "sum(cast(air_quality_available as int)) AS with_air_quality",
).show()
EOF

spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /tmp/check_integration.py
```

All three `with_*` counts should be close to `total_rows`. If `with_weather`
comes back `0`, do not run the benchmark — Q2, Q4, and
`weather_impact_summary` will all silently return zero rows.

## Layout

```
run_week2.py            CLI entrypoint (run-queries / build-products / run-benchmark / all)
src/
  queries.py             the 6 analytical queries (Task 1 & 2)
  data_products.py        the 5 reusable data products (Task 4)
  optimizations.py        the 4 optimization experiments (Task 3)
  benchmark_week2.py       ties timing + storage + plans into results.json / benchmark_report.md (Task 5)
  ingestion.py, integration.py, io_utils.py, quality.py, transforms.py, config.py, benchmark.py
                          Week 1 modules this project depends on
```

## Common gotchas

- **Match the Delta version to Spark.** `delta-spark_2.12:3.2.0` pairs with
  Spark 3.5.x. On a different Spark version, change that coordinate to match
  (e.g. Spark 3.4 → `delta-spark_2.12:3.1.0`).
- **`local[*]` with no `--driver-memory` set defaults the driver heap to
  1g.** The caching experiment in particular tries to hold the full
  `integrated_taxi_trips` table in memory — on a real dataset this will
  `OutOfMemoryError` without an explicit `--driver-memory`. Set it and cap
  parallelism (`--master local[4]` rather than `local[*]`) so fewer tasks
  compete for heap at once.
- **`PythonException: version mismatch`** — driver and worker must use the
  same Python. Set both:
  ```bash
  export PYSPARK_PYTHON=$(which python3)
  export PYSPARK_DRIVER_PYTHON=$(which python3)
  ```
- **`_pickle.PicklingError` / `RecursionError` from cloudpickle** — PySpark
  3.5.1's bundled cloudpickle is not compatible with Python 3.12+. Run under
  Python 3.11 or earlier.
- **Delta JAR won't download (offline/proxy)** — pre-download it once with
  internet access, or run on a cluster that already bundles Delta.

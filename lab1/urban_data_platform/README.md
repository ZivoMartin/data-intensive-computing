# Urban Data Integration Platform

## Layout

```
urban_data_platform/
├── main.py                  # CLI entrypoint
├── src/
│   ├── config.py             # DatasetSpec model
│   ├── io_utils.py           # generic load/write/naming/null helpers
│   ├── quality.py            # generic data-quality checks
│   ├── transforms.py         # dataset-specific transformation functions
│   ├── ingestion.py          # ingest_dataset() — the one function used per dataset
│   ├── integration.py        # Task 5 — builds gold/integrated_taxi_trips
│   └── benchmark.py          # Task 6 — two partitioning strategies, timed
└── urban_data_platform_tasks_2_5_report.md
```

## Expected raw input layout

```
data/raw/
├── taxi_trips/
│   └── yellow_tripdata_2026-01.parquet
├── weather/
│   └── weather.csv
├── air_quality/
│   └── air_quality.csv
└── taxi_zones/
    └── taxi_zone_lookup.csv
```

## Run

```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  main.py \
  --input-root ./data/raw \
  --output-root ./data/lakehouse \
  --run-benchmark
```

Match the `delta-spark` package version to the Spark version in your execution environment. Omit `--run-benchmark` to skip Task 6 and only run ingestion + integration.

## Output

```
data/lakehouse/bronze/<dataset>
data/lakehouse/silver/taxi_trips
data/lakehouse/silver/weather
data/lakehouse/silver/air_quality
data/lakehouse/silver/taxi_zones
data/lakehouse/gold/integrated_taxi_trips
data/lakehouse/metadata/ingestion_runs
data/lakehouse/metadata/rejected_records/<dataset>
data/lakehouse/bench/taxi_trips_by_month        # Task 6, Strategy A
data/lakehouse/bench/taxi_trips_by_pulocation   # Task 6, Strategy B
```

`--run-benchmark` prints a markdown results table at the end of the run — paste it into report section 6.6.

## Notes / known follow-ups

- `taxi_trips.trip_id` is a SHA-256 hash of the full normalized row, not a natural key — see report 3.7 for the reasoning and its limitation (differing any single field produces a different `trip_id`, so this catches exact duplicates only).
- Air-quality context is a citywide hourly mean per pollutant, not zone-specific — no spatial key exists between the two sources with the datasets provided (report 5.4, 5.6).
- Numeric range checks in `main.py`'s `build_specs()` (`fare_amount`, `trip_distance`) are starting values — tighten them against the actual data distribution before relying on them for grading-quality rejection counts.

---

# Week 2 — Querying & Optimizing the Platform

Week 2 extends the platform with an analytical query library, reusable Delta
data products, query-optimization experiments, and a benchmarking harness. It
builds on the Week 1 outputs (`silver/*` and `gold/integrated_taxi_trips`), so
run the Week 1 `main.py` first.

## New layout

```
urban_data_platform/
├── run_week2.py                 # Week 2 CLI entrypoint
├── src/
│   ├── queries.py                # 6 analytical queries (Spark SQL library)
│   ├── optimizations.py          # caching / pruning / broadcast / AQE experiments
│   ├── data_products.py          # 5 materialized Delta data products
│   └── benchmark_week2.py        # Task 5 evaluation harness
├── WEEK2_DESIGN_REPORT.md
└── WEEK2_BENCHMARK_REPORT.md
```

## Analytical queries (Task 1 & 2)

| Key | Analysis |
|---|---|
| `q1_monthly_demand_by_zone` | Monthly taxi demand for each taxi zone |
| `q2_avg_distance_by_weather` | Average trip distance under different weather conditions |
| `q3_air_quality_vs_demand` | Relationship between air quality and taxi demand |
| `q4_zone_demand_weather_variation` | Zones with the largest demand variation across weather |
| `q5_peak_hours_by_weekday` | Peak travel hours for each day of the week |
| `q6_monthly_demand_trend` | Monthly trends in taxi demand |

```bash
# Run all six queries (or one with --query <key>):
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 run_week2.py \
    run-queries --output-root ./data/lakehouse
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 run_week2.py \
    run-queries --output-root ./data/lakehouse --query q5_peak_hours_by_weekday --limit 30
```

## Data products (Task 4)

Five products materialized under `gold/products/<name>`, each carrying
`_data_source`, `_created_at`, `_refreshed_at`, `_schema_version` and logging a
refresh record (rows + bytes) to `metadata/data_products`:
`daily_mobility_summary`, `taxi_zone_statistics`, `weather_impact_summary`,
`air_quality_impact_summary`, `borough_mobility_summary`.

```bash
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 run_week2.py \
    build-products --output-root ./data/lakehouse
```

## Optimization experiments & benchmark (Task 3 & 5)

Runs the four optimization experiments (caching, partition pruning, broadcast
join, AQE) plus baseline query timings, materialization speedups, and product
storage overhead. Writes `bench_week2/results.json`,
`bench_week2/benchmark_report.md`, and `EXPLAIN FORMATTED` plans under
`bench_week2/plans/`.

```bash
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 run_week2.py \
    run-benchmark --output-root ./data/lakehouse --runs 3
# or build products + benchmark in one shot:
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 run_week2.py \
    all --output-root ./data/lakehouse --runs 3
```

## Week 2 output

```
data/lakehouse/gold/products/<product>          # materialized data products
data/lakehouse/metadata/data_products           # product refresh log (rows, bytes)
data/lakehouse/bench_week2/results.json         # machine-readable benchmark
data/lakehouse/bench_week2/benchmark_report.md  # rendered tables
data/lakehouse/bench_week2/plans/*.txt          # EXPLAIN FORMATTED plans
```

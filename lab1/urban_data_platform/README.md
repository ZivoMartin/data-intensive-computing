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

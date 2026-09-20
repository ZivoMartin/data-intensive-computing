# Week 2 Benchmark Report

_Generated: 2026-09-20T20:23:46.638354+00:00_

Timing method: median of 3 runs after one warm-up run; each run fully materializes the query via a `noop` write.


## 1. Baseline analytical query execution time

| Query | Title | Median (s) | Min (s) |
|---|---|---|---|
| q1_monthly_demand_by_zone | Monthly taxi demand for each taxi zone | 1.396 | 1.364 |
| q2_avg_distance_by_weather | Average trip distance under different weather conditions | 0.916 | 0.875 |
| q3_air_quality_vs_demand | Relationship between air quality and taxi demand | 0.952 | 0.950 |
| q4_zone_demand_weather_variation | Taxi zones with the largest variation in demand across weather conditions | 1.451 | 1.439 |
| q5_peak_hours_by_weekday | Peak travel hours for each day of the week | 1.685 | 1.535 |
| q6_monthly_demand_trend | Monthly trends in taxi demand | 0.458 | 0.436 |

## 2. Data product storage overhead

| Product | Rows | Storage (bytes) | Schema version |
|---|---|---|---|
| air_quality_impact_summary | 2 | 5434 | 1.0 |
| borough_mobility_summary | 31 | 16715 | 1.0 |
| daily_mobility_summary | 95 | 21169 | 1.0 |
| taxi_zone_statistics | 261 | 19686 | 1.0 |
| weather_impact_summary | 4 | 6609 | 1.0 |

## 3. Materialization speedup (source query vs product read)

| Query | Product | Source query (s) | Product read (s) | Speedup |
|---|---|---|---|---|
| q2_avg_distance_by_weather | weather_impact_summary | 0.729 | 0.197 | 3.695x |
| q3_air_quality_vs_demand | air_quality_impact_summary | 0.868 | 0.192 | 4.514x |
| q6_monthly_demand_trend | daily_mobility_summary | 0.413 | 0.258 | 1.598x |

## 4. Optimization experiments

| Technique | Query | Baseline (s) | Optimized (s) | Speedup | Results identical |
|---|---|---|---|---|---|
| caching | q5_peak_hours_by_weekday | 1.476 | 1.281 | 1.152x | True |
| partition_pruning | monthly_zone_agg_same_scope | 0.437 | 0.241 | 1.816x | True |
| broadcast_join | trips_per_borough_join | 2.072 | 0.703 | 2.95x | True |
| aqe | q4_zone_demand_weather_variation | 1.381 | 1.315 | 1.05x | True |

EXPLAIN FORMATTED plans for each experiment are saved under `plans/`.


### Notes per experiment

- **caching** (q5_peak_hours_by_weekday): Cached integrated_taxi_trips in memory before re-running a full-scan query. Look for InMemoryTableScan / InMemoryRelation in the optimized plan. Caveat for the report: the baseline is preceded by a warm-up run, so the OS page cache is already hot and the baseline is NOT a cold-disk read. The measured gain therefore reflects skipping deserialization and column decoding, not disk I/O — expect it to be modest on a single node and larger on a cluster where the data does not fit in page cache.
- **partition_pruning** (monthly_zone_agg_same_scope): Both queries return the same month (2003-01). Baseline filters on the data column pickup_date (no pruning); optimized filters on the partition columns pickup_year/pickup_month. In the plans, compare the FileScan node's PartitionFilters entry and the 'number of files read' / 'size of files read' metrics.
- **broadcast_join** (trips_per_borough_join): Baseline forces SortMergeJoin (autoBroadcastJoinThreshold=-1); optimized uses a BROADCAST hint. Expect BroadcastHashJoin + BroadcastExchange in the optimized plan and no shuffle of the large fact table.
- **aqe** (q4_zone_demand_weather_variation): Same query with spark.sql.adaptive.enabled false vs true. With AQE the plan shows AdaptiveSparkPlan isFinalPlan=true and coalesced shuffle partitions; effect is largest on skewed / over-partitioned shuffles.

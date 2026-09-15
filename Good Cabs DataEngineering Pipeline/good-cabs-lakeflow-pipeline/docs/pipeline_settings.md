# Pipeline Settings — Continuous Mode & Incremental Loading

## Pipeline mode

In the Databricks Lakeflow pipeline settings, set:

```
Pipeline mode: Continuous
```

Rather than a one-shot batch run or a manual daily trigger, **Continuous** mode keeps
the pipeline running in the background and reacts to new data automatically.

## What happens when a new file lands

1. A new day's file (e.g. `trips_2025_12_27.csv`) is uploaded to the S3 landing path.
2. **Autoloader** (`cloudFiles`) in `bronze/trips_bronze.py` detects the new file via
   directory listing / file notifications and streams only the new rows into
   `bronze.trips` — it never re-reads files it has already ingested.
3. The **staging view** (`trips_staging` in `silver/trips_silver.py`) re-evaluates
   its data-quality expectations against just the new/changed rows.
4. The **Auto CDC flow** merges those validated rows into `silver.trips` using
   SCD Type 1 upsert semantics, keyed on `trip_id` and sequenced by
   `bronze_ingest_date_time` — so late-arriving or out-of-order records are
   still merged correctly.
5. `gold.fact_trips` and the city-scoped Gold views are streaming/materialized
   tables downstream of `silver.trips`, so they refresh incrementally too.

## Why this matters

- No manual "has anything changed?" checks or custom checkpoint management.
- Compute is only spent on the new/changed data, not full reprocessing.
- Regional dashboards reflect new trips within minutes of the file landing in S3,
  instead of waiting for a nightly batch job.

## Triggered mode (for local testing / cost control)

While developing or testing this repo before wiring up Continuous mode, you can run
the pipeline in **Triggered** mode instead — it processes whatever is new since the
last run and then stops, which is cheaper for iterating on pipeline code.

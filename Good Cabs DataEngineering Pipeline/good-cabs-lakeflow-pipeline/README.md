# Good Cabs — Transportation Analytics Pipeline
### Databricks LakeFlow Spark Declarative Pipelines | Medallion Architecture (Bronze → Silver → Gold)

## Business Problem

**Good Cabs** is a cab service company operating across multiple cities/regions in India.
Regional managers previously had to work with delayed, generic dashboards and manually
export/rework data to get city-specific insights. This project builds an end-to-end,
automated data platform that gives every region fast, trustworthy, self-serve analytics.

## Why Declarative Pipelines?

Instead of hand-writing and manually orchestrating procedural Spark jobs, this project
uses **Databricks LakeFlow Spark Declarative Pipelines (SDP)** — you declare *what* the
data should look like, and the engine handles execution planning, dependency resolution,
orchestration, and incremental processing.

| | Imperative Spark | Declarative Pipelines (SDP) |
|---|---|---|
| Silver `trips` upsert logic | ~135 lines | ~50 lines |
| Orchestration | Manual job scheduling | Auto-generated DAG |
| Incremental loads | Custom checkpointing logic | Built-in (Autoloader + Auto CDC) |

## Architecture

```
                 Amazon S3 (raw CSVs: city.csv, trips_*.csv)
                                │
                                ▼
┌───────────────────────────────────────────────────────────┐
│  BRONZE — raw ingestion, metadata columns for lineage      │
│  • city    → Materialized View (batch read)                │
│  • trips   → Streaming Table via Autoloader (cloudFiles)   │
└──────────────────────────┬────────────────────────────────┘
                            ▼
┌───────────────────────────────────────────────────────────┐
│  SILVER — cleaned, validated, dimensionally modeled        │
│  • city      → renamed/typed Materialized View             │
│  • calendar  → generated date dimension (year/month/       │
│                quarter/week/weekday/holiday flags)         │
│  • trips     → data-quality expectations + Auto CDC        │
│                (SCD Type 1 upsert on trip_id)               │
└──────────────────────────┬────────────────────────────────┘
                            ▼
┌───────────────────────────────────────────────────────────┐
│  GOLD — business-ready, analytics-optimized                │
│  • fact_trips              → denormalized join of          │
│                               trips + city + calendar        │
│  • fact_trips_<city>       → secure, city-scoped views      │
│                               for regional RBAC               │
└──────────────────────────┬────────────────────────────────┘
                            ▼
            Regional Dashboards / BI Tools / Genie AI Q&A
```

## Repo Structure

```
good-cabs-lakeflow-pipeline/
├── README.md
├── setup/
│   └── 01_project_setup.py        # catalog, schemas, S3 external location
├── bronze/
│   ├── city_bronze.py             # materialized view, batch read from S3
│   └── trips_bronze.py            # streaming table, Autoloader
├── silver/
│   ├── city_silver.py             # cleaned dimension
│   ├── calendar_silver.py         # generated date dimension
│   └── trips_silver.py            # DQ expectations + Auto CDC (SCD1)
├── gold/
│   └── fact_trips_gold.py         # unified fact table + city-scoped views
├── access_management/
│   └── rbac_setup.sql             # Unity Catalog group-based RBAC
└── docs/
    └── pipeline_settings.md       # continuous mode / incremental load notes
```

## Tech Stack

- **Databricks Free Edition** (serverless compute)
- **LakeFlow Spark Declarative Pipelines** (`pyspark.pipelines`, formerly Delta Live Tables)
- **Delta Lake** + **Unity Catalog** (governance, RBAC, lineage)
- **Amazon S3** as the raw landing zone, connected via an external location
- **Databricks Autoloader** (`cloudFiles`) for incremental file ingestion
- **Auto CDC** for SCD Type 1 upserts

## How to Reproduce

1. Sign up for [Databricks Free Edition](https://www.databricks.com/product/pricing).
2. Run `setup/01_project_setup.py` to create the catalog/schemas and register the S3
   external location (update the bucket path and IAM role ARN for your own AWS account).
3. Upload `city.csv` and daily `trips_*.csv` files to your S3 bucket.
4. Create a new **Lakeflow Declarative Pipeline** in Databricks, point it at the
   `bronze/`, `silver/`, and `gold/` folders (in that source order), and set the
   catalog/target schema you created in step 2.
5. Set the pipeline mode to **Continuous** (see `docs/pipeline_settings.md`) so new
   files dropped in S3 are picked up automatically.
6. Run `access_management/rbac_setup.sql` in a SQL editor to create regional groups
   and scope their access to only their city's Gold view.

## Key Concepts Demonstrated

- Declarative vs. imperative pipeline design
- Medallion Architecture (Bronze/Silver/Gold)
- Materialized views vs. streaming tables
- Data quality enforcement with `expect` constraints
- Change Data Capture with `create_auto_cdc_flow` (SCD Type 1)
- Incremental/continuous processing with Autoloader
- Unity Catalog role-based access control (RBAC)

## Disclaimer

This is a learning/portfolio project built by following a public data engineering
tutorial on Databricks LakeFlow Declarative Pipelines. Table/column names and sample
scenario (a fictional cab company, "Good Cabs") are illustrative. No proprietary data
or code from the original source is copied — all pipeline code here was written from
scratch based on the documented pattern.

# Databricks Lakeflow Declarative Pipeline — Silver Layer
# Tables: silver.trips_staging (view), silver.trips (streaming table)
#
# Two-step pattern:
#  1. A staging VIEW applies data-quality expectations. Invalid rows are
#     logged/dropped rather than failing the whole pipeline.
#  2. An Auto CDC flow merges the staging view into the final streaming
#     table using SCD Type 1 (upsert) semantics, keyed on trip_id.
#
# Compared to hand-written imperative upsert logic (~135 lines with manual
# merge conditions, watermarking, and out-of-order handling), the Auto CDC
# flow below achieves the same result in a fraction of the code.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Step 1: Staging view with data quality expectations
# ---------------------------------------------------------------------------


@dp.view(
    name="trips_staging",
    comment="Validated view of bronze trips, ready for CDC merge into silver.",
)
@dp.expect_all_or_drop(
    {
        "valid_driver_rating": "driver_rating BETWEEN 1 AND 10",
        "valid_passenger_rating": "passenger_rating BETWEEN 1 AND 10",
        "valid_trip_date": "trip_date > '2020-01-01'",
    }
)
def trips_staging():
    return (
        spark.readStream.table("bronze.trips")
        .withColumnRenamed("ingest_date_time", "bronze_ingest_date_time")
        .withColumn("silver_process_date_time", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Step 2: Silver streaming table, target of the Auto CDC flow
# ---------------------------------------------------------------------------

dp.create_streaming_table(
    name="trips",
    comment="Deduplicated, upserted trip records (SCD Type 1) in the silver layer.",
)

dp.create_auto_cdc_flow(
    target="trips",
    source="trips_staging",
    keys=["trip_id"],
    sequence_by=F.col("bronze_ingest_date_time"),
    stored_as_scd_type=1,
)

# Databricks Lakeflow Declarative Pipeline — Silver Layer
# Table: silver.city
#
# Cleans up column names inherited from Bronze and stamps a silver-layer
# processing timestamp, distinct from the bronze ingest_date_time.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver.city",
    comment="Cleaned city dimension, renamed and timestamped for the silver layer.",
)
def city_silver():
    return (
        spark.read.table("bronze.city")
        .withColumnRenamed("ingest_date_time", "bronze_ingest_date_time")
        .withColumn("silver_process_date_time", F.current_timestamp())
        .select(
            "city_id",
            "city_name",
            "state",
            "bronze_ingest_date_time",
            "silver_process_date_time",
        )
    )

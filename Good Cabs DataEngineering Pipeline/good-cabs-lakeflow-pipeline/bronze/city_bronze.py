# Databricks Lakeflow Declarative Pipeline — Bronze Layer
# Table: bronze.city
#
# The city dimension is small and rarely changes, so it's modeled as a
# Materialized View: a full batch re-read of the source CSV on every
# pipeline run, rather than an incrementally-processed streaming table.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

S3_CITY_PATH = "s3://good-cabs-raw-data/city/"


@dp.materialized_view(
    name="bronze.city",
    comment="Raw city dimension data ingested from S3, with lineage metadata.",
)
def city_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(S3_CITY_PATH)
        .withColumn("file_name", F.input_file_name())
        .withColumn("ingest_date_time", F.current_timestamp())
    )

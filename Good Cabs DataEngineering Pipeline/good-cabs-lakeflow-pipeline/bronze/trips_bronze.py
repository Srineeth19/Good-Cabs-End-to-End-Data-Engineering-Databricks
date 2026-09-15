# Databricks Lakeflow Declarative Pipeline — Bronze Layer
# Table: bronze.trips
#
# Trips data arrives as daily CSV drops in S3 (trips_YYYY_MM_DD.csv). Modeled
# as a Streaming Table backed by Databricks Autoloader so new files are picked
# up incrementally — the pipeline never re-reads files it has already processed.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

S3_TRIPS_PATH = "s3://good-cabs-raw-data/trips/"
SCHEMA_LOCATION = "s3://good-cabs-raw-data/_schemas/trips/"


@dp.table(
    name="bronze.trips",
    comment="Raw daily trip records ingested incrementally from S3 via Autoloader.",
)
def trips_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .load(S3_TRIPS_PATH)
        .withColumn("file_name", F.input_file_name())
        .withColumn("ingest_date_time", F.current_timestamp())
    )

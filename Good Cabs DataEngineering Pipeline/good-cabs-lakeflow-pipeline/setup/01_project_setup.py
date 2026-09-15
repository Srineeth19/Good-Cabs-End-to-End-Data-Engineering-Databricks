# Databricks notebook source
# MAGIC %md
# MAGIC # Project Setup — Catalog, Schemas, and S3 External Location
# MAGIC
# MAGIC Run this once (as a notebook, in Databricks Free Edition) before creating the
# MAGIC Lakeflow Declarative Pipeline. It creates:
# MAGIC 1. A Unity Catalog catalog for the project
# MAGIC 2. Bronze / Silver / Gold schemas
# MAGIC 3. An external location pointing at your S3 bucket (raw landing zone)
# MAGIC
# MAGIC Update `CATALOG_NAME`, `S3_BUCKET_PATH`, and `STORAGE_CREDENTIAL_NAME` for your
# MAGIC own AWS account before running.

# COMMAND ----------

CATALOG_NAME = "good_cabs"
S3_BUCKET_PATH = "s3://good-cabs-raw-data/"          # <-- replace with your bucket
STORAGE_CREDENTIAL_NAME = "good_cabs_storage_cred"    # <-- must already exist / IAM role configured
EXTERNAL_LOCATION_NAME = "good_cabs_s3_landing"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${CATALOG_NAME};

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")

# COMMAND ----------

# Bronze / Silver / Gold schemas, following the Medallion Architecture
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}")
    print(f"Created schema: {CATALOG_NAME}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the S3 bucket as a Unity Catalog external location
# MAGIC
# MAGIC This assumes a storage credential (IAM role with S3 read access) has already
# MAGIC been created in Databricks under **Catalog > External Data > Credentials**.

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS {EXTERNAL_LOCATION_NAME}
URL '{S3_BUCKET_PATH}'
WITH (STORAGE CREDENTIAL {STORAGE_CREDENTIAL_NAME})
""")

# COMMAND ----------

# Sanity check: list files landing in the raw bucket
display(dbutils.fs.ls(S3_BUCKET_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 1. Upload `city.csv` and daily `trips_*.csv` files into `S3_BUCKET_PATH`.
# MAGIC 2. Create a new **Lakeflow Declarative Pipeline** in Databricks:
# MAGIC    - Source code: the `bronze/`, `silver/`, and `gold/` folders of this repo
# MAGIC    - Catalog: `good_cabs`
# MAGIC    - Target schema: leave per-file schema overrides as defined in the code
# MAGIC    - Pipeline mode: Triggered (for testing) or Continuous (see docs/pipeline_settings.md)
# MAGIC 3. Run the pipeline.

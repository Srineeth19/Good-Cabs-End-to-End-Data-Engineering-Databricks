# Databricks Lakeflow Declarative Pipeline — Silver Layer
# Table: silver.calendar
#
# A programmatically generated date dimension (no source file). Derives the
# usual BI attributes (year, month, quarter, week, weekday) and flags a small
# set of fixed Indian national holidays for time-series analysis.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

START_DATE = "2025-01-01"
END_DATE = "2025-12-31"

# Fixed-date Indian national holidays (illustrative subset)
NATIONAL_HOLIDAYS = [
    "2025-01-26",  # Republic Day
    "2025-08-15",  # Independence Day
    "2025-10-02",  # Gandhi Jayanti
]


@dp.materialized_view(
    name="silver.calendar",
    comment="Generated date dimension with calendar attributes and national holiday flags.",
)
def calendar_silver():
    df = spark.sql(
        f"""
        SELECT explode(sequence(
            to_date('{START_DATE}'),
            to_date('{END_DATE}'),
            interval 1 day
        )) AS date
        """
    )

    return (
        df.withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("weekday_name", F.date_format("date", "EEEE"))
        .withColumn(
            "is_weekend", F.dayofweek("date").isin([1, 7])  # Sunday=1, Saturday=7
        )
        .withColumn(
            "is_national_holiday",
            F.col("date").cast("string").isin(NATIONAL_HOLIDAYS),
        )
    )

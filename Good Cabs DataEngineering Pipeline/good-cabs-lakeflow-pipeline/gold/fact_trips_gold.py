# Databricks Lakeflow Declarative Pipeline — Gold Layer
# Tables: gold.fact_trips + one secure view per city (e.g. gold.fact_trips_vadodara)
#
# Denormalizes silver.trips with the city and calendar dimensions into a
# single BI-ready fact table, then produces per-city views so regional
# managers can be scoped (via Unity Catalog RBAC) to only their own data.

from pyspark import pipelines as dp

# Map of city_id -> a short, filesystem/SQL-safe suffix used for the view name.
# Extend this as new cities/regions come online.
CITY_VIEWS = {
    "GJ02": "vadodara",
    "MH01": "mumbai",
    "KA01": "bengaluru",
}


@dp.table(
    name="fact_trips",
    comment="Unified, denormalized trip fact table joining trips, city, and calendar.",
)
def fact_trips():
    trips = spark.read.table("silver.trips")
    city = spark.read.table("silver.city")
    calendar = spark.read.table("silver.calendar")

    return (
        trips.join(city, on="city_id", how="left")
        .join(calendar, trips.trip_date == calendar.date, how="left")
        .select(
            trips["trip_id"],
            trips["city_id"],
            city["city_name"],
            city["state"],
            trips["trip_date"],
            calendar["year"],
            calendar["month_name"],
            calendar["quarter"],
            calendar["weekday_name"],
            calendar["is_weekend"],
            calendar["is_national_holiday"],
            trips["fare_amount"],
            trips["distance_travelled_km"],
            trips["driver_rating"],
            trips["passenger_rating"],
        )
    )


def _make_city_view(city_id: str, suffix: str):
    """Factory that registers a secure, city-scoped Gold view."""

    @dp.table(
        name=f"fact_trips_{suffix}",
        comment=f"Secure regional view of fact_trips, scoped to city_id = '{city_id}'.",
    )
    def _city_view():
        return spark.read.table("fact_trips").filter(f"city_id = '{city_id}'")

    return _city_view


# Register one city-scoped Gold view per entry in CITY_VIEWS
for _city_id, _suffix in CITY_VIEWS.items():
    _make_city_view(_city_id, _suffix)

# Sample Data

Tiny illustrative CSVs matching the schema expected by the bronze layer, so you
can smoke-test the pipeline logic before wiring up a real S3 bucket. Upload
these (or your own larger dataset in the same shape) to your S3 landing path.

- `city.csv` — city dimension (city_id, city_name, state)
- `trips_2025_01_01.csv` — one day of trip records; add more `trips_YYYY_MM_DD.csv`
  files to simulate daily incremental drops for Autoloader to pick up.

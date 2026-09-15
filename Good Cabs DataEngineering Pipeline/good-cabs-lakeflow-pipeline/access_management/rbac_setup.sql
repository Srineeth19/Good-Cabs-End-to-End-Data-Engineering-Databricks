-- Access Management: Unity Catalog Role-Based Access Control
-- Run this after the Gold layer tables/views have been created by the pipeline.
--
-- Pattern: create one Unity Catalog group per region, grant that group SELECT
-- only on its own city-scoped Gold view, and explicitly ensure no access to
-- Bronze/Silver (regional managers should never see raw or intermediate data).

-- 1) Identity management: create a group per region
CREATE GROUP IF NOT EXISTS vadodara_team;
CREATE GROUP IF NOT EXISTS mumbai_team;
CREATE GROUP IF NOT EXISTS bengaluru_team;

-- 2) Add users to their regional group (repeat per user)
-- ALTER GROUP vadodara_team ADD USER 'manager1@goodcabs.com';
-- ALTER GROUP mumbai_team   ADD USER 'manager2@goodcabs.com';

-- 3) Scoped privilege grants: each group can only query its own Gold view
GRANT USE CATALOG  ON CATALOG good_cabs             TO vadodara_team;
GRANT USE SCHEMA   ON SCHEMA  good_cabs.gold        TO vadodara_team;
GRANT SELECT       ON TABLE   good_cabs.gold.fact_trips_vadodara TO vadodara_team;

GRANT USE CATALOG  ON CATALOG good_cabs             TO mumbai_team;
GRANT USE SCHEMA   ON SCHEMA  good_cabs.gold        TO mumbai_team;
GRANT SELECT       ON TABLE   good_cabs.gold.fact_trips_mumbai TO mumbai_team;

GRANT USE CATALOG  ON CATALOG good_cabs             TO bengaluru_team;
GRANT USE SCHEMA   ON SCHEMA  good_cabs.gold        TO bengaluru_team;
GRANT SELECT       ON TABLE   good_cabs.gold.fact_trips_bengaluru TO bengaluru_team;

-- 4) No grants are issued on bronze/silver schemas or on gold.fact_trips (the
--    unified table) to any regional group — access is denied by default in
--    Unity Catalog unless explicitly granted, so regional teams are
--    automatically restricted to their own city view.

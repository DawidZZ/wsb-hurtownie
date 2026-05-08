-- ============================================================
-- RESET SCHEMATU – usuwa wszystkie tabele w kolejności FK
-- Uruchom przed ponownym wykonaniem noaa_dw_schema.sql
-- ============================================================

-- 1. Fakty (mają FK-i do wymiarów)
DROP TABLE IF EXISTS dbo.fact_event;

-- 2. Wymiary
DROP TABLE IF EXISTS dbo.dim_event_type;
DROP TABLE IF EXISTS dbo.dim_flood_cause;
DROP TABLE IF EXISTS dbo.dim_location;
DROP TABLE IF EXISTS dbo.dim_magnitude_type;
DROP TABLE IF EXISTS dbo.dim_source;
DROP TABLE IF EXISTS dbo.dim_time;
DROP TABLE IF EXISTS dbo.dim_wfo;

-- 3. Staging (bez FK-ów, kolejność dowolna)
DROP TABLE IF EXISTS dbo.stg_dim_event_type;
DROP TABLE IF EXISTS dbo.stg_dim_flood_cause;
DROP TABLE IF EXISTS dbo.stg_dim_location;
DROP TABLE IF EXISTS dbo.stg_dim_magnitude_type;
DROP TABLE IF EXISTS dbo.stg_dim_source;
DROP TABLE IF EXISTS dbo.stg_dim_time;
DROP TABLE IF EXISTS dbo.stg_dim_wfo;
DROP TABLE IF EXISTS dbo.stg_fact_event;

-- 4. Log ETL
DROP TABLE IF EXISTS dbo.etl_delta_log;

-- Stara tabela stagingowa (jeśli istniała z poprzedniej wersji projektu)
DROP TABLE IF EXISTS dbo.stg_storm_events;

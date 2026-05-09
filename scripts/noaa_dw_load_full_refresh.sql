-- ============================================================
-- PEŁNY REFRESH HURTOWNI DANYCH NOAA
-- Resetuje wszystkie tabele docelowe i ładuje od zera
-- Szybszy niż delta-load — brak NOT EXISTS checks
-- ============================================================


-- Tabele docelowe są czyszczone przez Python (truncate_all_final_tables)
-- przed wywołaniem tego skryptu — TRUNCATE/DELETE i RESEED jest już wykonany.


-- ============================================================
-- 1. dim_time  (tally CTE, 2009–2030)
-- ============================================================

DECLARE @StartDate  DATE = '20090101';
DECLARE @CutoffDate DATE = '20301231';

;WITH
E1(N) AS (SELECT 1 FROM (VALUES(1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) t(N)),
E2(N) AS (SELECT 1 FROM E1 a, E1 b),
E4(N) AS (SELECT 1 FROM E2 a, E2 b),
seq(n) AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 FROM E4),
d(d) AS (
    SELECT DATEADD(DAY, n, @StartDate)
    FROM seq
    WHERE n <= DATEDIFF(DAY, @StartDate, @CutoffDate)
),
src AS (
    SELECT
        TheDate      = CONVERT(DATE, d),
        TheDay       = DATEPART(DAY,      d),
        TheDayName   = DATENAME(WEEKDAY,  d),
        TheDayOfWeek = DATEPART(WEEKDAY,  d),
        TheISOWeek   = DATEPART(ISO_WEEK, d),
        TheMonth     = DATEPART(MONTH,    d),
        TheMonthName = DATENAME(MONTH,    d),
        TheQuarter   = DATEPART(QUARTER,  d),
        TheYear      = DATEPART(YEAR,     d)
    FROM d
)
INSERT INTO [dbo].[dim_time] (
    full_date, year, quarter, month, month_name,
    week_of_year, year_week, day, day_of_week, day_name, is_weekend, season
)
SELECT
    TheDate, TheYear, TheQuarter, TheMonth, TheMonthName,
    TheISOWeek,
    CAST(CAST(TheYear AS VARCHAR(4)) + RIGHT('0' + CAST(TheISOWeek AS VARCHAR(2)), 2) AS INT),
    TheDay, TheDayOfWeek, TheDayName,
    CASE WHEN TheDayOfWeek IN (1, 7) THEN 1 ELSE 0 END,
    CASE
        WHEN TheMonth IN (12, 1, 2) THEN 'Winter'
        WHEN TheMonth IN (3, 4, 5)  THEN 'Spring'
        WHEN TheMonth IN (6, 7, 8)  THEN 'Summer'
        ELSE 'Fall'
    END
FROM src;
GO


-- ============================================================
-- 2. dim_source – bez delta-check, prosty DISTINCT INSERT
-- ============================================================

INSERT INTO [dbo].[dim_source] (source_name)
SELECT DISTINCT source_name
FROM [dbo].[stg_dim_source]
WHERE source_name IS NOT NULL AND source_name <> '';
GO


-- ============================================================
-- 3. dim_event_type
-- ============================================================

INSERT INTO [dbo].[dim_event_type] (event_type, category)
SELECT DISTINCT event_type,
    CASE
        WHEN event_type LIKE '%Tornado%'   OR event_type LIKE '%Thunderstorm%'
          OR event_type LIKE '%Hail%'      OR event_type LIKE '%Lightning%'
          OR event_type LIKE '%Funnel%'    THEN 'Convective'
        WHEN event_type LIKE '%Flood%'     OR event_type LIKE '%Rain%'
          OR event_type LIKE '%Water%'     THEN 'Hydrological'
        WHEN event_type LIKE '%Snow%'      OR event_type LIKE '%Ice%'
          OR event_type LIKE '%Blizzard%'  OR event_type LIKE '%Winter%'
          OR event_type LIKE '%Sleet%'     OR event_type LIKE '%Freeze%'
          OR event_type LIKE '%Frost%'     THEN 'Winter'
        WHEN event_type LIKE '%Hurricane%' OR event_type LIKE '%Tropical%'
          OR event_type LIKE '%Typhoon%'   THEN 'Tropical'
        WHEN event_type LIKE '%Heat%'      OR event_type LIKE '%Drought%'
          OR event_type LIKE '%Fire%'      OR event_type LIKE '%Wildfire%' THEN 'Heat/Drought'
        ELSE 'Other'
    END
FROM [dbo].[stg_dim_event_type]
WHERE event_type IS NOT NULL;
GO


-- ============================================================
-- 4. dim_flood_cause
-- ============================================================

INSERT INTO [dbo].[dim_flood_cause] (flood_cause)
SELECT DISTINCT flood_cause
FROM [dbo].[stg_dim_flood_cause]
WHERE flood_cause IS NOT NULL AND flood_cause <> '';
GO


-- ============================================================
-- 5. dim_wfo
-- ============================================================

INSERT INTO [dbo].[dim_wfo] (wfo_code)
SELECT DISTINCT wfo_code
FROM [dbo].[stg_dim_wfo]
WHERE wfo_code IS NOT NULL AND wfo_code <> '';
GO


-- ============================================================
-- 6. dim_magnitude_type
-- ============================================================

INSERT INTO [dbo].[dim_magnitude_type] (magnitude_type)
SELECT DISTINCT magnitude_type
FROM [dbo].[stg_dim_magnitude_type]
WHERE magnitude_type IS NOT NULL AND magnitude_type <> '';
GO


-- ============================================================
-- 7. dim_location – wszystkie lokalizacje, is_current = 1
-- ============================================================

INSERT INTO [dbo].[dim_location] (
    state_fips, cz_fips, state_abbr, state_name, cz_name, cz_type, time_zone,
    is_current, valid_from, valid_to
)
SELECT DISTINCT
    stg.state_fips, stg.cz_fips, UPPER(stg.state_abbr),
    stg.state_name, stg.cz_name, stg.cz_type, stg.time_zone,
    1, GETDATE(), '9999-12-31'
FROM [dbo].[stg_dim_location] AS stg;
GO


-- ============================================================
-- 8. fact_event
--    8a: znormalizuj stringi ze stagingu → #stg_new
-- ============================================================

-- Indeks na staging dla przyspieszenia
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'ix_stg_fact_event_id' AND object_id = OBJECT_ID('[dbo].[stg_fact_event]')
)
BEGIN
    CREATE INDEX ix_stg_fact_event_id ON [dbo].[stg_fact_event] (event_id);
END

-- Deduplikacja event_id (ten sam event może wystąpić w wielu plikach CSV)
-- ROW_NUMBER() PARTITION BY event_id — wybieramy jedną wersję per event
;WITH ranked AS (
    SELECT
        event_id, episode_id, source_event_id,
        begin_date, end_date, begin_time, end_time, duration_minutes, magnitude,
        state_fips, cz_fips,
        LTRIM(RTRIM(source))                AS source,
        LTRIM(RTRIM(event_type))            AS event_type,
        LTRIM(RTRIM(flood_cause))           AS flood_cause,
        UPPER(LTRIM(RTRIM(wfo)))            AS wfo,
        UPPER(LTRIM(RTRIM(magnitude_type))) AS magnitude_type,
        injuries_direct, injuries_indirect,
        deaths_direct,   deaths_indirect,
        damage_property, damage_crops,
        population, population_density, source_load_date,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY source_load_date DESC) AS rn
    FROM [dbo].[stg_fact_event]
    WHERE event_id IS NOT NULL
)
SELECT
    event_id, episode_id, source_event_id,
    begin_date, end_date, begin_time, end_time, duration_minutes, magnitude,
    state_fips, cz_fips, source, event_type, flood_cause, wfo, magnitude_type,
    injuries_direct, injuries_indirect, deaths_direct, deaths_indirect,
    damage_property, damage_crops, population, population_density, source_load_date
INTO #stg_new
FROM ranked
WHERE rn = 1;

-- Indeksy na #stg_new dla JOINów z wymiarami
CREATE INDEX ix_sn_begin ON #stg_new (begin_date);
CREATE INDEX ix_sn_loc   ON #stg_new (state_fips, cz_fips);
CREATE INDEX ix_sn_src   ON #stg_new (source);
CREATE INDEX ix_sn_evt   ON #stg_new (event_type);

-- ============================================================
--    8b: rozwiąż klucze zastępcze i wstaw fakty
-- ============================================================

INSERT INTO [dbo].[fact_event] (
    episode_id, event_id, source_event_id,
    begin_time_id, end_time_id, location_id, source_id,
    event_type_id, flood_cause_id, wfo_id, magnitude_type_id,
    begin_time, end_time, duration_minutes, magnitude,
    injuries_direct, injuries_indirect, deaths_direct, deaths_indirect,
    damage_property, damage_crops, population, population_density, source_load_date
)
SELECT
    stg.episode_id, stg.event_id, stg.source_event_id,
    dt_begin.id, dt_end.id, loc.id, src.id, evt.id, fld.id, wfo.id, mgt.id,
    stg.begin_time, stg.end_time, stg.duration_minutes, stg.magnitude,
    stg.injuries_direct, stg.injuries_indirect, stg.deaths_direct, stg.deaths_indirect,
    stg.damage_property, stg.damage_crops, stg.population, stg.population_density,
    stg.source_load_date
FROM #stg_new AS stg
INNER JOIN [dbo].[dim_time]     AS dt_begin ON stg.begin_date     = dt_begin.full_date
LEFT  JOIN [dbo].[dim_time]     AS dt_end   ON stg.end_date       = dt_end.full_date
INNER JOIN [dbo].[dim_location] AS loc
       ON stg.state_fips = loc.state_fips
      AND stg.cz_fips    = loc.cz_fips
      AND loc.is_current = 1
LEFT  JOIN [dbo].[dim_source]         AS src ON stg.source         = src.source_name
LEFT  JOIN [dbo].[dim_event_type]     AS evt ON stg.event_type     = evt.event_type
LEFT  JOIN [dbo].[dim_flood_cause]    AS fld ON stg.flood_cause    = fld.flood_cause
LEFT  JOIN [dbo].[dim_wfo]            AS wfo ON stg.wfo            = wfo.wfo_code
LEFT  JOIN [dbo].[dim_magnitude_type] AS mgt ON stg.magnitude_type = mgt.magnitude_type;

DROP TABLE #stg_new;
GO


-- ============================================================
-- 9. Log delty ETL
-- ============================================================

INSERT INTO [dbo].[etl_delta_log] (source_files, rows_inserted, rows_rejected, status)
SELECT
    'stg_fact_event (FULL REFRESH)',
    COUNT(*),
    0,
    'SUCCESS'
FROM [dbo].[fact_event]
WHERE CAST(insertion_date AS DATE) = CAST(GETDATE() AS DATE);
GO

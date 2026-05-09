-- ============================================================
-- ŁADOWANIE HURTOWNI DANYCH NOAA
-- Kolejność: wymiary najpierw, potem fakty
-- ============================================================


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
FROM src
WHERE NOT EXISTS (SELECT 1 FROM [dbo].[dim_time] dt WHERE dt.full_date = TheDate);
GO


-- ============================================================
-- 2. dim_source
-- ============================================================

INSERT INTO [dbo].[dim_source] (source_name)
SELECT DISTINCT stg.source_name
FROM [dbo].[stg_dim_source] AS stg
WHERE stg.source_name IS NOT NULL AND stg.source_name <> ''
    AND NOT EXISTS (SELECT 1 FROM [dbo].[dim_source] d WHERE d.source_name = stg.source_name);
GO


-- ============================================================
-- 3. dim_event_type
-- ============================================================

INSERT INTO [dbo].[dim_event_type] (event_type, category)
SELECT DISTINCT stg.event_type,
    CASE
        WHEN stg.event_type LIKE '%Tornado%'   OR stg.event_type LIKE '%Thunderstorm%'
          OR stg.event_type LIKE '%Hail%'      OR stg.event_type LIKE '%Lightning%'
          OR stg.event_type LIKE '%Funnel%'    THEN 'Convective'
        WHEN stg.event_type LIKE '%Flood%'     OR stg.event_type LIKE '%Rain%'
          OR stg.event_type LIKE '%Water%'     THEN 'Hydrological'
        WHEN stg.event_type LIKE '%Snow%'      OR stg.event_type LIKE '%Ice%'
          OR stg.event_type LIKE '%Blizzard%'  OR stg.event_type LIKE '%Winter%'
          OR stg.event_type LIKE '%Sleet%'     OR stg.event_type LIKE '%Freeze%'
          OR stg.event_type LIKE '%Frost%'     THEN 'Winter'
        WHEN stg.event_type LIKE '%Hurricane%' OR stg.event_type LIKE '%Tropical%'
          OR stg.event_type LIKE '%Typhoon%'   THEN 'Tropical'
        WHEN stg.event_type LIKE '%Heat%'      OR stg.event_type LIKE '%Drought%'
          OR stg.event_type LIKE '%Fire%'      OR stg.event_type LIKE '%Wildfire%' THEN 'Heat/Drought'
        ELSE 'Other'
    END
FROM [dbo].[stg_dim_event_type] AS stg
WHERE stg.event_type IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM [dbo].[dim_event_type] d WHERE d.event_type = stg.event_type);
GO


-- ============================================================
-- 4. dim_flood_cause
-- ============================================================

INSERT INTO [dbo].[dim_flood_cause] (flood_cause)
SELECT DISTINCT stg.flood_cause
FROM [dbo].[stg_dim_flood_cause] AS stg
WHERE stg.flood_cause IS NOT NULL AND stg.flood_cause <> ''
    AND NOT EXISTS (SELECT 1 FROM [dbo].[dim_flood_cause] d WHERE d.flood_cause = stg.flood_cause);
GO


-- ============================================================
-- 5. dim_wfo
-- ============================================================

INSERT INTO [dbo].[dim_wfo] (wfo_code)
SELECT DISTINCT stg.wfo_code
FROM [dbo].[stg_dim_wfo] AS stg
WHERE stg.wfo_code IS NOT NULL AND stg.wfo_code <> ''
    AND NOT EXISTS (SELECT 1 FROM [dbo].[dim_wfo] d WHERE d.wfo_code = stg.wfo_code);
GO


-- ============================================================
-- 6. dim_magnitude_type
-- ============================================================

INSERT INTO [dbo].[dim_magnitude_type] (magnitude_type)
SELECT DISTINCT stg.magnitude_type
FROM [dbo].[stg_dim_magnitude_type] AS stg
WHERE stg.magnitude_type IS NOT NULL AND stg.magnitude_type <> ''
    AND NOT EXISTS (SELECT 1 FROM [dbo].[dim_magnitude_type] d WHERE d.magnitude_type = stg.magnitude_type);
GO


-- ============================================================
-- 7. dim_location  (SCD Type 2 — tylko nowe kombinacje)
-- ============================================================

INSERT INTO [dbo].[dim_location] (
    state_fips, cz_fips, state_abbr, state_name, cz_name, cz_type, time_zone,
    is_current, valid_from, valid_to
)
SELECT DISTINCT
    stg.state_fips, stg.cz_fips, UPPER(stg.state_abbr),
    stg.state_name, stg.cz_name, stg.cz_type, stg.time_zone,
    1, GETDATE(), '9999-12-31'
FROM [dbo].[stg_dim_location] AS stg
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[dim_location] AS loc
    WHERE loc.state_fips = stg.state_fips
      AND loc.cz_fips    = stg.cz_fips
      AND loc.is_current = 1
);
GO


-- ============================================================
-- 8. fact_event
--    8a: wyfiltruj NOWE wiersze + znormalizuj stringi → #stg_new
--        (NOT EXISTS PRZED joinami — nie robimy pracy dla już załadowanych)
-- ============================================================

-- Indeks na staging dla przyspieszenia NOT EXISTS w SELECT INTO
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'ix_stg_fact_event_id' AND object_id = OBJECT_ID('[dbo].[stg_fact_event]')
)
BEGIN
    CREATE INDEX ix_stg_fact_event_id ON [dbo].[stg_fact_event] (event_id);
END

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
    population, population_density, source_load_date
INTO #stg_new
FROM [dbo].[stg_fact_event] AS stg
WHERE stg.event_id IS NOT NULL
  AND NOT EXISTS (
        SELECT 1 FROM [dbo].[fact_event] fe WHERE fe.event_id = stg.event_id
  );

-- Indeksy na #stg_new przyspieszają JOIN-y z wymiarami
CREATE INDEX ix_sn_begin ON #stg_new (begin_date);
CREATE INDEX ix_sn_loc   ON #stg_new (state_fips, cz_fips);
CREATE INDEX ix_sn_src   ON #stg_new (source);
CREATE INDEX ix_sn_evt   ON #stg_new (event_type);

-- ============================================================
--    8b: rozwiąż klucze zastępcze i wstaw fakty
--        (czyste joiny na znormalizowanych kolumnach → index seek)
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
    'stg_fact_event',
    COUNT(*),
    (SELECT COUNT(*) FROM [dbo].[stg_fact_event]
     WHERE event_id IS NULL OR event_id NOT IN (SELECT event_id FROM [dbo].[fact_event])),
    'SUCCESS'
FROM [dbo].[fact_event]
WHERE CAST(insertion_date AS DATE) = CAST(GETDATE() AS DATE);
GO

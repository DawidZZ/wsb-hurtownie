-- ============================================================
-- RAPORTY ANALITYCZNE – NOAA Storm Events DWH
-- Uruchamiaj po załadowaniu danych przez noaa_dw_load.sql
-- ============================================================


-- ============================================================
-- RAPORT 1: Top 10 stanów wg łącznych strat materialnych
-- ============================================================
-- Co pokazuje: które stany poniosły największe straty
--   w mieniu i uprawach łącznie, ile zdarzeń je spowodowało.
-- Agregacja: SUM po damage_property + damage_crops,
--            COUNT zdarzeń, GROUP BY stan.
-- Wymiary: dim_location (stan), fact_event (miary strat).
-- ============================================================

SELECT TOP 10
    loc.state_abbr                                      AS stan,
    loc.state_name                                      AS nazwa_stanu,
    COUNT(*)                                            AS liczba_zdarzen,
    SUM(f.damage_property)                              AS straty_mienie_usd,
    SUM(f.damage_crops)                                 AS straty_uprawy_usd,
    SUM(f.damage_property + f.damage_crops)             AS straty_lacznie_usd
FROM [dbo].[fact_event]        AS f
JOIN [dbo].[dim_location]      AS loc ON f.location_id  = loc.id
GROUP BY loc.state_abbr, loc.state_name
ORDER BY straty_lacznie_usd DESC;


-- ============================================================
-- RAPORT 2: Liczba ofiar i rannych wg kategorii zdarzenia i roku
-- ============================================================
-- Co pokazuje: jak zmieniała się liczba ofiar i rannych
--   w czasie, z podziałem na kategorię zjawiska pogodowego
--   (np. Convective, Tropical, Winter).
-- Agregacja: SUM po deaths_total i injuries_total,
--            GROUP BY rok + kategoria.
-- Wymiary: dim_time (rok), dim_event_type (kategoria).
-- Kolumny obliczane: deaths_total i injuries_total są
--   wyliczane automatycznie przez SQL Server (direct+indirect).
-- ============================================================

SELECT
    t.year                                              AS rok,
    et.category                                         AS kategoria,
    SUM(f.deaths_total)                                 AS ofiary_smiertelne,
    SUM(f.injuries_total)                               AS ranni,
    SUM(f.deaths_total + f.injuries_total)              AS poszkodowani_lacznie
FROM [dbo].[fact_event]        AS f
JOIN [dbo].[dim_time]          AS t  ON f.begin_time_id = t.id
JOIN [dbo].[dim_event_type]    AS et ON f.event_type_id = et.id
WHERE t.year IS NOT NULL
GROUP BY t.year, et.category
ORDER BY t.year ASC, ofiary_smiertelne DESC;


-- ============================================================
-- RAPORT 3: Sezonowość zdarzeń – liczba i średni czas trwania
-- ============================================================
-- Co pokazuje: w których sezonach roku najczęściej dochodzi
--   do zdarzeń pogodowych i jak długo średnio trwają.
--   Sezon jest obliczany z daty w dim_time (Winter/Spring/
--   Summer/Fall), nie ze źródła NOAA.
-- Agregacja: COUNT zdarzeń, AVG czasu trwania,
--            GROUP BY sezon + typ zdarzenia.
-- Wymiary: dim_time (sezon), dim_event_type (typ).
-- ============================================================

SELECT
    t.season                                            AS sezon,
    et.event_type                                       AS typ_zdarzenia,
    COUNT(*)                                            AS liczba_zdarzen,
    AVG(CAST(f.duration_minutes AS FLOAT)) / 60.0       AS sredni_czas_h,
    SUM(f.damage_property + f.damage_crops)             AS straty_usd
FROM [dbo].[fact_event]        AS f
JOIN [dbo].[dim_time]          AS t  ON f.begin_time_id = t.id
JOIN [dbo].[dim_event_type]    AS et ON f.event_type_id = et.id
WHERE f.duration_minutes IS NOT NULL
GROUP BY t.season, et.event_type
ORDER BY t.season ASC, liczba_zdarzen DESC;


-- ============================================================
-- DELTA – weryfikacja poprawności przyrostowego ładowania
-- ============================================================
-- Co pokazuje: historię uruchomień ETL.
-- Jeśli uruchomisz noaa_dw_load.sql dwa razy z tymi samymi
-- danymi, drugie uruchomienie pokaże rows_inserted = 0.
-- To dowód że delta działa – system nie wstawia duplikatów.
-- ============================================================

SELECT
    run_date                                            AS data_uruchomienia,
    source_files                                        AS zrodlo,
    rows_inserted                                       AS nowe_rekordy,
    rows_rejected                                       AS odrzucone,
    status                                              AS status
FROM [dbo].[etl_delta_log]
ORDER BY run_date DESC;

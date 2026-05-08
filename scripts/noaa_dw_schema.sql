-- ============================================================
-- HURTOWNIA DANYCH: NOAA Storm Events
-- ============================================================


-- ============================================================
-- 1. WYMIARY
-- ============================================================

-- Wymiar czasu (rozszerzony)
CREATE TABLE dim_time (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    full_date       DATE         NOT NULL,
    year            INT          NOT NULL,
    quarter         INT          NOT NULL,           -- 1–4
    month           INT          NOT NULL,           -- 1–12
    month_name      VARCHAR(20)  NOT NULL,
    week_of_year    INT          NOT NULL,           -- 1–53 (ISO)
    year_week       INT          NOT NULL,           -- np. 202415 – bezpieczne porównania między latami
    day             INT          NOT NULL,           -- 1–31
    day_of_week     INT          NOT NULL,           -- 1=Pon … 7=Nie (ISO)
    day_name        VARCHAR(20)  NOT NULL,
    is_weekend      BIT          NOT NULL DEFAULT 0,
    season          VARCHAR(10)  NOT NULL,           -- 'Winter','Spring','Summer','Fall'
    CONSTRAINT uq_dim_time_date UNIQUE (full_date)
);

-- Wymiar lokalizacji z SCD Type 2
CREATE TABLE dim_location (
    id              INT IDENTITY(1,1) PRIMARY KEY,   -- klucz zastępczy (surrogate key)
    state_fips      INT          NOT NULL,
    cz_fips         INT          NOT NULL,
    state_abbr      CHAR(2)      NOT NULL,           -- np. 'TX', 'FL'
    state_name      VARCHAR(100) NOT NULL,
    cz_name         VARCHAR(255),
    cz_type         CHAR(1),                         -- 'C'=county, 'Z'=zone (NOAA)
    time_zone       VARCHAR(50),                     -- np. 'CST-6', 'EST-5'
    -- SCD Type 2
    is_current      BIT          NOT NULL DEFAULT 1,
    valid_from      DATETIME     NOT NULL DEFAULT GETDATE(),
    valid_to        DATETIME     NOT NULL DEFAULT '9999-12-31',
    CONSTRAINT uq_dim_location_scd UNIQUE (state_fips, cz_fips, valid_from)
);

-- Wymiar źródła danych
CREATE TABLE dim_source (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    source_name     VARCHAR(255) NOT NULL,
    CONSTRAINT uq_dim_source_name UNIQUE (source_name)
);

-- Wymiar typu zdarzenia (rozszerzony o kategorię NOAA)
CREATE TABLE dim_event_type (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    event_type      VARCHAR(100) NOT NULL,
    category        VARCHAR(50),                     -- np. 'Convective','Hydrological','Winter','Tropical'
    CONSTRAINT uq_dim_event_type UNIQUE (event_type)
);

-- Wymiar przyczyny powodzi
CREATE TABLE dim_flood_cause (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    flood_cause     VARCHAR(255) NOT NULL,
    CONSTRAINT uq_dim_flood_cause UNIQUE (flood_cause)
);

-- Wymiar urzędu WFO (Weather Forecast Office)
CREATE TABLE dim_wfo (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    wfo_code        VARCHAR(10)  NOT NULL,
    CONSTRAINT uq_dim_wfo_code UNIQUE (wfo_code)
);

-- Wymiar skali natężenia (Fujita, Saffir-Simpson, itp.)
CREATE TABLE dim_magnitude_type (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    magnitude_type  VARCHAR(10)  NOT NULL,           -- np. 'EF','MS','MPH'
    CONSTRAINT uq_dim_magnitude_type UNIQUE (magnitude_type)
);

-- ============================================================
-- 2. TABELA FAKTÓW
-- ============================================================

CREATE TABLE fact_event (
    id                  INT IDENTITY(1,1) PRIMARY KEY,

    -- Klucze źródłowe (NOAA)
    episode_id          INT,                         -- grupuje powiązane zdarzenia
    event_id            INT,                         -- unikalny ID zdarzenia w NOAA
    source_event_id     INT,                         -- ID w oryginalnym pliku źródłowym
    CONSTRAINT uq_fact_event_source UNIQUE (event_id),

    -- Klucze obce do wymiarów
    begin_time_id       INT NOT NULL FOREIGN KEY REFERENCES dim_time(id),
    end_time_id         INT          FOREIGN KEY REFERENCES dim_time(id),
    location_id         INT NOT NULL FOREIGN KEY REFERENCES dim_location(id),
    source_id           INT          FOREIGN KEY REFERENCES dim_source(id),
    event_type_id       INT NOT NULL FOREIGN KEY REFERENCES dim_event_type(id),
    flood_cause_id      INT          FOREIGN KEY REFERENCES dim_flood_cause(id),
    wfo_id              INT          FOREIGN KEY REFERENCES dim_wfo(id),
    magnitude_type_id   INT          FOREIGN KEY REFERENCES dim_magnitude_type(id),

    -- Czas (granulacja wewnątrz dnia)
    begin_time          TIME,                        -- godzina rozpoczęcia zdarzenia
    end_time            TIME,                        -- godzina zakończenia zdarzenia
    duration_minutes    INT,                         -- obliczana lub ładowana z ETL

    -- Miary ofiar
    injuries_direct     INT          DEFAULT 0,
    injuries_indirect   INT          DEFAULT 0,
    injuries_total      AS (injuries_direct + injuries_indirect), -- kolumna obliczana
    deaths_direct       INT          DEFAULT 0,
    deaths_indirect     INT          DEFAULT 0,
    deaths_total        AS (deaths_direct + deaths_indirect),     -- kolumna obliczana

    -- Miary strat materialnych
    damage_property     DECIMAL(15,2) DEFAULT 0,     -- w USD
    damage_crops        DECIMAL(15,2) DEFAULT 0,     -- w USD
    damage_total        AS (damage_property + damage_crops),      -- kolumna obliczana

    -- Natężenie zdarzenia
    magnitude           DECIMAL(8,2),                -- wartość liczbowa (np. prędkość wiatru, EF scale)

    -- Kontekst demograficzny (wartość w momencie zdarzenia – stąd w fakcie, nie w dim)
    population          INT,
    population_density  FLOAT,

    -- Metadane ETL
    source_load_date    DATETIME     NOT NULL,        -- data aktualności danych w źródle NOAA
    insertion_date      DATETIME     NOT NULL DEFAULT GETDATE(),
    is_deleted          BIT          NOT NULL DEFAULT 0 -- miękkie usunięcie przy korektach NOAA
);

-- ============================================================
-- 3. TABELE STAGINGOWE (ładowane z CSV wygenerowanych przez Pythona)
-- ============================================================

-- Tabela logu ETL – rejestruje każde uruchomienie procesu ładowania
CREATE TABLE etl_delta_log (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    run_date        DATETIME     NOT NULL DEFAULT GETDATE(),
    source_files    VARCHAR(500),
    rows_inserted   INT,
    rows_rejected   INT,
    status          VARCHAR(20)  NOT NULL DEFAULT 'SUCCESS' -- 'SUCCESS' / 'FAILED'
);

-- ============================================================
-- 4. TABELE STAGINGOWE (ładowane z CSV wygenerowanych przez Pythona)
-- ============================================================

-- Odpowiada stg_dim_time.csv
CREATE TABLE stg_dim_time (
    full_date       DATE,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      VARCHAR(20),
    week_of_year    INT,
    year_week       INT,
    day             INT,
    day_of_week     INT,
    day_name        VARCHAR(20),
    is_weekend      BIT,
    season          VARCHAR(10)
);

-- Odpowiada stg_dim_location.csv
CREATE TABLE stg_dim_location (
    state_fips      INT,
    cz_fips         INT,
    state_abbr      CHAR(2),
    state_name      VARCHAR(100),
    cz_name         VARCHAR(255),
    cz_type         CHAR(1),
    time_zone       VARCHAR(50)
);

-- Odpowiada stg_dim_event_type.csv
CREATE TABLE stg_dim_event_type (
    event_type      VARCHAR(100)
);

-- Odpowiada stg_dim_flood_cause.csv
CREATE TABLE stg_dim_flood_cause (
    flood_cause     VARCHAR(255)
);

-- Odpowiada stg_dim_source.csv
CREATE TABLE stg_dim_source (
    source_name     VARCHAR(255)
);

-- Odpowiada stg_dim_wfo.csv
CREATE TABLE stg_dim_wfo (
    wfo_code        VARCHAR(10)
);

-- Odpowiada stg_dim_magnitude_type.csv
CREATE TABLE stg_dim_magnitude_type (
    magnitude_type  VARCHAR(10)
);

-- Odpowiada stg_fact_event.csv
CREATE TABLE stg_fact_event (
    episode_id          INT,
    event_id            INT,
    source_event_id     INT,
    begin_date          DATE,
    end_date            DATE,
    begin_time          TIME,
    end_time            TIME,
    duration_minutes    INT,
    state_fips          INT,
    cz_fips             INT,
    source              VARCHAR(255),
    event_type          VARCHAR(100),
    flood_cause         VARCHAR(255),
    wfo                 VARCHAR(10),
    magnitude_type      VARCHAR(10),
    magnitude           DECIMAL(8,2),
    injuries_direct     INT,
    injuries_indirect   INT,
    deaths_direct       INT,
    deaths_indirect     INT,
    damage_property     DECIMAL(15,2),
    damage_crops        DECIMAL(15,2),
    population          INT,
    population_density  FLOAT,
    source_load_date    DATETIME
);

-- ============================================================
-- 4. INDEKSY (wydajność zapytań analitycznych)
-- ============================================================

-- Najczęstsze filtrowanie: po czasie i lokalizacji
CREATE INDEX ix_fact_event_begin_time   ON fact_event (begin_time_id);
CREATE INDEX ix_fact_event_location     ON fact_event (location_id);
CREATE INDEX ix_fact_event_event_type   ON fact_event (event_type_id);
CREATE INDEX ix_fact_event_episode      ON fact_event (episode_id);
CREATE INDEX ix_fact_event_wfo          ON fact_event (wfo_id);

-- Przyspiesza lookups przy ładowaniu ETL
CREATE INDEX ix_dim_location_fips       ON dim_location (state_fips, cz_fips) WHERE is_current = 1;
CREATE INDEX ix_dim_time_date           ON dim_time (full_date);
CREATE INDEX ix_dim_event_type_cat      ON dim_event_type (category);


"""
NOAA Storm Events – główny runner ETL
======================================
Uruchamia pipeline:
  1. extract  – wczytanie surowych CSV NOAA + pliki referencyjne
  2. transform – czyszczenie, normalizacja, wzbogacenie demograficzne
  3. load     – załadowanie do SQL Server (staging tables) przez SQLAlchemy
               + opcjonalny backup CSV

Użycie:
    python noaa_etl_cleaning.py                      # pełny pipeline
    python noaa_etl_cleaning.py --csv-only           # tylko backup CSV, bez SQL
    python noaa_etl_cleaning.py --skip-load          # extract + transform, bez ładowania    python noaa_etl_cleaning.py --years 2010-2015    # wczytaj lata 2010-2015
    python noaa_etl_cleaning.py --years 2020:2023    # wczytaj lata 2020-2023 (format alt)"""

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from etl.extract import stream_events, count_event_files, load_reference_data
from etl.transform import transform_events, enrich_with_demographics, validate, build_staging_frames
from sqlalchemy import text
from etl.load import (
    get_engine,
    run_sql_script,
    save_csv_backup,
    truncate_all_staging,
    truncate_all_final_tables,
    append_to_table,
    load_dim_staging,
    STAGING_TABLES,
)

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------

RAW_EVENTS_DIR   = Path("data/storm_events")
RAW_AREA_FILE    = Path("data/state_area.csv")
RAW_POP_FILE     = Path("data/state_population.csv")
CSV_BACKUP_DIR   = Path("data/clean")
DW_LOAD_SCRIPT   = Path("scripts/noaa_dw_load.sql")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data/etl_cleaning.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

# Wymiary stagingowe (bez faktu) — akumulowane z wielu plików
_DIM_TABLES = [t for t in STAGING_TABLES if t != "stg_fact_event"]
_FACT_TABLE = "stg_fact_event"

# Klucze naturalne per tabela — używane przy finalnym drop_duplicates po scaleniu
# Bez tego te same (state_fips, cz_fips) z różnych lat mogą mieć różne cz_name
# i trafić jako dwa wiersze → naruszenie uq_dim_location_scd (GETDATE() identyczny)
_DIM_NATURAL_KEYS: dict[str, list[str]] = {
    "stg_dim_time":           ["full_date"],
    "stg_dim_location":       ["state_fips", "cz_fips"],
    "stg_dim_event_type":     ["event_type"],
    "stg_dim_flood_cause":    ["flood_cause"],
    "stg_dim_source":         ["source_name"],
    "stg_dim_wfo":            ["wfo_code"],
    "stg_dim_magnitude_type": ["magnitude_type"],
}


def run_pipeline(
    csv_only: bool = False,
    skip_load: bool = False,
    year_range: tuple[int, int] | None = None,
    full_refresh: bool = False,
    data_dir: Path | None = None,
) -> None:
    events_dir = data_dir if data_dir is not None else RAW_EVENTS_DIR
    t_total = time.perf_counter()
    log.info("═" * 60)
    log.info("  NOAA Storm Events ETL – start")
    if year_range:
        log.info(f"  Zakres lat: {year_range[0]}-{year_range[1]}")
    log.info("═" * 60)

    # 1. Dane referencyjne (małe — wczytujemy raz)
    log.info("\n┌─ Extract: dane referencyjne")
    t0 = time.perf_counter()
    df_area, df_pop = load_reference_data(RAW_AREA_FILE, RAW_POP_FILE)
    log.info(f"└─ Extract: dane referencyjne ({time.perf_counter() - t0:.1f}s)")

    # 2. Przygotowanie kontenerów
    n_files   = count_event_files(events_dir, year_range=year_range)
    dim_parts: dict[str, list[pd.DataFrame]] = {t: [] for t in _DIM_TABLES}
    seen_ids:  set[int] = set()
    n_clean = n_rejects = 0

    # Ścieżka faktu CSV (append mode w --csv-only)
    fact_csv_path = CSV_BACKUP_DIR / f"{_FACT_TABLE}.csv"

    # 3. DB: TRUNCATE przed pętlą
    engine = None
    dw_load_script = Path("scripts/noaa_dw_load.sql")
    if not csv_only and not skip_load:
        engine = get_engine()
        if full_refresh:
            truncate_all_final_tables(engine)
            dw_load_script = Path("scripts/noaa_dw_load_full_refresh.sql")
        else:
            truncate_all_staging(engine)
            dw_load_script = Path("scripts/noaa_dw_load.sql")

    # 4. Pętla strumieniowa po plikach
    log.info(f"\n┌─ Streaming {n_files} plików NOAA")
    t_stream = time.perf_counter()

    with logging_redirect_tqdm():
        bar = tqdm(
            stream_events(events_dir, year_range=year_range),
            total=n_files,
            desc="  ETL",
            unit="rok",
            leave=True,
            ncols=80,
            dynamic_ncols=False,
        )
        for df_raw in bar:
            year = df_raw["source_year"].iloc[0] if "source_year" in df_raw.columns else "?"
            bar.set_postfix(rok=year, refresh=False)

            # transform
            df = transform_events(df_raw)
            df = enrich_with_demographics(df, df_area, df_pop)

            # dedup cross-file: pomiń event_id które już widzieliśmy
            if "event_id" in df.columns:
                mask_new = ~df["event_id"].isin(seen_ids) & df["event_id"].notna()
                seen_ids.update(df.loc[mask_new, "event_id"].astype(int).tolist())
                df = df[mask_new].reset_index(drop=True)

            # validate
            df_clean, df_rej = validate(df)
            n_clean   += len(df_clean)
            n_rejects += len(df_rej)

            if len(df_clean) == 0:
                continue

            # build staging frames
            staging = build_staging_frames(df_clean)

            # akumuluj wymiary
            for t in _DIM_TABLES:
                if t in staging:
                    dim_parts[t].append(staging[t])

            # fakty: deduplikacja wewnątrz pliku po event_id (seen_ids obsługuje cross-file)
            fact_df = staging.get(_FACT_TABLE)
            if fact_df is not None and len(fact_df) > 0:
                fact_df = fact_df.drop_duplicates(subset=["event_id"])
                if engine is not None:
                    append_to_table(_FACT_TABLE, fact_df, engine)
                elif csv_only:
                    CSV_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
                    fact_df.to_csv(
                        fact_csv_path,
                        mode="a",
                        header=not fact_csv_path.exists(),
                        index=False,
                        encoding="utf-8-sig",
                    )

        bar.close()

    log.info(f"└─ Streaming ({time.perf_counter() - t_stream:.1f}s)")
    log.info(f"\n  Zdarzenia czyste:    {n_clean:>10,}")
    log.info(f"  Zdarzenia odrzucone: {n_rejects:>10,}")

    # 5. Scalenie i zapis wymiarów
    log.info("\n┌─ Scalanie wymiarów (distinct)")
    t0 = time.perf_counter()
    final_dims: dict[str, pd.DataFrame] = {}
    for t, parts in dim_parts.items():
        if not parts:
            continue
        merged = pd.concat(parts, ignore_index=True)
        subset = _DIM_NATURAL_KEYS.get(t)
        merged = merged.drop_duplicates(subset=subset).reset_index(drop=True)
        final_dims[t] = merged
    log.info(f"└─ Scalanie wymiarów ({time.perf_counter() - t0:.1f}s)")

    if skip_load:
        log.info("\n--skip-load: pominięto ładowanie do bazy.")
        save_csv_backup(final_dims, CSV_BACKUP_DIR)
    elif csv_only:
        save_csv_backup(final_dims, CSV_BACKUP_DIR)
    else:
        load_dim_staging(final_dims, engine)
        if dw_load_script.exists():
            log.info(f"\n┌─ Load: {dw_load_script.name}")
            t0 = time.perf_counter()

            with engine.connect() as _c:
                rows_before = _c.execute(text("SELECT COUNT(*) FROM dbo.fact_event")).scalar() or 0
            if not full_refresh:
                log.info(f"  ▶ fact_event przed delta:   {rows_before:>10,} wierszy")

            run_sql_script(dw_load_script, engine)

            with engine.connect() as _c:
                rows_after = _c.execute(text("SELECT COUNT(*) FROM dbo.fact_event")).scalar() or 0
            delta_rows = rows_after - rows_before
            if full_refresh:
                log.info(f"  ✔ FULL REFRESH: {rows_after:,} wierszy załadowanych")
            else:
                log.info(f"  ▶ fact_event po delta:      {rows_after:>10,} wierszy")
                log.info(f"  ✔ DELTA:        +{delta_rows:,} nowych wierszy  ({rows_before:,} już istniało → pominięto)")

            log.info(f"└─ Load: {dw_load_script.name} ({time.perf_counter() - t0:.1f}s)")
        else:
            log.warning(f"Skrypt {dw_load_script} nie istnieje – pominięto ładowanie wymiarów.")

    total = time.perf_counter() - t_total
    log.info(f"\n{'═' * 60}")
    log.info(f"  KONIEC  –  łącznie {total:.1f}s  –  {n_clean:,} zdarzeń")
    log.info("═" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NOAA Storm Events ETL pipeline")
    parser.add_argument("--csv-only",   action="store_true", help="Zapisz tylko CSV, nie ładuj do SQL Server")
    parser.add_argument("--skip-load",  action="store_true", help="Zakończ po transformacji, bez zapisu")
    parser.add_argument("--full-refresh", action="store_true", help="Resetuj wszystkie tabele docelowe i ładuj od zera (bez delta-check — szybsze)")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Katalog z plikami StormEvents CSV (domyślnie: data/storm_events). Użyj data/test dla danych testowych.",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Zakreś lat do wczytania, format: YYYY-YYYY lub YYYY:YYYY (np. 2010-2015)",
    )
    args = parser.parse_args()

    # Parsowanie year_range
    year_range = None
    if args.years:
        # Akceptuj zarówno "-" jak i ":" jako separator
        separator = "-" if "-" in args.years else ":"
        parts = args.years.split(separator)
        if len(parts) != 2:
            parser.error(f"--years musi być w formacie YYYY-YYYY lub YYYY:YYYY, otrzymano: {args.years}")
        try:
            min_year = int(parts[0].strip())
            max_year = int(parts[1].strip())
            if min_year > max_year:
                parser.error(f"--years: rok początkowy {min_year} nie może być większy od {max_year}")
            year_range = (min_year, max_year)
            log.info(f"Filtrowanie na lata: {min_year}-{max_year}")
        except ValueError:
            parser.error(f"--years: lata muszą być liczbami, otrzymano: {args.years}")

    data_dir = Path(args.data_dir) if args.data_dir else None
    run_pipeline(csv_only=args.csv_only, skip_load=args.skip_load, year_range=year_range, full_refresh=args.full_refresh, data_dir=data_dir)


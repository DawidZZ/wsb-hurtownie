"""
etl/extract.py – krok 1 ETL: wczytywanie danych źródłowych
===========================================================
Odpowiada za:
  - wczytanie wszystkich rocznych CSV NOAA Storm Events
  - wczytanie plików referencyjnych (area, population)
"""

import re
import logging
from pathlib import Path
from typing import Generator

import pandas as pd

log = logging.getLogger(__name__)

RAW_EVENTS_DIR = Path("data/storm_events")
RAW_AREA_FILE  = Path("data/state_area.csv")
RAW_POP_FILE   = Path("data/state_population.csv")


def count_event_files(
    directory: Path = RAW_EVENTS_DIR,
    year_range: tuple[int, int] | None = None,
) -> int:
    """Zwraca liczbę plików StormEvents_details*.csv w katalogu.
    
    Param year_range: tuple (min_year, max_year) — filtruje pliki po roku z nazwy.
                      Jeśli None, zwraca wszystkie pliki.
    """
    files = sorted(directory.glob("StormEvents_details*.csv"))
    
    if year_range is None:
        return len(files)
    
    min_year, max_year = year_range
    filtered = []
    for f in files:
        m = re.search(r"_d(\d{4})_", f.name)
        if m:
            year = int(m.group(1))
            if min_year <= year <= max_year:
                filtered.append(f)
    return len(filtered)


def stream_events(
    directory: Path = RAW_EVENTS_DIR,
    year_range: tuple[int, int] | None = None,
) -> Generator[pd.DataFrame, None, None]:
    """
    Generator: wczytuje pliki StormEvents_details*.csv jeden po drugim,
    dodaje kolumnę source_year i yield-uje każdy jako osobny DataFrame.
    Nie trzyma wszystkich danych w pamięci jednocześnie.
    
    Param year_range: tuple (min_year, max_year) — filtruje pliki po roku z nazwy.
                      Jeśli None, wczytuje wszystkie pliki.
    """
    files = sorted(directory.glob("StormEvents_details*.csv"))
    if not files:
        raise FileNotFoundError(f"Brak plików CSV w {directory}")

    # Filtruj pliki po roku jeśli podano year_range
    if year_range is not None:
        min_year, max_year = year_range
        filtered_files = []
        for f in files:
            m = re.search(r"_d(\d{4})_", f.name)
            if m:
                year = int(m.group(1))
                if min_year <= year <= max_year:
                    filtered_files.append(f)
        files = filtered_files
        if not files:
            log.warning(f"Brak plików w zakresie lat {min_year}-{max_year}")
    
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str, low_memory=False)
            df.columns = df.columns.str.lower().str.strip()
            m = re.search(r"_d(\d{4})_", f.name)
            df["source_year"] = int(m.group(1)) if m else None
            log.debug(f"  {f.name}: {len(df):,} wierszy")
            yield df
        except Exception as e:
            log.warning(f"  Błąd przy {f.name}: {e}")


def load_reference_data(
    area_file: Path = RAW_AREA_FILE,
    pop_file:  Path = RAW_POP_FILE,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Wczytuje pliki referencyjne:
      - state_area.csv     (STATE_FIPS, STATE_NAME, AREA_SQ_MI)
      - state_population.csv (STATE_FIPS, STATE_NAME, YEAR, POPULATION)

    Zwraca: (df_area, df_pop)
    """
    log.info("Wczytywanie danych referencyjnych...")

    df_area = pd.read_csv(area_file, dtype=str)
    df_area.columns = df_area.columns.str.lower().str.strip()
    df_area["state_fips"] = pd.to_numeric(df_area["state_fips"], errors="coerce").astype("Int64")
    df_area["area_sq_mi"] = pd.to_numeric(df_area["area_sq_mi"], errors="coerce")
    log.info(f"  area:       {len(df_area)} stanów")

    df_pop = pd.read_csv(pop_file, dtype=str)
    df_pop.columns = df_pop.columns.str.lower().str.strip()
    df_pop["state_fips"] = pd.to_numeric(df_pop["state_fips"], errors="coerce").astype("Int64")
    df_pop["year"]       = pd.to_numeric(df_pop["year"],       errors="coerce").astype("Int64")
    df_pop["population"] = pd.to_numeric(df_pop["population"], errors="coerce")
    log.info(f"  population: {len(df_pop)} wierszy (stan × rok)")

    return df_area, df_pop

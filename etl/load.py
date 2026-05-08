"""
etl/load.py – krok 3 ETL: ładowanie do SQL Server przez SQLAlchemy
===================================================================
Odpowiada za:
  - budowanie silnika SQLAlchemy z .env
  - ładowanie staging DataFrames (TRUNCATE + INSERT)
  - uruchamianie skryptów SQL (noaa_dw_load.sql) batch po batch
  - opcjonalny zapis CSV jako kopia zapasowa
"""

import os
import logging
import time
import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tqdm import tqdm

load_dotenv()
log = logging.getLogger(__name__)

# Kolejność ważna: wymiary przed faktem (FK constraints)
STAGING_TABLES = [
    "stg_dim_time",
    "stg_dim_location",
    "stg_dim_event_type",
    "stg_dim_flood_cause",
    "stg_dim_source",
    "stg_dim_wfo",
    "stg_dim_magnitude_type",
    "stg_fact_event",
]


def get_engine() -> Engine:
    host = os.getenv("CHECK_SCHEMA_HOST", os.getenv("SQL_SERVER_HOST", "localhost"))
    port = os.getenv("SQL_SERVER_PORT", "1433")
    db   = os.getenv("SQL_SERVER_DB", "hurtownie")
    pwd  = os.getenv("SA_PASSWORD", "")
    url  = f"mssql+pymssql://sa:{pwd}@{host}:{port}/{db}"
    return create_engine(url)


def truncate_all_staging(engine: Engine | None = None) -> None:
    """TRUNCATE wszystkich tabel stg_* jednorazowo przed pętlą streamingową."""
    if engine is None:
        engine = get_engine()
    with engine.begin() as conn:
        for table in STAGING_TABLES:
            conn.execute(text(f"TRUNCATE TABLE dbo.{table}"))
    log.info(f"TRUNCATE: {len(STAGING_TABLES)} tabel stagingowych wyczyszczonych")


def execute_bulk_insert(
    conn,
    table: str,
    df: pd.DataFrame,
    schema: str = "dbo",
    batch_size: int = 1000,
) -> None:
    """
    Wstawia wiersze DataFrame do tabeli SQL Server używając multi-row INSERT VALUES.
    Max 1000 wierszy na statement (limit SQL Server 2016+).
    ~10-20x szybsze niż to_sql() dla dużych DataFrames.
    
    Param conn: SQLAlchemy connection
    Param table: nazwa tabeli (bez schematu)
    Param df: DataFrame do wstawienia
    Param schema: schemat tabeli (domyślnie "dbo")
    Param batch_size: ile wierszy na INSERT statement (max 1000)
    """
    if len(df) == 0:
        return

    columns = df.columns.tolist()
    col_names = ", ".join(f"[{c}]" for c in columns)
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size]
        
        # Buduj VALUES tuples z proper escaping
        rows_values = []
        for _, row in batch.iterrows():
            row_values = []
            for val in row:
                if pd.isna(val) or val is None:
                    row_values.append("NULL")
                elif isinstance(val, bool):
                    # bool musi być przed int (bool jest subclass int)
                    row_values.append("1" if val else "0")
                elif isinstance(val, (int, np.integer)):
                    row_values.append(str(int(val)))
                elif isinstance(val, (float, np.floating)):
                    # NaN już obsłużony powyżej; zwykłe float
                    row_values.append(str(float(val)))
                elif isinstance(val, (datetime.date, datetime.time)):
                    row_values.append(f"'{val.isoformat()}'")
                elif isinstance(val, pd.Timestamp):
                    row_values.append(f"'{val.isoformat()}'")
                else:
                    # String — escape single quotes ('' to ' w SQL)
                    str_val = str(val)
                    escaped = str_val.replace("'", "''")
                    row_values.append(f"'{escaped}'")
            rows_values.append("(" + ", ".join(row_values) + ")")
        
        values_clause = ", ".join(rows_values)
        sql = f"INSERT INTO [{schema}].[{table}] ({col_names}) VALUES {values_clause}"
        conn.execute(text(sql))


def append_to_table(
    table: str,
    df: pd.DataFrame,
    engine: Engine | None = None,
) -> None:
    """
    INSERT wierszy bez poprzedniego TRUNCATE.
    Używany do strumieniowego dołączania faktów w pętli po plikach.
    Teraz używa multi-row INSERT VALUES zamiast to_sql() — ~10-20x szybciej.
    """
    if engine is None:
        engine = get_engine()
    with engine.begin() as conn:
        execute_bulk_insert(conn, table, df, schema="dbo", batch_size=1000)


def load_dim_staging(
    dims: dict[str, pd.DataFrame],
    engine: Engine | None = None,
) -> None:
    """
    TRUNCATE + INSERT dla każdego wymiaru stagingowego (po scaleniu z wielu plików).
    Wypisuje drzewiaste logi z czasem per tabela.
    Teraz używa multi-row INSERT VALUES zamiast to_sql() — ~10-20x szybciej.
    """
    if engine is None:
        engine = get_engine()

    total = len(dims)
    log.info(f"┌─ Ładowanie {total} wymiarów stagingowych")
    with engine.begin() as conn:
        for i, (table, df) in enumerate(dims.items(), 1):
            t0 = time.perf_counter()
            conn.execute(text(f"TRUNCATE TABLE dbo.{table}"))
            execute_bulk_insert(conn, table, df, schema="dbo", batch_size=1000)
            elapsed = time.perf_counter() - t0
            prefix = "└─" if i == total else "├─"
            log.info(f"{prefix} [{i}/{total}] {table:<35} {len(df):>7,} wierszy  ({elapsed:.1f}s)")


def load_staging(staging: dict[str, pd.DataFrame], engine: Engine | None = None) -> None:
    """
    Klasyczne ładowanie wsadowe (TRUNCATE + INSERT) dla wszystkich tabel stagingowych.
    Zachowane dla kompatybilności / trybu --csv-only bez streamingu.
    Teraz używa multi-row INSERT VALUES zamiast to_sql() — ~10-20x szybciej.
    """
    if engine is None:
        engine = get_engine()

    total = len(staging)
    log.info(f"┌─ Ładowanie {total} tabel stagingowych do SQL Server")

    with engine.begin() as conn:
        for i, (table, df) in enumerate(staging.items(), 1):
            t0 = time.perf_counter()
            conn.execute(text(f"TRUNCATE TABLE dbo.{table}"))
            execute_bulk_insert(conn, table, df, schema="dbo", batch_size=1000)
            elapsed = time.perf_counter() - t0
            prefix = "└─" if i == total else "├─"
            log.info(f"{prefix} [{i}/{total}] {table:<35} {len(df):>7,} wierszy  ({elapsed:.1f}s)")


def run_sql_script(script_path: Path, engine: Engine | None = None) -> None:
    """
    Uruchamia plik .sql batch po batchu (splituje po 'GO').
    Pasek tqdm w tej samej linii (leave=False), logi przez tqdm.write.
    """
    if engine is None:
        engine = get_engine()

    sql = script_path.read_text(encoding="utf-8")
    batches = [b.strip() for b in sql.split("\nGO") if b.strip()]
    non_empty = [
        b for b in batches
        if "\n".join(ln for ln in b.splitlines() if not ln.strip().startswith("--")).strip()
    ]

    log.info(f"Uruchamianie {script_path.name} ({len(non_empty)} batchy)...")

    # Kody błędów SQL Server, które traktujemy jako ostrzeżenia (nie przerywają pipeline)
    # 2627 = naruszenie UK (duplikat), 1913 = indeks już istnieje, 2601 = duplikat PK
    IGNORABLE_CODES = {2627, 2601, 1913}

    bar = tqdm(
        non_empty,
        desc=f"  {script_path.name}",
        unit="batch",
        leave=False,
        ncols=80,
        dynamic_ncols=False,
    )
    for batch in bar:
        stripped = "\n".join(
            ln for ln in batch.splitlines()
            if not ln.strip().startswith("--")
        ).strip()
        # Każdy batch w osobnej transakcji — błąd w jednym nie cofa pozostałych
        try:
            with engine.begin() as conn:
                conn.execute(text(stripped))
        except Exception as e:
            code = getattr(e.orig, "args", [None])[0] if hasattr(e, "orig") else None
            if code in IGNORABLE_CODES:
                log.warning(f"  Pominięto (już istnieje, kod {code}): {str(e)[:120]}")
            else:
                bar.close()
                log.error(f"  Błąd w batchu: {e}")
                raise
    bar.close()
    log.info(f"  {script_path.name} wykonany.")


def save_csv_backup(staging: dict[str, pd.DataFrame], output_dir: Path) -> None:
    """Opcjonalny zapis DataFrames do CSV jako kopia zapasowa / debug."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for table, df in staging.items():
        path = output_dir / f"{table}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        log.info(f"  Backup CSV: {path.name} ({len(df):,} wierszy)")

#!/usr/bin/env python3
"""Sprawdza stan schemy w bazie NOAA DWH i wyświetla istniejące tabele."""

import os
import sys
import pymssql
from dotenv import load_dotenv

load_dotenv()

HOST     = os.getenv("CHECK_SCHEMA_HOST", os.getenv("SQL_SERVER_HOST", "localhost"))
PORT     = int(os.getenv("SQL_SERVER_PORT", 1433))
DB       = os.getenv("SQL_SERVER_DB", "hurtownie")
PASSWORD = os.getenv("SA_PASSWORD")

EXPECTED_TABLES = {
    "dim_time", "dim_location", "dim_source", "dim_event_type",
    "dim_flood_cause", "dim_wfo", "dim_magnitude_type",
    "fact_event",
    "stg_dim_time", "stg_dim_location", "stg_dim_source", "stg_dim_event_type",
    "stg_dim_flood_cause", "stg_dim_wfo", "stg_dim_magnitude_type",
    "stg_fact_event",
    "etl_delta_log",
}

# ANSI colors
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def main():
    print(f"\n{BOLD}=== NOAA DWH – Stan schematu ==={RESET}")
    print(f"  Serwer : {HOST}:{PORT}")
    print(f"  Baza   : {DB}\n")

    try:
        conn = pymssql.connect(
            host=HOST, port=PORT,
            user="sa", password=PASSWORD,
            database=DB,
            login_timeout=10,
        )
    except Exception as e:
        print(f"{RED}✗ Nie można połączyć się z bazą danych:{RESET} {e}")
        sys.exit(1)

    cursor = conn.cursor(as_dict=True)

    # Tabele z liczbą wierszy
    cursor.execute("""
        SELECT
            t.name                        AS table_name,
            SUM(p.rows)                   AS row_count,
            CAST(
                SUM(a.total_pages) * 8.0 / 1024
            AS DECIMAL(10,2))             AS size_kb
        FROM sys.tables t
        JOIN sys.partitions p
            ON t.object_id = p.object_id AND p.index_id IN (0,1)
        JOIN sys.allocation_units a
            ON p.partition_id = a.container_id
        WHERE t.type = 'U'
        GROUP BY t.name
        ORDER BY t.name
    """)
    rows = cursor.fetchall()

    existing = {r["table_name"] for r in rows}
    missing  = EXPECTED_TABLES - existing
    extra    = existing - EXPECTED_TABLES

    # -- Tabele obecne ---------------------------------------------------------
    col_w = [30, 12, 10]
    header = f"{'Tabela':<{col_w[0]}} {'Wiersze':>{col_w[1]}} {'Rozmiar':>{col_w[2]}}"
    sep    = "-" * (sum(col_w) + 2)

    print(f"{BOLD}{CYAN}Tabele w bazie:{RESET}")
    print(sep)
    print(f"{BOLD}{header}{RESET}")
    print(sep)

    for r in rows:
        name      = r["table_name"]
        row_count = r["row_count"] or 0
        size      = r["size_kb"] or 0
        status    = "" if name in EXPECTED_TABLES else f" {YELLOW}[nieoczekiwana]{RESET}"
        print(f"{GREEN}{name:<{col_w[0]}}{RESET} {row_count:>{col_w[1]},} {size:>{col_w[1]-2}.1f} KB{status}")

    print(sep)
    print(f"  Razem: {len(existing)} tabel\n")

    # -- Brakujące tabele ------------------------------------------------------
    if missing:
        print(f"{BOLD}{RED}Brakujące tabele ({len(missing)}):{RESET}")
        for t in sorted(missing):
            print(f"  {RED}✗  {t}{RESET}")
        print()
    else:
        print(f"{GREEN}{BOLD}✓ Wszystkie oczekiwane tabele istnieją.{RESET}\n")

    # -- Indeksy ---------------------------------------------------------------
    cursor.execute("""
        SELECT
            t.name  AS table_name,
            i.name  AS index_name,
            i.type_desc
        FROM sys.indexes i
        JOIN sys.tables  t ON i.object_id = t.object_id
        WHERE i.type > 0
          AND t.name NOT LIKE 'stg_%'
        ORDER BY t.name, i.name
    """)
    indexes = cursor.fetchall()

    print(f"{BOLD}{CYAN}Indeksy (tabele docelowe):{RESET}")
    print(sep)
    for idx in indexes:
        print(f"  {idx['table_name']:<30} {idx['index_name']:<40} {idx['type_desc']}")
    if not indexes:
        print(f"  {YELLOW}Brak indeksów.{RESET}")
    print()

    conn.close()


if __name__ == "__main__":
    main()

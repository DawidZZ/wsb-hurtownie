#!/usr/bin/env python3
"""
Szybki podgląd danych w bazie docelowej NOAA DWH.
Uruchom po python noaa_etl_cleaning.py.
"""

import os
import sys
import pymssql
from dotenv import load_dotenv

load_dotenv()

HOST     = os.getenv("CHECK_SCHEMA_HOST", os.getenv("SQL_SERVER_HOST", "localhost"))
PORT     = int(os.getenv("SQL_SERVER_PORT", 1433))
DB       = os.getenv("SQL_SERVER_DB", "hurtownie")
PASSWORD = os.getenv("SA_PASSWORD")

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

# ---------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n{BOLD}{CYAN}── {title} {'─' * max(0, 54 - len(title))}{RESET}")


def run(cursor, sql: str) -> list[dict]:
    cursor.execute(sql)
    return cursor.fetchall() or []


def fmt_n(n) -> str:
    if n is None:
        return "NULL"
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def fmt_usd(n) -> str:
    if n is None:
        return "NULL"
    n = float(n)
    if n >= 1e9:
        return f"${n/1e9:.2f}B"
    if n >= 1e6:
        return f"${n/1e6:.1f}M"
    if n >= 1e3:
        return f"${n/1e3:.1f}K"
    return f"${n:.0f}"


# ---------------------------------------------------------------------------

def main() -> None:
    print(f"\n{BOLD}=== NOAA DWH – Podgląd danych ==={RESET}")
    print(f"  {DIM}{HOST}:{PORT}  /  {DB}{RESET}")

    try:
        conn = pymssql.connect(
            host=HOST, port=PORT, user="sa", password=PASSWORD,
            database=DB, login_timeout=10,
        )
    except Exception as e:
        print(f"{RED}✗ Brak połączenia: {e}{RESET}")
        sys.exit(1)

    cur = conn.cursor(as_dict=True)

    # ------------------------------------------------------------------
    # 1. Liczba wierszy w tabelach docelowych
    # ------------------------------------------------------------------
    section("Tabele docelowe – liczba wierszy")

    rows = run(cur, """
        SELECT t.name AS tbl, SUM(p.rows) AS cnt
        FROM sys.tables t
        JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
        WHERE t.name IN (
            'dim_time','dim_location','dim_source','dim_event_type',
            'dim_flood_cause','dim_wfo','dim_magnitude_type','fact_event'
        )
        GROUP BY t.name ORDER BY t.name
    """)

    for r in rows:
        cnt = int(r["cnt"])
        color = GREEN if cnt > 0 else RED
        print(f"  {r['tbl']:<28} {color}{cnt:>10,}{RESET}")

    fact_count = next((int(r["cnt"]) for r in rows if r["tbl"] == "fact_event"), 0)
    if fact_count == 0:
        print(f"\n  {RED}fact_event jest puste – ETL mógł nie wykonać noaa_dw_load.sql{RESET}")
        conn.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # 2. Zakres lat i dat
    # ------------------------------------------------------------------
    section("Zakres danych w fact_event")

    r = run(cur, """
        SELECT
            MIN(t.year)      AS rok_min,
            MAX(t.year)      AS rok_max,
            MIN(t.full_date) AS data_min,
            MAX(t.full_date) AS data_max,
            COUNT(DISTINCT t.year) AS lat
        FROM fact_event f
        JOIN dim_time t ON f.begin_time_id = t.id
    """)[0]
    print(f"  Lata    : {r['rok_min']} – {r['rok_max']}  ({r['lat']} lat)")
    print(f"  Daty    : {r['data_min']} – {r['data_max']}")

    # ------------------------------------------------------------------
    # 3. Sumy kontrolne
    # ------------------------------------------------------------------
    section("Sumy kontrolne")

    r = run(cur, """
        SELECT
            SUM(damage_property + damage_crops) AS straty_total,
            SUM(deaths_total)                   AS ofiary,
            SUM(injuries_total)                 AS ranni,
            COUNT(DISTINCT location_id)         AS lokalizacji,
            COUNT(DISTINCT event_type_id)       AS typow_zdarzen
        FROM fact_event
    """)[0]
    print(f"  Straty łączne    : {YELLOW}{fmt_usd(r['straty_total'])}{RESET}")
    print(f"  Ofiary śmiertelne: {fmt_n(r['ofiary'])}")
    print(f"  Ranni            : {fmt_n(r['ranni'])}")
    print(f"  Lokalizacji      : {fmt_n(r['lokalizacji'])}")
    print(f"  Typów zdarzeń    : {fmt_n(r['typow_zdarzen'])}")

    # ------------------------------------------------------------------
    # 4. Integralność kluczy obcych
    # ------------------------------------------------------------------
    section("Integralność FK")

    checks = [
        ("begin_time_id IS NULL",  "Fakty bez daty początku"),
        ("location_id IS NULL",    "Fakty bez lokalizacji"),
        ("event_type_id IS NULL",  "Fakty bez typu zdarzenia"),
    ]
    ok = True
    for cond, label in checks:
        r = run(cur, f"SELECT COUNT(*) AS n FROM fact_event WHERE {cond}")[0]
        n = int(r["n"])
        color = GREEN if n == 0 else RED
        print(f"  {label:<30} {color}{n:>8,}{RESET}")
        if n > 0:
            ok = False

    if ok:
        print(f"\n  {GREEN}✓ Wszystkie FK poprawne{RESET}")

    # ------------------------------------------------------------------
    # 5. Top 5 stanów wg strat
    # ------------------------------------------------------------------
    section("Top 5 stanów wg łącznych strat")

    rows = run(cur, """
        SELECT TOP 5
            loc.state_abbr,
            loc.state_name,
            COUNT(*)                                    AS zdarzen,
            SUM(f.damage_property + f.damage_crops)    AS straty
        FROM fact_event f
        JOIN dim_location loc ON f.location_id = loc.id
        GROUP BY loc.state_abbr, loc.state_name
        ORDER BY straty DESC
    """)
    for i, r in enumerate(rows, 1):
        print(f"  {i}. {r['state_abbr']} {r['state_name']:<22}"
              f" {fmt_usd(r['straty']):>8}  ({fmt_n(r['zdarzen'])} zdarzeń)")

    # ------------------------------------------------------------------
    # 6. Top 5 typów zdarzeń wg liczby
    # ------------------------------------------------------------------
    section("Top 5 typów zdarzeń wg liczby")

    rows = run(cur, """
        SELECT TOP 5
            et.event_type,
            et.category,
            COUNT(*) AS n,
            SUM(f.deaths_total + f.injuries_total) AS poszkodowani
        FROM fact_event f
        JOIN dim_event_type et ON f.event_type_id = et.id
        GROUP BY et.event_type, et.category
        ORDER BY n DESC
    """)
    for r in rows:
        print(f"  {r['event_type']:<28} {fmt_n(r['n']):>8}  ofiary: {fmt_n(r['poszkodowani'])}")

    # ------------------------------------------------------------------
    # 7. Sezonowość (liczba zdarzeń)
    # ------------------------------------------------------------------
    section("Sezonowość")

    rows = run(cur, """
        SELECT t.season, COUNT(*) AS n, SUM(f.damage_property + f.damage_crops) AS straty
        FROM fact_event f
        JOIN dim_time t ON f.begin_time_id = t.id
        WHERE t.season IS NOT NULL
        GROUP BY t.season
        ORDER BY n DESC
    """)
    for r in rows:
        bar_len = int(int(r["n"]) / fact_count * 40)
        bar = "█" * bar_len
        print(f"  {r['season']:<8} {bar:<40} {fmt_n(r['n']):>8}  {fmt_usd(r['straty'])}")

    # ------------------------------------------------------------------
    # 8. Historia ETL delta log
    # ------------------------------------------------------------------
    section("Historia ETL (ostatnie 5 uruchomień)")

    rows = run(cur, """
        SELECT TOP 5 run_date, rows_inserted, rows_rejected, status
        FROM etl_delta_log ORDER BY run_date DESC
    """)
    if rows:
        for r in rows:
            color = GREEN if r["status"] == "SUCCESS" else RED
            print(f"  {str(r['run_date'])[:19]}  "
                  f"inserted: {fmt_n(r['rows_inserted']):>8}  "
                  f"rejected: {fmt_n(r['rows_rejected']):>6}  "
                  f"{color}{r['status']}{RESET}")
    else:
        print(f"  {YELLOW}Brak wpisów w etl_delta_log{RESET}")

    print()
    conn.close()


if __name__ == "__main__":
    main()

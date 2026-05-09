"""
Snippet pokazujący co dodać do load.py
"""

# Dodaj tę stałą po STAGING_TABLES:
FINAL_TABLES = [
    "fact_event",
    "etl_delta_log",
    "dim_location",
    "dim_event_type",
    "dim_flood_cause",
    "dim_source",
    "dim_wfo",
    "dim_magnitude_type",
    "dim_time",
]


# Dodaj tę funkcję po truncate_all_staging():
def truncate_all_final_tables(engine: Engine | None = None) -> None:
    """TRUNCATE wszystkich tabel docelowych (dim_*, fact_event) — dla full refresh.
    
    Resets IDENTITY seeds do 0 aby ID były od 1 — używane tylko w --full-refresh mode.
    """
    if engine is None:
        engine = get_engine()
    with engine.begin() as conn:
        for table in FINAL_TABLES:
            conn.execute(text(f"TRUNCATE TABLE dbo.{table}"))
            log.debug(f"  TRUNCATE: {table}")
    
    # Reset IDENTITY seeds
    with engine.begin() as conn:
        for table in ["dim_time", "dim_location", "dim_event_type", "dim_flood_cause",
                      "dim_source", "dim_wfo", "dim_magnitude_type", "fact_event"]:
            conn.execute(text(f"DBCC CHECKIDENT ('dbo.{table}', RESEED, 0)"))
    
    log.info(f"TRUNCATE + IDENTITY RESET: {len(FINAL_TABLES)} tabel docelowych wyczyszczonych")

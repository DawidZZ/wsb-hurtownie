from sqlalchemy import create_engine, text
from pathlib import Path
import pandas as pd

CLEAN_DIR = Path("data/clean")

ENGINE = create_engine(
    "mssql+pyodbc://localhost/hurtownie_danych-PROJEKT_2026"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

STG_FILES = {
    "stg_dim_location":      "stg_dim_location.csv",
    "stg_dim_event_type":    "stg_dim_event_type.csv",
    "stg_dim_flood_cause":   "stg_dim_flood_cause.csv",
    "stg_dim_source":        "stg_dim_source.csv",
    "stg_dim_wfo":           "stg_dim_wfo.csv",
    "stg_dim_magnitude_type":"stg_dim_magnitude_type.csv",
    "stg_fact_event":        "stg_fact_event.csv",
}

def load_staging():
    with ENGINE.begin() as conn:
        for table, fname in STG_FILES.items():
            path = CLEAN_DIR / fname
            df = pd.read_csv(path, encoding="utf-8-sig")
            conn.execute(text(f"TRUNCATE TABLE dbo.{table}"))
            df.to_sql(table, conn, schema="dbo", if_exists="append", index=False)
            print(f"  {table}: {len(df):,} wierszy")

if __name__ == "__main__":
    load_staging()
"""
etl/transform.py – krok 2 ETL: czyszczenie i transformacja
============================================================
Odpowiada za:
  - normalizację typów zdarzeń, stref czasowych, nazw stanów
  - parsowanie uszkodzeń (K/M/B)
  - składanie datetime z kolumn NOAA
  - wzbogacanie o dane demograficzne
  - deduplikację i walidację
  - budowanie słowników DataFrames gotowych do załadowania (staging)
"""

import re
import logging
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mapowania
# ---------------------------------------------------------------------------

EVENT_TYPE_MAP = {
    "THUNDERSTORM WINDS":            "Thunderstorm Wind",
    "THUNDERSTORM WIND":             "Thunderstorm Wind",
    "TSTM WIND":                     "Thunderstorm Wind",
    "TSTM WIND/HAIL":                "Thunderstorm Wind/Hail",
    "MARINE TSTM WIND":              "Marine Thunderstorm Wind",
    "FLASH FLOODING":                "Flash Flood",
    "FLASH FLOOD/FLOOD":             "Flash Flood",
    "FLOODING":                      "Flood",
    "RIVER FLOOD":                   "Flood",
    "URBAN/SML STREAM FLD":          "Flood",
    "WILD/FOREST FIRE":              "Wildfire",
    "WILDFIRES":                     "Wildfire",
    "EXTREME COLD":                  "Extreme Cold/Wind Chill",
    "EXTREME COLD/WIND CHILL":       "Extreme Cold/Wind Chill",
    "EXTREME WINDCHILL":             "Extreme Cold/Wind Chill",
    "HEAVY SNOW":                    "Heavy Snow",
    "EXCESSIVE HEAT":                "Excessive Heat",
    "HEAT WAVE":                     "Excessive Heat",
    "HIGH WINDS":                    "High Wind",
    "HIGH WIND":                     "High Wind",
    "STRONG WINDS":                  "Strong Wind",
    "STRONG WIND":                   "Strong Wind",
    "TORNADO":                       "Tornado",
    "WATERSPOUT":                    "Waterspout",
    "HURRICANE":                     "Hurricane",
    "HURRICANE/TYPHOON":             "Hurricane",
    "TYPHOON":                       "Hurricane",
    "TROPICAL STORM":                "Tropical Storm",
    "BLIZZARD":                      "Blizzard",
    "WINTER STORM":                  "Winter Storm",
    "WINTER WEATHER":                "Winter Weather",
    "ICE STORM":                     "Ice Storm",
    "HAIL":                          "Hail",
    "DROUGHT":                       "Drought",
    "DUST STORM":                    "Dust Storm",
    "DENSE FOG":                     "Dense Fog",
    "FREEZING FOG":                  "Freezing Fog",
    "FROST/FREEZE":                  "Frost/Freeze",
    "COLD/WIND CHILL":               "Cold/Wind Chill",
    "LIGHTNING":                     "Lightning",
    "RIP CURRENT":                   "Rip Current",
    "RIP CURRENTS":                  "Rip Current",
    "AVALANCHE":                     "Avalanche",
    "DENSE SMOKE":                   "Dense Smoke",
    "SLEET":                         "Sleet",
    "SEICHE":                        "Seiche",
    "TSUNAMI":                       "Tsunami",
    "VOLCANIC ASH":                  "Volcanic Ash",
    "ASTRONOMICAL LOW TIDE":         "Astronomical Low Tide",
    "ASTRONOMICAL HIGH TIDE":        "Astronomical High Tide",
    "COASTAL FLOOD":                 "Coastal Flood",
    "STORM SURGE/TIDE":              "Storm Surge/Tide",
    "LAKESHORE FLOOD":               "Lakeshore Flood",
    "LAKE-EFFECT SNOW":              "Lake-Effect Snow",
    "FUNNEL CLOUD":                  "Funnel Cloud",
    "DUST DEVIL":                    "Dust Devil",
    "SNEAKERWAVE":                   "High Surf",
    "HIGH SURF":                     "High Surf",
    "LANDSLIDE":                     "Debris Flow",
    "DEBRIS FLOW":                   "Debris Flow",
    "MARINE HIGH WIND":              "Marine High Wind",
    "MARINE STRONG WIND":            "Marine Strong Wind",
    "MARINE DENSE FOG":              "Marine Dense Fog",
    "MARINE HAIL":                   "Marine Hail",
    "NORTHERN LIGHTS":               "Northern Lights",
}

STATE_ABBR_MAP = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY", "District Of Columbia": "DC",
    "Puerto Rico": "PR", "Virgin Islands": "VI", "American Samoa": "AS",
    "Guam": "GU", "Northern Mariana Islands": "MP",
}

TIMEZONE_MAP = {
    "EST": "EST-5",  "EDT": "EST-5",
    "CST": "CST-6",  "CDT": "CST-6",
    "MST": "MST-7",  "MDT": "MST-7",
    "PST": "PST-8",  "PDT": "PST-8",
    "AST": "AST-4",  "ADT": "AST-4",
    "HST": "HST-10", "AKST": "AKST-9",
    "AKDT": "AKST-9","GMT": "GMT-0",
    "UNK": None,     "SST": "SST-11",
    "GST": "GST-10",
}

# ---------------------------------------------------------------------------
# Pomocnicze funkcje transformacji
# ---------------------------------------------------------------------------

def parse_damage(value) -> float:
    """Parsuje pola damage_*: '1.50K', '2.00M', '500B', '0', '', NaN → float USD."""
    if pd.isna(value) or str(value).strip() == "":
        return 0.0
    raw = str(value).strip().upper()
    multipliers = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    suffix = raw[-1]
    if suffix in multipliers:
        try:
            return float(raw[:-1]) * multipliers[suffix]
        except ValueError:
            return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def build_datetime(yearmonth, day, time_hhmm) -> Optional[pd.Timestamp]:
    """Składa pd.Timestamp z osobnych kolumn NOAA (yearmonth=202301, day=15, time=1430)."""
    try:
        ym  = str(int(yearmonth)).zfill(6)
        d   = str(int(day)).zfill(2)
        t   = str(int(time_hhmm)).zfill(4)
        return pd.Timestamp(f"{ym[:4]}-{ym[4:]}-{d} {t[:2]}:{t[2:]}:00")
    except Exception:
        return None


def normalize_event_type(raw) -> str:
    if pd.isna(raw):
        return "Unknown"
    return EVENT_TYPE_MAP.get(raw.strip().upper(), raw.strip().title())


def normalize_cz_type(val) -> Optional[str]:
    if pd.isna(val):
        return None
    v = str(val).strip().upper()
    return v if v in ("C", "Z") else None


def clean_wfo(val) -> Optional[str]:
    if pd.isna(val):
        return None
    v = str(val).strip().upper()
    return v if re.match(r"^[A-Z]{3}$", v) else None


def clean_magnitude(val) -> Optional[float]:
    try:
        f = float(val)
        return f if f >= 0 else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Główna transformacja
# ---------------------------------------------------------------------------

def transform_events(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transformacja storm events...")

    def col(name: str, fallback=None) -> pd.Series:
        aliases = {
            "time_zone": ["time_zone", "cz_timezone", "event_timezone"],
            "state":     ["state", "state_name"],
            "cz_fips":   ["cz_fips", "cz_fips_code"],
            "magnitude": ["magnitude", "tor_f_scale"],
        }
        for c in aliases.get(name, [name]):
            if c in df.columns:
                return df[c]
        log.warning(f"  Kolumna '{name}' nie istnieje w danych")
        val = fallback if fallback is not None else None
        return pd.Series([val] * len(df), index=df.index, dtype=object)

    out = pd.DataFrame()

    out["episode_id"]      = pd.to_numeric(col("episode_id"),  errors="coerce").astype("Int64")
    out["event_id"]        = pd.to_numeric(col("event_id"),    errors="coerce").astype("Int64")
    out["source_event_id"] = pd.to_numeric(col("event_id"),    errors="coerce").astype("Int64")

    out["state_fips"]  = pd.to_numeric(col("state_fips"), errors="coerce").astype("Int64")
    out["cz_fips"]     = pd.to_numeric(col("cz_fips"),    errors="coerce").astype("Int64")
    out["state_name"]  = col("state").str.strip().str.title()
    out["state_abbr"]  = out["state_name"].map(STATE_ABBR_MAP).fillna(
        col("state").str.strip().str.upper().str[:2]
    )
    out["cz_name"]     = col("cz_name").str.strip().str.title()
    out["cz_type"]     = col("cz_type").apply(normalize_cz_type)
    out["wfo"]         = col("wfo").apply(clean_wfo)
    out["time_zone"]   = col("time_zone").map(
        lambda x: TIMEZONE_MAP.get(str(x).strip().upper()) if pd.notna(x) else None
    )

    log.info("  Składanie kolumn datetime...")
    out["begin_datetime"] = [
        build_datetime(ym, d, t)
        for ym, d, t in zip(col("begin_yearmonth"), col("begin_day"), col("begin_time"))
    ]
    out["end_datetime"] = [
        build_datetime(ym, d, t)
        for ym, d, t in zip(col("end_yearmonth"), col("end_day"), col("end_time"))
    ]
    out["begin_date"] = pd.to_datetime(out["begin_datetime"]).dt.date
    out["end_date"]   = pd.to_datetime(out["end_datetime"]).dt.date
    out["begin_time"] = pd.to_datetime(out["begin_datetime"]).dt.time
    out["end_time"]   = pd.to_datetime(out["end_datetime"]).dt.time

    out["duration_minutes"] = (
        (pd.to_datetime(out["end_datetime"]) - pd.to_datetime(out["begin_datetime"]))
        .dt.total_seconds().div(60).round().astype("Int64")
    )
    out.loc[out["duration_minutes"] < 0, "duration_minutes"] = pd.NA

    out["event_type"]  = col("event_type").apply(normalize_event_type)
    out["flood_cause"] = col("flood_cause").str.strip().str.title()
    out["source"]      = col("source").str.strip()

    for c in ["injuries_direct", "injuries_indirect", "deaths_direct", "deaths_indirect"]:
        out[c] = pd.to_numeric(col(c), errors="coerce").fillna(0).astype(int).clip(lower=0)

    log.info("  Parsowanie damage_property i damage_crops...")
    out["damage_property"] = col("damage_property").apply(parse_damage)
    out["damage_crops"]    = col("damage_crops").apply(parse_damage)

    out["magnitude"]      = col("magnitude").apply(clean_magnitude)
    out["magnitude_type"] = col("magnitude_type").str.strip().str.upper()

    out["source_year"]      = col("source_year")
    out["source_load_date"] = pd.Timestamp("today").normalize()

    return out


def enrich_with_demographics(
    df_events: pd.DataFrame,
    df_area:   pd.DataFrame,
    df_pop:    pd.DataFrame,
) -> pd.DataFrame:
    """Dołącza powierzchnię i populację stanu, oblicza population_density."""
    log.info("Wzbogacanie o dane demograficzne...")

    df_events["year"] = pd.to_datetime(df_events["begin_datetime"]).dt.year.astype("Int64")

    df = df_events.merge(df_area[["state_fips", "area_sq_mi"]], on="state_fips", how="left")

    pop_years = df_pop["year"].dropna().unique()
    pop_min, pop_max = int(pop_years.min()), int(pop_years.max())
    df["_year_clamped"] = df["year"].clip(lower=pop_min, upper=pop_max)

    df = df.merge(
        df_pop[["state_fips", "year", "population"]],
        left_on=["state_fips", "_year_clamped"],
        right_on=["state_fips", "year"],
        how="left",
        suffixes=("", "_pop"),
    ).drop(columns=["_year_clamped", "year_pop"], errors="ignore")

    df["population_density"] = np.where(
        (df["area_sq_mi"] > 0) & df["population"].notna(),
        (df["population"] / df["area_sq_mi"]).round(4),
        np.nan,
    )

    missing = df["population_density"].isna().sum()
    if missing:
        log.warning(f"  Brak danych demograficznych dla {missing:,} zdarzeń")

    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Usuwa duplikaty event_id, zachowuje rekord z najnowszego source_year."""
    before = len(df)
    df = (
        df.sort_values("source_year", ascending=False)
          .drop_duplicates(subset=["event_id"], keep="first")
          .sort_values(["begin_datetime", "event_id"])
          .reset_index(drop=True)
    )
    removed = before - len(df)
    if removed:
        log.info(f"Deduplikacja: usunięto {removed:,} duplikatów event_id")
    return df


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Oddziela wiersze z krytycznymi brakami (brak event_id lub begin_date)."""
    log.info("Walidacja danych...")

    mask = df["event_id"].isna() | df["begin_date"].isna()

    log.warning(f"  Brak event_id:    {df['event_id'].isna().sum():,} → rejects")
    log.warning(f"  Brak begin_date:  {df['begin_date'].isna().sum():,} → rejects")

    neg_prop = (df["damage_property"] < 0).sum()
    neg_crop = (df["damage_crops"]    < 0).sum()
    if neg_prop or neg_crop:
        log.warning(f"  Ujemne damage_property: {neg_prop}, damage_crops: {neg_crop}")

    long_dur = (df["duration_minutes"] > 43_200).sum()
    if long_dur:
        log.warning(f"  duration > 30 dni: {long_dur:,} wierszy — sprawdź ręcznie")

    clean   = df[~mask].copy().reset_index(drop=True)
    rejects = df[mask].copy().reset_index(drop=True)
    log.info(f"Dane czyste: {len(clean):,} | odrzucone: {len(rejects):,}")
    return clean, rejects


# ---------------------------------------------------------------------------
# Budowanie ramek stagingowych
# ---------------------------------------------------------------------------

def build_staging_frames(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Zwraca słownik {nazwa_tabeli: DataFrame} gotowy do załadowania do bazy.
    Odpowiada tabelom stg_* zdefiniowanym w noaa_dw_schema.sql.
    """

    dates = pd.concat([
        pd.to_datetime(df["begin_date"].dropna()).rename("full_date"),
        pd.to_datetime(df["end_date"].dropna()).rename("full_date"),
    ]).drop_duplicates().sort_values()

    dim_time = pd.DataFrame({"full_date": dates})
    dim_time["year"]         = dim_time["full_date"].dt.year
    dim_time["quarter"]      = dim_time["full_date"].dt.quarter
    dim_time["month"]        = dim_time["full_date"].dt.month
    dim_time["month_name"]   = dim_time["full_date"].dt.strftime("%B")
    dim_time["week_of_year"] = dim_time["full_date"].dt.isocalendar().week.astype(int)
    dim_time["year_week"]    = (
        dim_time["full_date"].dt.isocalendar().year.astype(str)
        + dim_time["full_date"].dt.isocalendar().week.astype(str).str.zfill(2)
    ).astype(int)
    dim_time["day"]          = dim_time["full_date"].dt.day
    dim_time["day_of_week"]  = dim_time["full_date"].dt.isocalendar().day.astype(int)
    dim_time["day_name"]     = dim_time["full_date"].dt.strftime("%A")
    dim_time["is_weekend"]   = (dim_time["day_of_week"] >= 6).astype(int)
    dim_time["season"]       = dim_time["month"].map({
        12: "Winter", 1: "Winter",  2: "Winter",
        3:  "Spring",  4: "Spring", 5: "Spring",
        6:  "Summer",  7: "Summer", 8: "Summer",
        9:  "Fall",   10: "Fall",  11: "Fall",
    })

    staging = {
        "stg_dim_time": dim_time,

        "stg_dim_location": (
            df[["state_fips", "cz_fips", "state_abbr", "state_name", "cz_name", "cz_type", "time_zone"]]
            .drop_duplicates(subset=["state_fips", "cz_fips"])
            .reset_index(drop=True)
        ),

        "stg_dim_event_type": (
            df[["event_type"]].dropna().drop_duplicates().reset_index(drop=True)
        ),

        "stg_dim_flood_cause": (
            df[["flood_cause"]].dropna().drop_duplicates().reset_index(drop=True)
        ),

        "stg_dim_source": (
            df[["source"]].dropna()
            .rename(columns={"source": "source_name"})
            .drop_duplicates().reset_index(drop=True)
        ),

        "stg_dim_wfo": (
            df[["wfo"]].dropna()
            .rename(columns={"wfo": "wfo_code"})
            .drop_duplicates().reset_index(drop=True)
        ),

        "stg_dim_magnitude_type": (
            df[["magnitude_type"]].dropna().drop_duplicates().reset_index(drop=True)
        ),

        "stg_fact_event": df[[c for c in [
            "episode_id", "event_id", "source_event_id",
            "begin_date", "end_date", "begin_time", "end_time", "duration_minutes",
            "state_fips", "cz_fips", "source", "event_type", "flood_cause",
            "wfo", "magnitude_type", "magnitude",
            "injuries_direct", "injuries_indirect",
            "deaths_direct", "deaths_indirect",
            "damage_property", "damage_crops",
            "population", "population_density",
            "source_load_date",
        ] if c in df.columns]],
    }

    return staging

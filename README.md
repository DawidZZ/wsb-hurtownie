# NOAA Storm Events ETL

## Wymagania

- **Docker Desktop** (Windows/macOS) lub **Docker Engine + Compose plugin** (Linux)
- **Python 3.10+**
- Dane NOAA w `data/storm_events/` (pliki CSV o nazwie `StormEvents_details-ftp_v1.0_dYYYY_*.csv`)

---

## 1. Konfiguracja środowiska

### Plik `.env`

Plik `.env` jest już dołączony z domyślnymi wartościami:

```env
SA_PASSWORD=Hurtownie2026!
MSSQL_PID=Express
SQL_SERVER_HOST=mssql
SQL_SERVER_PORT=1433
SQL_SERVER_DB=hurtownie
CHECK_SCHEMA_HOST=localhost
```

> `SQL_SERVER_HOST=mssql` jest używany wewnątrz Docker network.  
> Skrypty Python uruchamiane na hoście łączą się przez `localhost:1433`.

---

### Wirtualne środowisko Python

**Linux / macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Windows (cmd):**
```cmd
python -m venv venv
venv\Scripts\activate.bat
pip install -r requirements.txt
```

**Windows (PowerShell):**
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## 2. Uruchomienie bazy danych

W terminalu, w folderze projektu:

```bash
docker compose up -d
```

Kontener `mssql-init` automatycznie tworzy bazę danych i inicjalizuje schemat (`noaa_dw_schema.sql`). Poczekaj aż zdrowie kontenera przejdzie w `healthy` (~30–60 s):

```bash
docker compose ps
```

Sprawdzenie połączenia (opcjonalne):
```bash
python test_connection.py
```

---

## 3. Uruchomienie ETL

### Pełny load (wszystkie lata, tryb delta):
```bash
python noaa_etl_cleaning.py
```

### Wybrany zakres lat:
```bash
python noaa_etl_cleaning.py --years 2010-2015
```

### Full refresh (truncate + reload, szybszy):
```bash
python noaa_etl_cleaning.py --full-refresh
python noaa_etl_cleaning.py --full-refresh --years 2010-2020
```

### Opcje diagnostyczne:
```bash
# Tylko CSV → staging (pomija load do hurtowni)
python noaa_etl_cleaning.py --csv-only

# Pomija load do hurtowni (tylko transform)
python noaa_etl_cleaning.py --skip-load
```

### Weryfikacja danych po załadowaniu:
```bash
python check_schema.py
python check_data.py
```

---

## Tryby ładowania

| Tryb | Kiedy używać |
|------|---|
| delta (domyślny) | Doładowanie nowych danych bez utraty istniejących |
| `--full-refresh` | Czysty reload — szybszy, usuwa wszystkie dane i ładuje od nowa |

---

## 4. Dane testowe

W katalogu `data/test/` znajdują się dwa małe pliki CSV do weryfikacji poprawności pipeline'u bez potrzeby ładowania pełnych danych produkcyjnych (~50 MB / rok).

| Plik | Rok | Zawartość |
|------|-----|-----------|
| `StormEvents_details-ftp_v1.0_d2029_c20260101.csv` | 2029 | 10 zdarzeń (różne typy, stany, wartości uszkodzeń) + 1 duplikat event_id do testu deduplikacji |
| `StormEvents_details-ftp_v1.0_d2028_c20260101.csv` | 2028 | 5 nowych zdarzeń + 3 rekordy z event_id identycznymi jak w 2029 (do testu delta) |

### Test podstawowy (transformacja + load):

```bash
python noaa_etl_cleaning.py --data-dir data/test --full-refresh
```

Oczekiwany wynik: `10 zdarzeń` w `fact_event` (duplikat odfiltrowany przez deduplikację).

### Test delta (dwa kroki):

```bash
# Krok 1: załaduj rok 2029 jako bazę
python noaa_etl_cleaning.py --data-dir data/test --full-refresh --years 2029-2029

# Krok 2: delta-load rok 2028
python noaa_etl_cleaning.py --data-dir data/test --years 2028-2028
```

Oczekiwane logi po kroku 2:
```
▶ fact_event przed delta:           10 wierszy
▶ fact_event po delta:              15 wierszy
✔ DELTA:        +5 nowych wierszy  (10 już istniało → pominięto)
```

### Tylko transformacja (bez DB):

```bash
python noaa_etl_cleaning.py --data-dir data/test --skip-load
```

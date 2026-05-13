# CLAUDE.md — evo_contratos / contracts-timeseries

## Project Overview

ETL pipeline that builds a **monthly time-series table of rental contracts** for the AssetPlan BI platform. It reads raw contract history from the production MySQL database, transforms it via SQL, and loads the result into the BI schema.

- **Source table**: `assetplan_rentas.contrato_arriendos`
- **Target table**: `bi_assetplan.etl_biDimContractTimeSeries`
- **Load strategy**: full replace on every run (`if_exists='replace'`)

---

## Repository Structure

```
contracts-timeseries/
├── main.py                   # Entry point: extract → load
├── utils.py                  # Engine class, db_config(), load_sql_query(), logger
├── check.py                  # Stub — not yet implemented
├── queries/
│   ├── contracts_ts.sql      # v1 — original transformation logic (multi-CTE SQL)
│   └── contracts_ts_v2.sql   # v2 — revised pipeline with overlap resolution and deduplication
├── .env                      # DB credentials (not committed)
├── requirements.txt          # Python dependencies
└── CLAUDE.md                 # This file
```

---

## How to Run

```bash
# 1. Activate virtual environment
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Unix

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure .env (see section below)

# 4. Run
python main.py
```

---

## Environment Variables (.env)

```
DATABASE_HOST=...
DATABASE_NAME=bi_assetplan
DATABASE_USERNAME=...
DATABASE_PASSWORD=...
```

The `db_config()` function in `utils.py` loads these. Passwords are URL-encoded automatically to handle special characters.

Optional prefix support: `db_config(type="PROD")` reads `PROD_DATABASE_*` vars.

---

## Key Design Decisions

- **Batch insert**: rows are uploaded in chunks of 10,000 using `method='multi'` for performance.
- **Full replace**: `main.py` uses `if_exists='replace'`, which drops and recreates the table on every run. `Engine.upload_data()` has a separate `prod` flag that uses `DELETE` + `append` instead — use that for safer production loads.
- **SQL-first transformation**: all business logic lives in `contracts_ts.sql`, not in Python. Python only orchestrates execution.
- **Gap records**: the SQL generates synthetic `period_id=0` rows (labeled `NOCONTRACT`) for periods between contracts. Real contract rows use `period_id=1`.

---

## SQL Query Structure

### v1 — `contracts_ts.sql`

Multi-CTE pipeline:

| CTE | Purpose |
|---|---|
| `fechas` | Cleans invalid `fecha_retiro_efectiva` values (`0000-00-01`, NULL) |
| `contratos` | Normalizes dates to month-first, computes `fin_contrato_anterior` via LAG |
| `tabla` | Calculates gap (in months) between consecutive contracts |
| `final` | Selects contract rows with `period_id=1` |
| `gaps` | Generates no-contract rows for gaps > 0 months |
| `juntar` | UNIONs `final` + `gaps` |
| `completo` | Fills end dates for gap rows using LEAD |
| `fecha_lag` | Recomputes `fin_contrato_anterior` on combined dataset |
| `duracion` | Calculates `duracion_periodo` in months |

**Filters applied in `contratos`:**
- `YEAR(fecha_inicio) > 2016`
- `monto_arriendo >= 150000`
- `pais_id = 1` (Chile only)
- All key fields must be non-null

---

### v2 — `contracts_ts_v2.sql`

Revised pipeline that adds deduplication, a 4-case end-date resolver, and overlap correction before generating gap rows.

| CTE | Purpose |
|---|---|
| `filas` | Deduplicates source rows — keeps latest contract per `property_id` via `ROW_NUMBER()` ordered by `fecha_inicio DESC, fecha_fin DESC, id DESC`. Also filters `deleted_at IS NULL` and `pais_id = 1` here. |
| `contratos` | Resolves `fecha_retiro_efectiva`: replaces zero-date with `fecha_fin`; for the last active contract with no retiro, extends to `CURDATE() + 1 month`. |
| `fechas` | Caps start dates: contracts that began before 2017 but ended in 2017+ have `fecha_inicio` reset to `2017-01-01`. Applies `monto_arriendo >= 150000` filter. |
| `termino_contrato` | Computes `fecha_termino` with 4 explicit cases: (1) normal renewal — same `contract_id`, later start → close at next start; (2) duplicate start date — use `fecha_fin`; (3) different renter, same property — close at next tenant's start; (4) last contract — use `fecha_fin`. |
| `ajuste_inicio` | Overlap resolution — if the previous contract's `fecha_termino` ≥ current `fecha_inicio`, pushes `fecha_inicio` forward to eliminate crossing dates. |
| `termino_anterior` | Computes `fin_contrato_anterior` via LAG; defaults to `2017-01-01` for the first contract of each property. |
| `date_diffs` | Calculates `diff_cont` (gap in months to previous contract) and `duracion` (contract length in months); sets `period_id = 1`. |
| `final` | Selects real contract rows. |
| `gaps` | Generates `period_id = 0` / `NOCONTRACT` rows for gaps > 0 months. |
| `juntar` | UNIONs `final` + `gaps`, ordered by `property_id, fecha_inicio`. Final SELECT adds `duracion_periodo` computed from `fecha_retiro_efectiva`. |

**Key differences from v1:**
- Adds `filas` deduplication step at the source.
- `deleted_at IS NULL` filter moved to the earliest CTE.
- `termino_contrato` replaces the single LAG-based end-date with a 4-case LEAD/LAG resolver.
- `ajuste_inicio` eliminates overlapping date ranges before gap generation.
- Removes `completo`, `fecha_lag`, and `duracion` CTEs from v1; their logic is absorbed into earlier stages.
- `YEAR(fecha_inicio) > 2016` filter applied in `termino_contrato` (was in `contratos` in v1).

---

## Utils Reference

### `Engine` class
- `Engine()` — connects using `.env` credentials
- `engine.execute(query)` → `pd.DataFrame`
- `engine.upload_data(df, database, table_name)` — transactional upload with rollback on error
- `engine.close()` — disposes connection pool

### `load_sql_query(filename, folder="queries")`
Reads a `.sql` file from the `queries/` folder and returns it as a string.

### `logger`
Standard Python `logging` at `INFO` level. Use `logger.info()` / `logger.error()` for output.

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| pandas | 2.3.1 | DataFrame handling |
| numpy | 1.26.4 | Numeric support |
| SQLAlchemy | 2.0.43 | DB engine abstraction |
| mysql-connector-python | 9.4.0 | MySQL driver |
| python-dotenv | 1.1.1 | `.env` loading |
| tqdm | 4.67.1 | Progress bars (available, not yet used in main flow) |

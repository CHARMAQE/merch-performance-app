# Data Engineering Pipeline

This folder contains the Python data engineering workflow for the Merch Performance project.

It is responsible for extracting Excel data, transforming it, loading MySQL tables, building the normalized analytics layer, and running validation rules.

## Current Flow

Main command from the repository root opens the desktop launcher:

```bash
python data-engineering/main.py
```

Date-filtered command example:

```bash
python data-engineering/main.py --file "C:\path\to\export.xlsx" --start-date 2026-05-01 --end-date 2026-05-03 --skip-validation
```

Local Excel command examples:

```bash
python data-engineering/main.py --file "C:\path\to\export.xlsx" --skip-validation
python data-engineering/main.py --file "C:\path\to\export.xlsx" --run-validation
```

Portal command example:

```bash
python data-engineering/main.py --portal
```

Coverage portal command example:

```bash
python data-engineering/main.py --coverage-portal
```

Command-line flags are still available for direct advanced runs.

When `--portal` is used, the downloaded Data Dump file is loaded without an automatic date filter. Use `--start-date` and `--end-date` only when you intentionally want to restrict the load.

The run does this:

1. Ask for the source type:
   - local Data Dump Excel file
   - automatic Data Dump portal download
   - local Coverage Excel file
   - local Coverage Excel file
2. Optionally filter rows by visit date.
3. Read the Excel file into a pandas dataframe.
4. Build base table dataframes:
   - employees
   - stores
   - products
   - visits
5. Load base tables into MySQL.
6. Detect task rows and map them to dynamic `task_*` tables.
7. Create or alter dynamic task tables as needed.
8. Load task responses.
9. Build `survey_responses`.
10. Load `survey_responses`.
11. Run database validation rules when validation mode is enabled.

## Folder Structure

```text
data-engineering/
├── config/
│   ├── db_config.py
│   └── env_loader.py
├── extract/
│   └── portal_exporter.py
├── load/
│   ├── load_base_tables.py
│   ├── load_survey_responses.py
│   └── load_task_tables.py
├── transform/
│   ├── build_base_tables.py
│   ├── build_survey_responses.py
│   ├── build_task_tables.py
│   ├── etl_constants.py
│   ├── etl_excel_to_mysql.py
│   └── etl_helpers.py
├── validation/
│   ├── engine/
│   ├── rules/
│   └── validation_runner.py
├── .env
├── .env.example
├── main.py
└── README.md
```

## Configuration

Local secrets live in:

```text
data-engineering/.env
```

Create it from the example:

```bash
[ -f data-engineering/.env ] || cp data-engineering/.env.example data-engineering/.env
```

Required database values:

```env
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_DATABASE=unilever_db
MYSQL_USER=root
MYSQL_PASSWORD=Root@123
```

Portal download values:

```env
PORTAL_BASE_URL=https://smartmanagement.smollan.com/#/login
PORTAL_USERNAME=
PORTAL_PASSWORD=
PORTAL_ENTITY=Morocco Unilever
DATA_DUMP_REPORT_NAME=Data Dump
COVERAGE_REPORT_NAME=Coverage Data
COVERAGE_LOOKBACK_DAYS=3
DOWNLOAD_DIR=./data-engineering/downloads
BACKUP_DIR=./data-engineering/backups
LOG_DIR=./logs
PORTAL_HEADLESS=false
```

Do not commit `.env`. Older local `DB_*`, `PORTAL_USER`, `PORTAL_PASS`, and `UNILEVER_DOWNLOAD_DIR` names are still accepted for compatibility, but new setups should use the standardized names above.

## Important Files

### `main.py`

The project entrypoint.

When run without arguments, it opens the desktop launcher. When run with command-line arguments, it coordinates:

- source selection
- optional visit date filtering
- Excel reading
- core ETL
- survey response build/load
- validation run

### `extract/portal_exporter.py`

Uses Playwright to log into the Smollan portal and export Excel reports.

Supported report helpers:

- `download_excel_from_portal()` for Data Dump
- `download_coverage_from_portal()` for Coverage

If the portal report labels are different, update these values in `.env`:

```env
DATA_DUMP_REPORT_NAME=Data Dump
COVERAGE_REPORT_NAME=Coverage Data
COVERAGE_LOOKBACK_DAYS=3
```

If you use this path, install Playwright:

```bash
python -m pip install -r requirements.txt
python -m playwright install chromium
```

### `transform/build_base_tables.py`

Builds clean pandas dataframes for:

- `employees`
- `stores`
- `products`
- `visits`

### `transform/build_task_tables.py`

Maps Excel task/title values to dynamic task table names.

Examples:

- `LOCATION CHECK IN` -> `task_location_checkin`
- `CALLCYCLE DEVIATION` -> `task_callcycle_deviation`
- titles containing `OSA`, `COC`, `MH`, or `PACK` -> `task_osa_pack_coc_mh`

### `transform/etl_excel_to_mysql.py`

Coordinates the core ETL.

It loads:

- base tables
- visits
- dynamic task tables

### `load/load_task_tables.py`

Creates and updates dynamic `task_*` table structures.

It also pivots question/response rows into wide task-specific records.

### `transform/build_survey_responses.py`

Builds a normalized table shape from the Excel source.

Output columns include:

- `visit_id`
- `employee_code`
- `store_code`
- `product_code`
- `task`
- `title`
- `question`
- `response`
- `response_datetime`
- `latitude`
- `longitude`

### `validation/validation_runner.py`

Creates a validation run log, executes active validation rules, and records run status.

### `validation/rules/osa_unusual_non.py`

Current active rule:

```text
OSA_UNUSUAL_NON_BY_BANNER
```

This rule searches for suspicious `Non` OSA answers when weekly product/banner availability is mostly `Oui`.

## Database Tables Used

Base tables:

- `employees`
- `stores`
- `products`
- `visits`

Analytics table:

- `survey_responses`

Validation tables:

- `validation_results`
- `validation_run_log`

Dynamic task tables:

- created by the ETL
- dropped by `database/reset_data.sql`
- not created directly in `database/schema.sql`

## Running The Pipeline

From the repository root:

```bash
python data-engineering/main.py
```

For a local Excel file:

1. choose option `1`
2. paste the Excel file path

For portal download:

1. choose option `2`
2. make sure portal credentials exist in `.env`
3. make sure Playwright is installed

For the desktop popup:

```bash
python data-engineering/main.py
```

The desktop app separates:

- Daily Pipeline: Data Dump and Coverage, selected together or separately
- Monthly Masters: Store, User, Assortment, and Call Cycle file selection

The monthly master tab currently stages file selection only. The actual dimension loaders should be implemented with the future `dim_store_master`, `dim_user_master`, `dim_assortment_master`, and `dim_call_cycle_master` tables.

## Known Issues

- Use Python 3.12 for the most predictable local setup on Windows and macOS.
- `playwright` is listed in the root `requirements.txt`, but Chromium still needs a separate `python -m playwright install chromium` step.
- The ETL creates and alters task tables dynamically, so schema can change based on incoming questions.
- Current validation execution is code-driven, not database-rule-driven.
- There is no dedicated test suite yet for transformations or validation rules.

## How To Continue

Best next improvements:

1. Add unit tests for `clean_text`, `question_to_column`, and dataframe builders.
2. Add a small fixture Excel file for repeatable local testing.
3. Make validation rules easier to register.
4. Add a run log for manual ETL runs.
5. Decide whether to keep dynamic task tables or replace them with more normalized reporting tables.
6. Document each expected Excel column and what it maps to.

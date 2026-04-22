# Data Engineering Pipeline

## Overview

This folder contains the full data engineering workflow for the Merch Performance project.

Its responsibilities are:

- optionally download the latest Excel export from the Smollan portal
- read the Excel file once into memory
- transform the data into base tables and dynamic task tables
- load the transformed data into MySQL
- build and load the `survey_responses` analytical table
- run validation rules on the loaded data

## Current Flow

### Main run

Run:

```powershell
python .\data-engineering\main.py
```

The current flow is:

1. Choose a source:
   - local Excel file
   - automatic portal download
2. Read the Excel file once with `prepare_source_dataframe`
3. Run the core ETL for:
   - employees
   - stores
   - products
   - visits
   - dynamic task tables
4. Build `survey_responses`
5. Load `survey_responses`
6. Run validation rules

### Optional portal extraction

If you choose portal download in `main.py`, the project uses:

- `extract/portal_exporter.py`

This file logs into the portal, exports the Excel file, and stores it locally before the ETL continues.

## Folder Structure

```text
data-engineering/
├── .env
├── config/
│   ├── db_config.py
│   ├── env_loader.py
│   └── __init__.py
├── extract/
│   ├── portal_exporter.py
│   └── __init__.py
├── load/
│   ├── load_base_tables.py
│   ├── load_task_tables.py
│   ├── load_survey_responses.py
│   └── __init__.py
├── transform/
│   ├── build_base_tables.py
│   ├── build_task_tables.py
│   ├── build_survey_responses.py
│   ├── etl_constants.py
│   ├── etl_excel_to_mysql.py
│   ├── etl_helpers.py
│   └── __init__.py
├── validation/
│   ├── engine/
│   │   ├── validation_engine.py
│   │   └── __init__.py
│   ├── rules/
│   │   ├── osa_unusual_non.py
│   │   └── __init__.py
│   ├── validation_runner.py
│   └── __init__.py
├── main.py
└── README.md
```

## File Roles

### `transform/`

Contains transformation logic only.

Naming rule:
- `build_*.py` prepares pandas DataFrames

Important files:
- `build_base_tables.py`
- `build_task_tables.py`
- `build_survey_responses.py`
- `etl_excel_to_mysql.py` as the ETL coordinator

### `load/`

Contains MySQL load logic only.

Naming rule:
- `load_*.py` reads or writes database objects

Important files:
- `load_base_tables.py`
- `load_task_tables.py`
- `load_survey_responses.py`

### `validation/`

Contains validation orchestration and rule implementations.

Current active rule:
- `OSA_UNUSUAL_NON_BY_BANNER`

### `config/`

Contains configuration helpers.

Important files:
- `db_config.py` builds the MySQL config from environment variables
- `env_loader.py` loads values from `data-engineering/.env`

### `extract/`

Contains optional download automation for the Smollan portal.

Important file:
- `portal_exporter.py`

## Environment Setup

The project uses:

- `data-engineering/.env` for local secrets

Typical values stored there:

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- `PORTAL_USER`
- `PORTAL_PASS`
- `PORTAL_ENTITY`
- `UNILEVER_DOWNLOAD_DIR`

This file is local only and should not be committed to GitHub.

## Design Rules

The ETL now follows these rules:

- read the Excel source once, then reuse the dataframe
- keep transformation logic in `transform/`
- keep SQL logic in `load/`
- keep validation isolated in `validation/`

That separation makes the project easier to debug, refactor, and extend.

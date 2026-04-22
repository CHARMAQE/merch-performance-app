# Merch Performance App

## Overview

Merch Performance App is a workspace for loading, validating, and exposing Unilever retail execution data.

The repository currently contains three main parts:

- `data-engineering/` for Excel extraction, ETL, MySQL loading, and validation rules
- `backend/` for the Spring Boot API layer
- `frontend/` for the React user interface

The project goal is to transform exported field data into structured tables, build analytical layers such as `survey_responses`, and make the data available for validation, reporting, and future dashboard work.

## Repository Structure

```text
merch-performance-app/
├── backend/
├── data-engineering/
├── database/
├── docs/
├── frontend/
├── docker-compose.yml
└── requirements.txt
```

## Current Data Flow

1. Raw Excel data is exported from the Smollan portal manually or through `data-engineering/extract/portal_exporter.py`.
2. The ETL pipeline reads the Excel file once and prepares base tables and dynamic task tables.
3. The pipeline builds and loads the `survey_responses` analytical table.
4. Validation rules run on the loaded MySQL data.
5. The backend exposes selected data through REST endpoints.
6. The frontend consumes backend endpoints for simple dashboard views.

## Main Components

### `data-engineering/`

Contains the ETL and validation logic.

Main responsibilities:
- read Excel files
- load employees, stores, products, visits, and task tables into MySQL
- build and load `survey_responses`
- run validation rules

Main entrypoint:
- `python .\data-engineering\main.py`

### `backend/`

Spring Boot application connected to the same MySQL database.

Current backend responsibilities:
- expose employees through `/api/employees/`
- expose deviation summary data through `/api/reports/deviation-summary`

Default backend port from the current config:
- `9000`

### `frontend/`

React application that currently fetches employee data from the backend and renders a simple dashboard page.

Current frontend expectation:
- backend available at `http://localhost:9000`

## Environment Notes

The `data-engineering` module now uses:

- `data-engineering/.env` for local secrets
- `.gitignore` to keep that file out of GitHub

Typical secrets stored there:
- MySQL connection settings
- portal credentials for automatic Excel download

## Run Order

If you want to use the full local flow:

1. Start MySQL and make sure the target schema exists.
2. Run the data engineering pipeline.
3. Start the Spring Boot backend.
4. Start the React frontend.

## Status

The repository is now organized around a clear separation:

- `build_*` files prepare data
- `load_*` files write data to MySQL
- `main.py` and `etl_excel_to_mysql.py` orchestrate the ETL flow

This makes the ETL side much easier to maintain than the previous single-file structure.

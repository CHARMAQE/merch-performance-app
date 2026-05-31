# Merch Performance App

Merch Performance App is a full-stack PFE project for Smollan / Unilever retail execution follow-up.

The main company-facing deliverable is a mobile app for Unilever supervisors. It helps them follow stores, merchandisers, field visits, execution status, and anomalies directly from the field. Behind the mobile app, the project uses an Excel-to-MySQL ETL pipeline, validation rules, a Spring Boot API, a React validation dashboard, and future Power BI reporting.

## Project Status

This project is currently a strong academic prototype with a working end-to-end data pipeline, validation layer, backend API, mobile app, and first dashboard version.

The strongest part of the project is the data engineering and validation flow. The next priority is to make the mobile supervisor experience clearer, more professional, and better connected to the real Unilever field workflow.

## Main Architecture

```text
Excel export
  -> Python data engineering pipeline
  -> MySQL database
  -> validation rules
  -> Spring Boot backend API
  -> Expo mobile supervisor app
  -> React validation dashboard
  -> Power BI reporting
```

## Repository Structure

```text
merch-performance-app/
├── backend/              # Spring Boot API
├── data-engineering/     # Python ETL, portal export, validation
├── database/             # MySQL schema and helper SQL scripts
├── frontend/             # React frontend
├── mobile/               # Expo mobile app
├── requirements.txt      # Python dependencies
└── README.md
```

## Cross-Platform Setup

Use the setup guide when moving between Windows 11 and MacBook Air M1:

```text
SETUP.md
```

Standard local development versions:

- Python 3.12
- Node.js 20.x
- Java 17
- MySQL 8.4 through Docker Compose
- backend Maven wrapper from `backend/mvnw` or `backend/mvnw.cmd`

Power BI editing remains Windows-only.

## Main Components

### Data Engineering

Location:

```text
data-engineering/
```

Responsibilities:

- choose a local Excel file or download one from the Smollan portal
- read the Excel export with pandas
- build base tables for employees, stores, products, and visits
- create and load dynamic `task_*` tables
- build the normalized `survey_responses` table
- run validation rules and store results in MySQL

Main entrypoint:

```bash
python data-engineering/main.py
```

### Database

Location:

```text
database/
```

Important files:

- `schema.sql` creates the main MySQL schema
- `seed_supervisors.sql` creates demo supervisors and assigns stores by city
- `reset_data.sql` clears loaded data and drops dynamic task tables
- `manual_validation_queries.sql` contains useful manual queries for inspection
- `reporting_queries.sql` contains quick reporting queries for dashboard and Power BI checks

Main database name:

```text
unilever_db
```

Core tables:

- `employees`
- `stores`
- `products`
- `visits`
- `survey_responses`
- `validation_results`
- `validation_run_log`

Dynamic task tables are created by the ETL at runtime.

Examples:

- `task_location_checkin`
- `task_location_checkout`
- `task_callcycle_deviation`
- `task_osa_pack_coc_mh`
- `task_sos`

### Backend

Location:

```text
backend/
```

Stack:

- Java
- Spring Boot
- Spring JDBC
- MySQL

Default port:

```text
9000
```

Current API endpoints:

```text
GET /api/dashboard/overview
GET /api/dashboard/latest-issues
GET /api/map/stores
GET /api/map/stores/{storeCode}/details
```

### Frontend

Location:

```text
frontend/
```

Stack:

- React
- React Scripts

Current behavior:

- starts a local React development server
- calls the backend map and dashboard endpoints
- displays store locations and the current validation dashboard context

The frontend is now a first dashboard version focused on stores, validation, and anomaly context.

### Mobile App

Location:

```text
mobile/
```

Stack:

- Expo
- React Native

Main role:

- provide Unilever supervisors with a field-ready view of assigned stores and merchandisers
- replace or improve the manual daily report
- show daily execution indicators, store coverage, merchandiser activity, maps, and anomaly context

Current mobile screens:

- login
- dashboard overview
- merchandiser execution
- store map
- store details

## Local Setup

### 1. Create the MySQL Database

Open MySQL and run:

```sql
SOURCE database/schema.sql;
```

Or copy and run the SQL from:

```text
database/schema.sql
```

Important: see the known issues section before rerunning the schema many times.

### 2. Configure Python Environment

Create a local environment file:

```bash
cp data-engineering/.env.example data-engineering/.env
```

Then fill in your real local values.

Do not commit `data-engineering/.env`.

### 3. Install Python Dependencies

From the repository root:

```bash
pip install -r requirements.txt
```

If you use the portal downloader, Playwright is also required:

```bash
pip install playwright
playwright install chromium
```

### 4. Run the ETL

From the repository root:

```bash
python data-engineering/main.py
```

The command asks you to choose:

- local Excel file
- automatic portal download

If you choose a local Excel file, you can also choose:

- `History / backfill load`: loads the data but skips validation
- `Daily load`: loads the data and runs validation

Validation is now scoped to the visits coming from the uploaded file, while the database keeps the full month history.

### 5. Start the Backend

From the backend folder:

```bash
cd backend
./mvnw spring-boot:run
```

On Windows:

```powershell
cd backend
.\mvnw.cmd spring-boot:run
```

The backend should run at:

```text
http://localhost:9000
```

### 6. Start the Frontend

From the frontend folder:

```bash
cd frontend
npm install
npm start
```

The frontend usually runs at:

```text
http://localhost:3000
```

## Current Data Flow

1. `data-engineering/main.py` starts the run.
2. The user chooses a local Excel file or portal download.
3. `prepare_source_dataframe` reads and normalizes the Excel data.
4. `run_etl` builds and loads base tables.
5. The ETL creates and loads dynamic task tables.
6. The project fetches visit IDs from MySQL.
7. `build_survey_responses_dataframe` creates normalized survey rows.
8. `load_survey_responses` inserts those rows into MySQL.
9. `validation_runner.py` creates a validation run log.
10. The validation engine runs active validation rules.
11. Validation issues are inserted into `validation_results`.
12. Backend endpoints expose selected data.
13. The mobile app reads supervisor execution data.
14. The web dashboard reads validation and anomaly data.

## Current Validation Rules

The active validation rules are:

```text
OSA_UNUSUAL_NON_BY_BANNER
GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH
```

Purpose:

- `OSA_UNUSUAL_NON_BY_BANNER`
  - finds OSA responses marked `Non`
  - compares them against weekly product/banner availability patterns
  - flags suspicious `Non` answers when most other answers are `Oui`

- `GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH`
  - compares repeated monthly visits for the same merchandiser and store
  - detects GPS check-ins that are far from the normal monthly GPS cluster
  - flags suspicious visit locations

Result table:

```text
validation_results
```

Rule metadata table:

```text
validation_rules
```

Run log table:

```text
validation_run_log
```

## Known Issues

These are important before continuing development:

- `requirements.txt` still needs a small dependency cleanup and review for portal-export dependencies.
- Backend credentials now support environment variables, but the fallback defaults should still be hardened before deployment.
- The dashboard is a strong first version, but more KPI views and filters are still needed.

## How To Continue This Project

Recommended next steps:

1. Redesign the mobile supervisor dashboard around the field workflow.
2. Improve merchandiser and store execution screens.
3. Add more business validation rules.
4. Expand web dashboard KPIs and drill-down filters.
5. Add tests for ETL transformations and validation rules.
6. Add richer backend API tests instead of only context load.
7. Review Python dependencies and explicitly document portal-export requirements.
8. Decide whether dynamic task tables should remain dynamic or move toward controlled migrations.
9. Add Docker Compose only when you are ready to run MySQL, backend, frontend, and mobile backend services together.

## Development Notes

- The ETL currently reads an Excel source once and reuses the in-memory dataframe.
- Base table loading is separated from task table loading.
- Validation logic is separated from ETL logic.
- `survey_responses` is the best table for analytics and validation because it keeps responses normalized.
- `task_*` tables are useful for task-specific wide reporting, but their schema can change depending on incoming questions.

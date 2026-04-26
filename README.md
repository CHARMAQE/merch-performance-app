# Merch Performance App

Merch Performance App is a full-stack project for processing Unilever/Smollan retail execution data.

The project takes exported field execution data from Excel, loads it into MySQL, runs validation logic, exposes selected data through a Spring Boot API, and displays early dashboard data in a React frontend.

## Project Status

This project is currently a working development foundation, not a finished production app.

The strongest part of the project right now is the data engineering flow. The backend and frontend are still early, but they already connect to the same MySQL database and provide the beginning of a reporting/dashboard layer.

## Main Architecture

```text
Excel export
  -> Python data engineering pipeline
  -> MySQL database
  -> validation rules
  -> Spring Boot backend API
  -> React frontend dashboard
```

## Repository Structure

```text
merch-performance-app/
├── backend/              # Spring Boot API
├── data-engineering/     # Python ETL, portal export, validation
├── database/             # MySQL schema and helper SQL scripts
├── docs/                 # Review notes and diagrams
├── frontend/             # React frontend
├── docker-compose.yml    # Currently empty / not ready
├── requirements.txt      # Python dependencies
└── README.md
```

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
- `reset_data.sql` clears loaded data and drops dynamic task tables
- `Scripts.sql` contains useful manual queries for inspection
- `analytics.sql` is currently only a placeholder

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
- Spring Data JPA
- MySQL

Default port:

```text
9000
```

Current API endpoints:

```text
GET /api/employees/
GET /api/reports/deviation-summary
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
- calls `http://localhost:9000/api/employees/`
- displays a basic employee list

The frontend is still an early dashboard starting point.

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
13. Frontend reads backend data.

## Current Validation Rule

The active validation rule is:

```text
OSA_UNUSUAL_NON_BY_BANNER
```

Purpose:

- find OSA responses marked `Non`
- compare them against weekly product/banner availability patterns
- flag suspicious `Non` answers when most other answers are `Oui`

Result table:

```text
validation_results
```

Run log table:

```text
validation_run_log
```

## Known Issues

These are important before continuing development:

- `docker-compose.yml` is empty, so Docker startup is not ready.
- `requirements.txt` is currently UTF-16 encoded; normal Python tools usually expect UTF-8.
- `playwright` is used by the portal exporter but is not listed in `requirements.txt`.
- Backend database credentials are currently written directly in `backend/src/main/resources/application.properties`.
- `database/schema.sql` runs `ALTER TABLE validation_results` before creating `validation_results`; rerunning it can fail.
- `CREATE TABLE validation_results` does not use `IF NOT EXISTS`.
- `docs/data-engineering-end-to-end-review.md` mentions orchestration files that no longer exist as source `.py` files.
- The frontend is still minimal and only displays employees.

## How To Continue This Project

Recommended next steps:

1. Fix `database/schema.sql` so `validation_results` is created safely.
2. Convert `requirements.txt` to UTF-8.
3. Add `playwright` to Python dependencies if portal download is required.
4. Move backend database credentials into environment variables.
5. Expand backend endpoints for validation and dashboard reporting.
6. Build frontend dashboard pages around validation results and deviation summaries.
7. Add tests for ETL transformations.
8. Add backend API tests.
9. Decide whether dynamic task tables should remain dynamic or move toward controlled migrations.
10. Fill `docker-compose.yml` only when you are ready to run MySQL/backend/frontend through Docker.

## Development Notes

- The ETL currently reads an Excel source once and reuses the in-memory dataframe.
- Base table loading is separated from task table loading.
- Validation logic is separated from ETL logic.
- `survey_responses` is the best table for analytics and validation because it keeps responses normalized.
- `task_*` tables are useful for task-specific wide reporting, but their schema can change depending on incoming questions.

# Data Engineering End-to-End Review

## Scope

This review covers the Python pipeline under `data-engineering/` and the SQL schema files under `database/`.

The goal is to document:

- the current end-to-end flow
- what is already working well
- the main structural and operational risks
- a better target organization for the project
- a phased roadmap to get there safely

## Current Flow

Today the pipeline is organized around this flow:

1. `extract/portal_exporter.py`
   - Uses Playwright to log into the Smollan portal and export an Excel file.
   - Saves downloaded files into the inbound directory.

2. `orchestration/pipeline_daily.py`
   - Runs the extraction step.
   - Then runs the ETL daily runner.

3. `orchestration/etl_daily_runner.py`
   - Picks the latest Excel file from the inbound folder.
   - Computes the file hash.
   - Writes ETL run metadata to `etl_run_log`.
   - Skips already loaded files using `etl_file_registry`.
   - Runs the main ETL.
   - Builds `survey_responses`.
   - Loads `survey_responses`.
   - Archives the file into success or failed folders.

4. `transform/etl_excel_to_mysql.py`
   - Reads the Excel file.
   - Loads dimension-style tables: `employees`, `stores`, `products`.
   - Loads `visits`.
   - Dynamically creates and alters task tables.
   - Pivots task answers into wide task-specific tables.

5. `transform/build_survey_responses.py`
   - Re-reads the same Excel file.
   - Builds a normalized dataframe for `survey_responses`.
   - Matches visit IDs by querying `visits`.

6. `load/load_survey_responses.py`
   - Deletes all rows in `survey_responses`.
   - Inserts the rebuilt dataframe row by row.

7. `validation/validation_runner.py`
   - Creates a validation run log entry.
   - Executes rules from `validation/engine/validation_engine.py`.
   - Stores issues in `validation_results`.

## What Is Already Good

- The pipeline is split into recognizable stages: extract, transform, load, orchestration, validation.
- There is already a useful run-log idea for ETL and validation.
- The file registry with SHA-256 is a good foundation for idempotency.
- The `survey_responses` layer is a strong idea because it gives the validation layer a normalized source instead of depending only on wide task tables.
- The validation layer is already separated from the ETL logic at a folder level.

## Main Findings

### 1. Configuration is hardcoded and environment-specific

Examples:

- `data-engineering/config/db_config.py`
- `data-engineering/config/paths.py`
- `data-engineering/run_pipeline.ps1`

Current issues:

- Database credentials are committed in code.
- Portal credentials are committed in a script.
- Default file paths point to one machine and even to a different operating system layout.

Impact:

- Hard to run on another laptop, server, or container.
- High security risk.
- Hard to promote from local development to staging or production.

### 2. `survey_responses` is rebuilt destructively

Example:

- `data-engineering/load/load_survey_responses.py`

Current issue:

- The loader executes `DELETE FROM survey_responses` before inserting the current dataframe.

Impact:

- A daily run for one file wipes the historical normalized dataset.
- Validation logic that depends on weekly patterns can become incorrect because history disappears.
- The ETL file registry says a file was successfully loaded, but the normalized layer no longer reflects all successful files.

### 3. ETL logic is mixed with schema migration logic

Example:

- `data-engineering/transform/etl_excel_to_mysql.py`

Current issues:

- The ETL creates tables dynamically.
- The ETL adds columns dynamically from question text.
- Foreign keys are created during runtime.

Impact:

- The database schema changes based on incoming data.
- Reproducibility is weak across environments.
- Deployments become risky because schema evolution is not controlled.
- Analytics and BI usage become harder because task tables drift over time.

### 4. The same file is parsed more than once and some mappings are recomputed inefficiently

Examples:

- `transform/etl_excel_to_mysql.py`
- `transform/build_survey_responses.py`

Current issues:

- The Excel file is read once for the base ETL and again for `survey_responses`.
- `build_survey_responses.py` queries all visits and maps row-by-row using `df.apply`.

Impact:

- Extra runtime and memory use.
- More room for mismatches between stages.
- Harder to reason about one pipeline run as a single consistent transformation.

### 5. Orchestration is duplicated across entrypoints

Examples:

- `data-engineering/main.py`
- `data-engineering/orchestration/etl_daily_runner.py`
- `data-engineering/orchestration/pipeline_daily.py`

Current issues:

- There are multiple entrypoints for similar steps.
- One flow is interactive and one is scheduled.
- The same business pipeline exists in several places.

Impact:

- Logic can drift between manual runs and automated runs.
- Bug fixes must be repeated in more than one entrypoint.

### 6. Validation is not yet rule-driven

Examples:

- `data-engineering/validation/engine/validation_engine.py`
- `database/validation_tables.sql`

Current issues:

- Rules are manually imported and manually called.
- The `validation_rules` table exists, but execution is not controlled by it.

Impact:

- Scaling from one rule to many rules becomes painful.
- Turning rules on or off requires code changes.
- Rule metadata lives partly in SQL and partly in Python with no strong link.

### 7. Testing and packaging are still missing

Current issues:

- No dedicated `tests/` package was found for the data-engineering module.
- The Python project does not yet have a package layout such as `src/...` or a `pyproject.toml`.

Impact:

- Refactoring is risky.
- CI/CD will be harder to introduce.
- It is difficult to validate business rules safely over time.

## Recommended Target Organization

The best next organization is not just "more folders". It is a clearer separation of concerns.

Recommended target shape:

```text
data-engineering/
|
+-- pyproject.toml
+-- README.md
+-- .env.example
+-- src/
|   +-- merch_pipeline/
|       +-- cli/
|       |   +-- run_manual.py
|       |   +-- run_daily.py
|       |
|       +-- config/
|       |   +-- settings.py
|       |   +-- logging.py
|       |
|       +-- db/
|       |   +-- connection.py
|       |   +-- repositories/
|       |
|       +-- extract/
|       |   +-- portal_exporter.py
|       |
|       +-- ingest/
|       |   +-- file_registry.py
|       |   +-- landing.py
|       |
|       +-- transform/
|       |   +-- staging.py
|       |   +-- dimensions.py
|       |   +-- visits.py
|       |   +-- survey_responses.py
|       |   +-- task_tables.py
|       |
|       +-- load/
|       |   +-- base_loader.py
|       |   +-- survey_loader.py
|       |
|       +-- validation/
|       |   +-- runner.py
|       |   +-- registry.py
|       |   +-- rules/
|       |
|       +-- pipelines/
|       |   +-- daily_pipeline.py
|       |   +-- manual_pipeline.py
|       |
|       +-- shared/
|           +-- dataframe_utils.py
|           +-- text_utils.py
|
+-- tests/
|   +-- unit/
|   +-- integration/
|   +-- fixtures/
|
+-- sql/
|   +-- migrations/
|   +-- ddl/
|   +-- queries/
|
+-- scripts/
    +-- run_pipeline.ps1
```

## Target Data Layers

To keep the pipeline maintainable, I recommend a three-layer data model:

### 1. Landing / Bronze

Purpose:

- Preserve what came from the source.
- Track file-level lineage and ingestion status.

Examples:

- inbound files
- file registry
- optional raw-sheet snapshots or row counts

### 2. Core / Silver

Purpose:

- Hold normalized, trusted operational data.

Examples:

- `employees`
- `stores`
- `products`
- `visits`
- `survey_responses`

This is where most validation rules should read from.

### 3. Business / Gold

Purpose:

- Expose business-facing outputs and quality findings.

Examples:

- task marts if still required by reporting
- weekly OSA aggregates
- validation result tables
- KPI-ready analytics views

## The Best End-to-End Organization for This Project

If this project continues to grow, the cleanest long-term boundary is:

- `extract` for automation only
- `ingest` for file detection, hashing, registry, archive movement
- `transform` for dataframe and SQL shaping only
- `load` for inserts, upserts, and transaction boundaries
- `validation` for rule definitions and execution
- `pipelines` for composing full workflows
- `sql/migrations` for schema ownership
- `tests` for business and regression protection

In other words:

- ETL should not create schema on the fly.
- loaders should not decide business rules.
- validation should not depend on interactive scripts.
- configuration should not live inside business code.

## Highest-Priority Improvements

### Priority 1: Safety and reproducibility

1. Move all secrets and paths to environment variables.
2. Remove committed credentials from code and scripts.
3. Add a single `settings.py` module that validates required configuration on startup.
4. Create `.env.example` and document every required variable.

### Priority 2: Fix data correctness

1. Stop deleting all rows from `survey_responses`.
2. Replace full-table delete with one of these patterns:
   - delete only rows for the affected `visit_id`s, then reinsert
   - or use an idempotent upsert strategy keyed by a natural business key
3. Make the validation layer read from a complete historical normalized layer.

### Priority 3: Separate schema management from ETL

1. Move DDL into versioned SQL migrations.
2. Keep ETL responsible for data only, not schema evolution.
3. Decide whether task tables are truly required as physical tables or should become views / marts.

### Priority 4: Unify orchestration

1. Keep one pipeline service that receives:
   - source file path
   - run mode: manual or daily
   - options like `full_refresh`
2. Make `main.py` and the scheduled runner thin wrappers around the same pipeline service.

### Priority 5: Make validation scalable

1. Add a rule registry in Python.
2. Let each rule expose:
   - `rule_code`
   - `is_applicable()`
   - `run(run_id)`
3. Optionally use the `validation_rules` table to enable or disable rules without code edits.

### Priority 6: Add tests before larger refactors

Start with:

- unit tests for `clean_text`, `clean_float`, and `question_to_column`
- unit tests for task-to-table routing
- unit tests for banner normalization
- integration tests for one small Excel fixture through ETL
- regression tests for validation rules

## Concrete Refactor Plan

### Phase 1: Quick wins

- Centralize settings and secrets.
- Stop destructive reload of `survey_responses`.
- Introduce structured logging.
- Add a `tests/` folder and a few critical tests.

### Phase 2: Internal cleanup

- Extract repository functions for employees, stores, products, visits.
- Split `etl_excel_to_mysql.py` into smaller modules.
- Make one pipeline service used by both manual and daily runs.

### Phase 3: Platform readiness

- Add `pyproject.toml`.
- Add migrations.
- Add CI to run linting and tests.
- Add data quality metrics and run summaries.

### Phase 4: Analytics readiness

- Decide which tables are operational core, which are marts, and which are validation outputs.
- Move reusable weekly aggregates into views or dedicated marts.
- Add documentation for downstream consumers.

## Suggested Immediate Next Actions

If only a small amount of time is available, I would do these three first:

1. Replace hardcoded config with environment-based settings.
2. Fix `survey_responses` loading so history is not deleted.
3. Break `etl_excel_to_mysql.py` into smaller data loaders and move schema DDL out of runtime code.

Those three changes will improve security, correctness, and maintainability the fastest.

## Bottom Line

This project already has the right business idea:

- extract operational data
- build a normalized analytical layer
- run validation logic on top

The main improvement needed now is architectural discipline:

- configuration outside code
- schema outside ETL runtime
- one orchestration path
- idempotent loads
- test coverage for business rules

Once those are in place, the project will be much easier to run, explain, extend, and trust.

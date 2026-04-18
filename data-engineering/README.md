# Data Engineering Pipeline

## Overview

This module contains the full **data engineering pipeline** of the Merch Performance project.

It is responsible for:
- extracting data from the Smollan portal
- transforming raw Excel data
- loading structured data into MySQL
- building an analytical base (`survey_responses`)
- running validation rules (OSA validation layer)

---

## Architecture

```text
Portal (Smollan)
      ↓
Playwright Extraction
      ↓
Excel Files (Inbound)
      ↓
ETL (Transform + Load)
      ↓
MySQL Database
      ↓
survey_responses Layer
      ↓
Validation Engine


data-engineering/
│
├── config/
│   ├── db_config.py        # Database connection
│   └── paths.py            # File paths (inbound, archive...)
│
├── extract/
│   └── portal_exporter.py  # Playwright automation for data export
│
├── transform/
│   ├── etl_excel_to_mysql.py   # Main ETL logic
│   ├── etl_helpers.py          # Helper functions
│   ├── etl_constants.py        # Constants
│   └── build_survey_responses.py # Build normalized table
│
├── load/
│   └── load_survey_responses.py # Insert into survey_responses
│
├── orchestration/
│   ├── pipeline_daily.py       # Full pipeline runner
│   └── etl_daily_runner.py     # ETL execution logic
│
├── validation/
│   ├── validation_runner.py
│   ├── engine/
│   │   └── validation_engine.py
│   └── rules/
│       ├── osa_unusual_non.py
│       └── osa_contradictory_same_visit.py
│
└── run_pipeline.ps1            # PowerShell script to run full pipeline
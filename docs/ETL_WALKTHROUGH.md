# ETL Walkthrough

This document explains the current ETL pipeline in simple project language.

## What ETL Means In This Project

ETL means:

```text
Extract -> Transform -> Load
```

In this project:

```text
Extract: get the Excel file from the computer or from the Smollan portal.
Transform: clean the raw Excel rows and build structured dataframes.
Load: insert or update the MySQL tables.
```

## Main Entrypoint

File:

```text
data-engineering/main.py
```

This file controls the full run:

1. Read command-line options.
2. Choose the Excel source:
   - local Excel file
   - automatic portal download
3. Choose the load mode:
   - history/backfill load without validation
   - daily load with validation
4. Read the Excel file once.
5. Optionally filter rows by visit date.
6. Run the core ETL.
7. Build and load `survey_responses`.
8. Run validation rules if enabled.

## Source Excel Preparation

File:

```text
data-engineering/transform/build_base_tables.py
```

Function:

```text
prepare_source_dataframe()
```

What it does:

1. Reads the Excel file with pandas.
2. Converts column names to lowercase.
3. Builds a real `date` column from `dateid`.
4. Converts `responsedate` from Excel serial format to datetime.

Important raw columns include:

```text
dateid
employeeCode
Username
StoreCode
StoreName
StoreCity
StoreState
StoreRegion
StoreFormat
ProductCode
ProductBarCode
ProductDescription
Task
Title
Question
Response
responseDate
latitude
longitude
```

## Base Tables

The ETL creates four base dataframes:

```text
employees
stores
products
visits
```

### employees

Built from:

```text
employeeCode
Username
```

Business key:

```text
employee_code
```

### stores

Built from:

```text
StoreCode
StoreName
StoreCity
StoreState
StoreRegion
StoreFormat
```

Business key:

```text
store_code
```

### products

Built from:

```text
ProductCode
ProductBarCode
ProductDescription
BrandName
Category
subCategory
```

Business key:

```text
product_code
```

### visits

A visit means:

```text
one merchandiser + one store + one date
```

Natural key:

```text
visit_date + employee_code + store_code
```

This is important because the Excel file has many rows for the same visit. Each row can represent one question/response.

## Core ETL

File:

```text
data-engineering/transform/etl_excel_to_mysql.py
```

Function:

```text
run_etl()
```

What it does:

1. Builds employees, stores, products, and visits dataframes.
2. Connects to MySQL.
3. Loads base tables.
4. Builds dynamic task table batches.
5. Loads dynamic `task_*` tables.
6. Returns the affected visit IDs.

The affected visit IDs are later used so validation can focus on the uploaded daily data.

## Dynamic Task Tables

Files:

```text
data-engineering/transform/build_task_tables.py
data-engineering/load/load_task_tables.py
```

The Excel file contains different task types. The ETL maps them to dynamic tables.

Examples:

```text
LOCATION CHECK IN -> task_location_checkin
LOCATION CHECK OUT -> task_location_checkout
CALLCYCLE DEVIATION -> task_callcycle_deviation
OSA / COC / MH / PACK -> task_osa_pack_coc_mh
SOS -> task_sos
```

The loader creates or updates these tables automatically.

For most task tables, question names become database columns. This is why the ETL is dynamic: if the portal adds new questions, the database table can be altered.

## Normalized Analytics Table

File:

```text
data-engineering/transform/build_survey_responses.py
```

Output table:

```text
survey_responses
```

This table keeps the data in a simple normalized shape:

```text
visit_id
employee_code
store_code
product_code
task
title
question
response
response_datetime
latitude
longitude
```

This table is very important for:

1. validation rules
2. web dashboard
3. Power BI
4. future analytics

## Load Strategy

File:

```text
data-engineering/load/load_base_tables.py
```

The base tables use upsert logic:

```text
INSERT ... ON DUPLICATE KEY UPDATE
```

This means:

1. if a store/product/employee already exists, it is updated;
2. if it does not exist, it is inserted.

For visits, the natural key prevents duplicate visits:

```text
visit_date + employee_id + store_id
```

Before reloading task details for affected visits, old payload rows are deleted. This avoids duplicate task responses when the same daily file is loaded again.

## Validation Step

Files:

```text
data-engineering/validation/validation_runner.py
data-engineering/validation/engine/validation_engine.py
data-engineering/validation/engine/registry.py
data-engineering/validation/rules/
```

After the ETL, validation can run automatically.

Current active rules:

```text
OSA_UNUSUAL_NON_BY_BANNER
GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH
```

Validation results are stored in:

```text
validation_run_log
validation_rules
validation_results
```

## Current ETL Summary For The Report

The report should describe the ETL as a pipeline that transforms raw portal Excel data into a structured MySQL model used by validation, backend APIs, web dashboard, mobile supervisor app, and Power BI.

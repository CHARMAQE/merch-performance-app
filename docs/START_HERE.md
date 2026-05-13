# Start Here - PFE Work Plan

This file is the working roadmap for the Merch Performance App PFE.

The project is not only a mobile app. It is a complete data platform:

```text
Excel portal export
-> Python ETL
-> MySQL database
-> validation and anomaly rules
-> Spring Boot backend
-> React web validation dashboard
-> Expo mobile supervisor app
-> Power BI reporting
```

## 1. Start With The Data Foundation

Before improving the web or mobile app, understand and stabilize the data.

Your first work block is:

1. Read the Excel export structure.
2. Understand what one row means.
3. Confirm the business keys:
   - `StoreCode` identifies a store.
   - `employeeCode` identifies a merchandiser.
   - one visit is one employee visiting one store on one date.
4. Confirm the MySQL tables:
   - `employees`
   - `stores`
   - `products`
   - `visits`
   - `survey_responses`
   - `validation_rules`
   - `validation_run_log`
   - `validation_results`

Output for the report:

```text
Chapter 3: Source des donnees et regles de gestion
Chapter 4: Conception de la base de donnees
```

## 2. Validate The ETL Pipeline

The ETL is the part that transforms Excel files into database tables.

Main command:

```bash
python data-engineering/main.py
```

What to verify:

1. The Excel file is read correctly.
2. Base tables are generated.
3. Dynamic `task_*` tables are generated.
4. `survey_responses` is filled.
5. The load does not duplicate visits.

Output for the report:

```text
Chapter 5: Pipeline ETL
```

## 3. Work On Validation Rules

Validation is the core business value of the project.

Existing rules include:

```text
OSA_UNUSUAL_NON_BY_BANNER
GPS inconsistent check-in for the same store/month
```

Useful next rules:

1. Missing required response.
2. Unknown store code.
3. Unknown product code.
4. Duplicate visit.
5. Store visited by wrong merchandiser.
6. GPS far from expected store location.
7. Suspicious OSA answers.

Output for the report:

```text
Chapter 3: Besoins fonctionnels
Chapter 5: Regles de validation
Chapter 6: Tests et resultats
```

## 4. Connect Backend And Web Platform

The backend exposes validated data to the frontend and mobile app.

Existing backend stack:

```text
Java Spring Boot
Spring JDBC
MySQL
```

Existing endpoint groups:

```text
/api/dashboard
/api/map
/api/mobile
```

Frontend role:

```text
React dashboard for stores, validation, and anomalies
```

Output for the report:

```text
Chapter 4: Architecture applicative
Chapter 5: Realisation backend et frontend
```

## 5. Build The Mobile Supervisor App

The mobile app is for Unilever supervisors.

Its goal is to replace or improve the daily report.

Main expected screens:

1. Login.
2. Daily overview.
3. Merchandiser execution.
4. Store map.
5. Store details and anomalies.

Output for the report:

```text
Chapter 5: Application mobile
```

## 6. Build Power BI Dashboards

Power BI should tell the business story.

Recommended dashboards:

1. Execution overview.
2. Store coverage.
3. Merchandiser performance.
4. OSA availability.
5. Validation anomalies.
6. Regional comparison.

Output for the report:

```text
Chapter 5: Business Intelligence
Chapter 6: Resultats obtenus
```

## 7. Daily Working Method

Every work session should follow this pattern:

1. Choose one small task.
2. Make the app/database/report change.
3. Test or inspect the result.
4. Write what changed in the report.
5. Update the project checklist.

Example:

```text
Task: Add a duplicate visit validation rule.
App work: implement SQL/Python rule.
Test: run validation on sample Excel file.
Report work: document the rule and add result screenshot/table.
```

## Current Priority

Start here:

```text
Priority 1: Validate the database and ETL pipeline.
Priority 2: Document the source Excel structure and database design.
Priority 3: Strengthen validation rules.
Priority 4: Improve web validation dashboard.
Priority 5: Improve mobile supervisor app.
Priority 6: Build Power BI dashboards.
```

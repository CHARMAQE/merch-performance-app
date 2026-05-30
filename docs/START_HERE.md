# Start Here - PFE Work Plan

This file is the working roadmap for the Merch Performance App PFE.

The company-facing need is the mobile app for Unilever supervisors. Technically, the project is a complete data platform that feeds this mobile app with reliable execution data:

```text
Excel portal export
-> Python ETL
-> MySQL database
-> validation and anomaly rules
-> Spring Boot backend
-> Expo mobile supervisor app
-> React web validation dashboard
-> Power BI reporting
```

## 1. Start With The Mobile Business Need

Before touching code, keep the business problem clear:

```text
Unilever supervisors work in the field and need a mobile tool to see stores, merchandisers, visits, execution state, and anomalies without waiting for a manual daily report.
```

The mobile app must answer simple operational questions:

1. Which stores are assigned or visited?
2. Which merchandisers executed their work?
3. Which stores have anomalies or missing execution?
4. Where are the stores located?
5. What should the supervisor check first today?

Output for the report:

```text
Chapter 1: Problematique
Chapter 3: Besoins fonctionnels de l'application mobile
Chapter 5: Realisation de l'application mobile
```

## 2. Stabilize The Data Foundation

The mobile app is only useful if the data behind it is clean. So the first technical foundation is the Excel-to-MySQL pipeline.

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

## 3. Validate The ETL Pipeline

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

## 4. Work On Validation Rules

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

## 5. Connect Backend To The Mobile App

The backend exposes validated data to the mobile app first, then to the web dashboard and Power BI.

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

Mobile role:

```text
Expo app for Unilever supervisors: stores, merchandisers, execution, map, anomalies
```

Output for the report:

```text
Chapter 4: Architecture applicative
Chapter 5: Realisation backend et application mobile
```

## 6. Build The Mobile Supervisor App

The mobile app is the central operational deliverable for Unilever supervisors.

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

## 7. Improve The Web Validation Dashboard

The web dashboard is useful for internal control, data validation, and anomaly analysis. It supports the mobile app but does not replace it.

Expected web views:

1. Validation issues.
2. Store execution analysis.
3. Merchandiser performance.
4. OSA quality checks.

Output for the report:

```text
Chapter 5: Plateforme web de validation
```

## 8. Build Power BI Dashboards

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

## 9. Daily Working Method

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
Priority 1: Define the mobile supervisor workflow clearly.
Priority 2: Redesign the mobile dashboard and screens.
Priority 3: Validate the database and ETL pipeline that feeds the app.
Priority 4: Document the source Excel structure and database design.
Priority 5: Strengthen validation rules used by the app and web dashboard.
Priority 6: Improve web validation dashboard.
Priority 7: Build Power BI dashboards.
```

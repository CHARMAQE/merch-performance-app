# Requirements Design - Smollan Unilever Merch Performance Platform

Prepared for: PFE Big Data and IoT  
Student: CHARMAQE Hamza  
School: ENSAM Casablanca  
Company context: Smollan / Unilever Morocco  
Target readiness date: 2026-06-20

## 1. Project Objective

The objective of this project is to build a data-driven platform for monitoring merchandiser execution for the Unilever project.

The platform must transform raw field execution data into operational and analytical information for three different uses:

- Mobile app: daily supervisor execution follow-up.
- Web app: analytics, validation, monitoring, and administration.
- Power BI: management reporting and PFE analytical storytelling.

The project is not only an application. It is a complete data system composed of:

- Python ETL pipeline.
- MySQL analytical database.
- Spring Boot REST API.
- React web dashboard.
- Expo mobile application.
- OSA prediction MVP.
- Power BI reporting layer.

## 2. Data Source Understanding

The requirements are based on the Unilever raw Excel dump used for the project.

Example file analyzed:

`UL_Data_Dump-28-29-APR.xlsx`

The file contains one main sheet with these fields:

- Year
- Month
- Dateid
- date
- employeeCode
- Username
- StoreRegion
- StoreState
- StoreCity
- StoreFormat
- StoreCode
- StoreName
- Task
- Title
- subCategory
- ActivityName
- Category
- BrandName
- ProductCode
- ProductBarCode
- ProductDescription
- Question
- Response
- responseDate
- latitude
- longitude

Observed sample statistics for April 28-29:

| Metric | Value |
|---|---:|
| Raw response rows | 57,065 |
| Unique visits | 550 |
| Active merchandisers | 112 |
| Distinct visited stores | 461 |
| Distinct products | 252 |
| Task types | 16 |
| Questions | 91 |
| OSA response rows | 43,883 |
| OSA Yes rate | 53.95% |

Daily execution sample:

| Day | Active merchandisers | Visited stores | Field visits | Revisited stores |
|---|---:|---:|---:|---:|
| 2026-04-28 | 88 | 254 | 258 | 4 |
| 2026-04-29 | 95 | 286 | 292 | 6 |

## 3. Important Data Rules

### Store Identity

The unique business key for stores is `StoreCode`.

`StoreName` must not be used as the unique identifier because different stores can have the same or similar names.

Technical keys:

- Database primary key: `stores.store_id`
- Business key: `stores.store_code`
- Raw file key: `StoreCode`

### Merchandiser Identity

The unique business key for merchandisers is `employeeCode`.

Technical keys:

- Database primary key: `employees.employee_id`
- Business key: `employees.employee_code`
- Raw file key: `employeeCode`

### Visit Grain

A visit is defined as one merchandiser visiting one store on one date.

Current natural key:

`visit_date + employee_id + store_id`

This means if the same employee has multiple response rows for one store on one date, those rows belong to one visit.

### Response Grain

Each row in the raw Excel file is a survey response or captured answer.

Several response rows can belong to the same visit.

### OSA Question

OSA availability can be derived from the question:

`EST CE QUE LE SKU CI-DESSOUS EST DISPONIBLE?`

Typical values:

- `Oui`: product available.
- `Non`: product unavailable.

### Response Date Limitation

The analyzed file contains `responseDate`, but it appears as an Excel serial date without reliable activity time.

For this reason, the platform should not yet rely on:

- first activity time
- last activity time
- visit duration

These can be added later only if a reliable timestamp source is confirmed.

## 4. User Personas

### Supervisor

The supervisor needs a mobile tool to replace or improve the daily report.

Main questions:

- Are my merchandisers working today?
- Which stores were visited?
- Which stores were not visited?
- Which merchandiser visited which store?
- Which stores were visited more than once by different merchandisers?
- Are there execution or GPS problems?
- What is the situation by city, zone, store format, or store?

### Admin / Data Analyst

The admin or analyst needs to monitor all data, validate quality, and understand execution globally.

Main questions:

- Is the uploaded data complete?
- Are there data quality issues?
- Which regions, stores, products, or merchandisers have problems?
- What is the OSA situation?
- Which anomalies should be corrected?

### Manager / Business Stakeholder

The manager needs summarized performance views and decision support.

Main questions:

- How is field execution performing?
- Which regions or categories are weak?
- Which products have OSA risks?
- What are the priorities for action?

## 5. Mobile App Requirements

The mobile app must be operational and simple. It should focus on daily supervisor follow-up, not deep validation.

### Mobile Screen 1 - Login

Purpose:

Allow supervisors and admin users to access the mobile application.

Required features:

- Username/password login.
- Supervisor role.
- Admin role.
- Logout button.
- Clear error message if credentials are wrong.

MVP status:

Required.

### Mobile Screen 2 - Daily Overview

Purpose:

Give the supervisor a fast execution summary for the selected day or month.

Recommended KPIs:

| KPI | Definition |
|---|---|
| Stores | Distinct stores visited in the selected period |
| Field Visits | Total visits executed in the selected period |
| Merchandisers | Distinct merchandisers active in the selected period |
| Stores Revisited | Stores visited by more than one different merchandiser |

Optional supporting KPIs:

| KPI | Definition |
|---|---|
| Portfolio Stores | Stores assigned to the supervisor |
| Coverage Rate | Visited stores / portfolio stores |
| Not Visited Stores | Portfolio stores not visited in the selected period |

MVP status:

Required.

### Mobile Screen 3 - Merchandiser Execution

Purpose:

Show supervisor what each merchandiser did.

Recommended information:

- Employee code.
- Username.
- Number of visits.
- Number of distinct stores visited.
- Cities covered.
- Main tasks executed.
- Possible low-activity flag.

Example supervisor questions answered:

- Who worked today?
- Who has low visit count?
- Which merchandiser covered which stores?

MVP status:

High priority after the dashboard.

### Mobile Screen 4 - Store Coverage

Purpose:

Show visited and not visited stores.

Recommended filters:

- Date.
- City.
- Store format.
- Visited status.
- Search by store code or store name.

Recommended information:

- Store code.
- Store name.
- City.
- Format.
- Visit status.
- Merchandiser assigned/executed.

MVP status:

High priority.

### Mobile Screen 5 - Store Details

Purpose:

Allow supervisor to open a store and understand execution details.

Recommended information:

- Store code.
- Store name.
- Region/city/format.
- Visit date.
- Merchandiser.
- Tasks executed.
- OSA summary.
- GPS location.
- Revisited flag.

MVP status:

High priority.

### Mobile Screen 6 - Alerts

Purpose:

Show execution issues that need supervisor attention.

Recommended alert types:

- Store revisited by different merchandisers.
- Missing GPS coordinates.
- Possible callcycle deviation.
- Store not visited.
- Low merchandiser activity.

MVP status:

Important, but can be implemented progressively.

### Mobile Screen 7 - Map

Purpose:

Visualize store execution geographically.

Required features:

- Supervisor sees only assigned stores.
- Admin sees all stores.
- Store markers.
- Search store.
- Store details from marker.

MVP status:

Required.

## 6. Web App Requirements

The web app is the analytical and validation platform. It should contain deeper views than the mobile app.

### Web Page 1 - Login

Purpose:

Control access to the web platform.

MVP status:

Required.

### Web Page 2 - Executive Dashboard

Purpose:

Show global platform KPIs.

Recommended KPIs:

- Active employees.
- Visited stores.
- Products captured.
- Visits.
- Responses.
- Latest validation run.
- Validation issues by severity.
- Validation issues by rule.

MVP status:

Required.

### Web Page 3 - Validation Center

Purpose:

Show data quality and execution anomalies.

Recommended features:

- Latest validation run summary.
- Issue list.
- Filter by rule.
- Filter by severity.
- Filter by store code.
- Issue details.

Example validation topics:

- OSA suspicious patterns.
- GPS inconsistencies.
- duplicate or repeated execution.
- callcycle deviation.

MVP status:

Required.

### Web Page 4 - Store Map

Purpose:

Allow analysis of all stores and execution geography.

Recommended features:

- All stores on map.
- Search by store code/name.
- Open store details.
- Show latest execution.
- Show store-level OSA summary.

MVP status:

Required.

### Web Page 5 - Merchandiser Monitoring

Purpose:

Analyze merchandiser productivity and execution.

Recommended KPIs:

- Visits per merchandiser.
- Stores covered per merchandiser.
- Cities covered.
- OSA responses captured.
- GPS quality.
- Revisited store involvement.

MVP status:

High priority.

### Web Page 6 - Store Monitoring

Purpose:

Analyze execution by store.

Recommended KPIs:

- Visit count.
- Last visit date.
- Merchandisers who visited the store.
- OSA rate.
- Main unavailable products.
- Validation issues.

MVP status:

High priority.

### Web Page 7 - OSA Analytics

Purpose:

Analyze product availability.

Recommended views:

- OSA rate by category.
- OSA rate by brand.
- OSA rate by product.
- OSA rate by region/city/store format.
- Top unavailable products.
- Stores with weak OSA.

MVP status:

Required for Big Data/Data Analyst value.

### Web Page 8 - OSA Prediction MVP

Purpose:

Show predictive value by identifying products or stores at risk of OSA problems.

Recommended output:

- Product/store risk score.
- Predicted unavailable probability.
- Main explanatory features.
- Priority action list.

MVP status:

Required for PFE differentiation.

## 7. Power BI Requirements

Power BI should be used as the management reporting layer.

### Page 1 - Executive Summary

Recommended visuals:

- Total visits.
- Active merchandisers.
- Visited stores.
- OSA rate.
- Validation issues.
- Trends by date.

### Page 2 - Regional Performance

Recommended visuals:

- KPIs by region.
- KPIs by city.
- KPIs by store format.
- Map or region/city table.

### Page 3 - Merchandiser Performance

Recommended visuals:

- Visits by merchandiser.
- Stores covered.
- Productivity ranking.
- Low-activity detection.

### Page 4 - Store Coverage

Recommended visuals:

- Visited stores.
- Not visited stores.
- Revisited stores.
- Coverage by city/format.

### Page 5 - OSA Analysis

Recommended visuals:

- OSA rate by category.
- OSA rate by product.
- OSA rate by brand.
- Top unavailable products.

### Page 6 - Data Quality

Recommended visuals:

- Validation issues by rule.
- Validation issues by severity.
- Issues by store/category.
- Trend of issues after each upload.

### Page 7 - Prediction Insights

Recommended visuals:

- OSA risk score.
- Priority products.
- Priority stores.
- Risk by region/category.

## 8. KPI Dictionary

### Stores

Business meaning:

Distinct stores visited in the selected period.

SQL logic:

`COUNT(DISTINCT v.store_id)`

Important note:

This KPI is different from supervisor portfolio stores.

### Field Visits

Business meaning:

Total visit executions in the selected period.

SQL logic:

`COUNT(*) FROM visits`

Visit definition:

One employee + one store + one date.

### Merchandisers

Business meaning:

Distinct merchandisers who executed at least one visit in the selected period.

SQL logic:

`COUNT(DISTINCT v.employee_id)`

### Stores Revisited

Business meaning:

Stores visited by more than one different merchandiser in the selected period.

SQL logic:

Group by `store_code` and keep stores where:

`COUNT(DISTINCT employee_code) > 1`

### Portfolio Stores

Business meaning:

Stores assigned to a supervisor.

SQL logic:

`COUNT(DISTINCT supervisor_stores.store_id)`

### Coverage Rate

Business meaning:

Percentage of assigned stores visited in the selected period.

Formula:

`Visited Stores / Portfolio Stores`

### OSA Rate

Business meaning:

Percentage of product availability responses equal to `Oui`.

Formula:

`OSA Yes Responses / Total OSA Responses`

### Validation Issues

Business meaning:

Data or execution anomalies detected by validation rules.

Source table:

`validation_results`

## 9. MVP Prioritization

### Must Have Before Final PFE Demo

- Stable mobile dashboard.
- Mobile store map.
- Mobile merchandiser execution list.
- Web executive dashboard.
- Web validation center.
- Web OSA analytics.
- Power BI executive/reporting dashboard.
- OSA prediction MVP.
- KPI dictionary and data dictionary.
- Final demo script.
- Report chapters for architecture, data pipeline, analytics, and prediction.

### Important If Time Allows

- Docker deployment.
- More advanced authentication with JWT.
- Better admin user management.
- More advanced ML model.
- Real-time or near-real-time pipeline.

### Later Perspective

- Cloud deployment.
- Automated CI/CD.
- Real-time streaming pipeline.
- Advanced explainable AI.
- Mobile push notifications.

## 10. Roadmap Until 2026-06-20

| Period | Objective |
|---|---|
| 2026-05-11 to 2026-05-15 | Requirements, KPI dictionary, mobile dashboard stabilization |
| 2026-05-16 to 2026-05-22 | Authentication, API documentation, ETL hardening |
| 2026-05-23 to 2026-05-30 | Mobile store/merch execution and web validation dashboard |
| 2026-05-31 to 2026-06-05 | Web OSA analytics, merch/store monitoring, Power BI first version |
| 2026-06-06 to 2026-06-10 | OSA prediction MVP |
| 2026-06-11 to 2026-06-15 | Testing, screenshots, report architecture and data chapters |
| 2026-06-16 to 2026-06-20 | Final report, slides, demo script, rehearsal |

## 11. Current Design Decision

The current mobile application is not considered complete.

The current dashboard cards are a first foundation:

- Stores
- Field Visits
- Merchandisers
- Stores Revisited

The next product design step is to add supervisor-oriented operational views:

- Merchandiser execution.
- Store coverage.
- Store details.
- Alerts.

The web application and Power BI must carry the deeper analytics and validation workload.

## 12. PFE Positioning

This project can be presented as:

> A data-driven platform for transforming raw field execution data into operational monitoring, data quality validation, OSA analytics, and predictive decision support for merchandiser performance management.

This positioning connects the project to:

- Big Data engineering.
- Data warehousing and analytical modeling.
- Backend API design.
- Mobile and web application development.
- Business intelligence.
- Machine learning prediction.
- Data quality and validation.


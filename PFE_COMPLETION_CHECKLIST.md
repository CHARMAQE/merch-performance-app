# PFE Completion Checklist - Merch Performance Analytics Platform

Prepared for: CHARMAQE Hamza  
Program: Master Big Data et Internet des Objets - ENSAM Casablanca  
Host company: Smollan  
Project context: Unilever Morocco retail execution / merchandiser performance  
Planning date: 2026-05-11  
Target readiness date: 2026-06-20  

## Project Positioning

This PFE must not be presented as only a mobile application. The professional positioning is:

> End-to-end data analytics platform for monitoring merchandiser field performance, including ETL processing, MySQL data modeling, Spring Boot APIs, mobile operational reporting, web validation dashboards, and an OSA prediction module.

The strongest story is:

- Data engineering: transform raw Excel/portal exports into clean analytical tables.
- Data quality: detect suspicious OSA and GPS issues.
- Analytics: produce KPIs for supervisors and admin.
- Software engineering: expose data through Spring Boot REST APIs.
- Decision support: mobile app for daily field monitoring and web app for deeper analysis.

## Status Legend

| Status | Meaning |
| --- | --- |
| Not started | Nothing implemented yet |
| In progress | Started but not stable |
| To verify | Implemented but needs testing and screenshots |
| Done | Ready for report and demo |
| Risk | Must be handled quickly |

## Official School Requirements Checklist

| Requirement | Expected Deliverable | Status | Deadline |
| --- | --- | --- | --- |
| Official cover page | Use `Page de garde_BDIoT.docx` and fill student, title, company, jury, date | Not started | 2026-06-07 |
| Company signature | Signed report page or approval from company responsible | Not started | 2026-06-12 |
| Trilingual abstracts | Arabic, French, English, max 100 words each | Not started | 2026-06-07 |
| Keywords | 5 to 8 relevant keywords | Not started | 2026-06-07 |
| Table of contents | Paginated, 1 to 2 pages | Not started | 2026-06-05 |
| Introduction generale | Max 4 pages | Not started | 2026-05-30 |
| Background / theoretical context | 5 to 10 pages | Not started | 2026-05-31 |
| Problem statement and difficulties | 5 to 10 pages | Not started | 2026-06-01 |
| Possible solutions and retained choice | 2 to 3 pages | Not started | 2026-06-01 |
| Adopted solution and algorithms | 5 to 10 pages | Not started | 2026-06-02 |
| Realization / implementation | 2 to 5 pages | Not started | 2026-06-03 |
| Chronogram | 1 to 2 pages | Not started | 2026-06-04 |
| Conclusion and perspectives | 1 to 2 pages | Not started | 2026-06-06 |
| Bibliography | 1 to 3 pages, clean format | Not started | 2026-06-06 |
| Annexes | Screenshots, SQL examples, API examples, extra diagrams | Not started | 2026-06-10 |
| Formatting | Times New Roman, margins 2.5 cm, justified text, captions | Not started | 2026-06-08 |

## Report Formatting Rules To Respect

- Level 1 title: Times New Roman, 14 pt, bold.
- Level 2 title: Times New Roman, 12 pt, bold.
- Body text: Times New Roman, 12 pt.
- Line spacing: single.
- Paragraph spacing: 6 pt before and after.
- Margins: 2.5 cm.
- First-line indent: 1.25 cm.
- Alignment: justified.
- Header/footer: Times New Roman, 9 pt.
- Tables and figures centered horizontally.
- Captions below figures/tables.
- Captions: Times New Roman, 9 pt, italic.
- Avoid "je"; use formal academic style.

## Proposed PFE Title Options

| Option | Title |
| --- | --- |
| French - recommended | Conception et realisation d'une plateforme analytique de suivi de la performance des merchandisers pour le projet Unilever |
| French - data focused | Mise en place d'une solution Big Data pour le suivi, la validation et l'analyse des donnees d'execution terrain |
| English | Design and Implementation of a Data Analytics Platform for Merchandiser Field Performance Monitoring |

Decision needed by: 2026-05-13.

## High-Level Architecture To Present

```text
Excel / Portal Export
        |
        v
Python ETL Pipeline
        |
        v
MySQL Analytical Database
        |
        v
Spring Boot REST API
        |
        +------------------+
        |                  |
        v                  v
React Web Dashboard   Expo Mobile App
        |
        v
Validation & Analytics
```

## Technical Work Packages

### WP1 - Data Engineering Pipeline

| Task | Why It Matters | Status | Deadline |
| --- | --- | --- | --- |
| Document Excel column mapping | Jury must understand how raw data becomes database tables | In progress | 2026-05-14 |
| Document cleaning logic | Show Big Data/Data Analyst value: trim, date parsing, duplicate handling | In progress | 2026-05-14 |
| Explain StoreCode business key | StoreName is not unique; StoreCode identifies stores | Done | 2026-05-14 |
| Add ETL run evidence | Screenshot/log showing rows loaded, stores, visits, responses | Not started | 2026-05-16 |
| Add small test dataset | Repeatable demo without full production Excel | Not started | 2026-05-18 |
| Add transformation tests | Test `clean_text`, store duplicate logic, visit natural key | Not started | 2026-05-20 |
| Document dynamic task tables | Explain why `task_*` tables are generated dynamically | Not started | 2026-05-21 |
| Document validation run flow | Explain `validation_run_log` and `validation_results` | Not started | 2026-05-22 |

### WP2 - Database And Data Model

| Task | Why It Matters | Status | Deadline |
| --- | --- | --- | --- |
| Finalize schema explanation | Needed for report chapter and jury questions | In progress | 2026-05-17 |
| Add ER diagram | Visual proof of modeling work | Not started | 2026-05-18 |
| Add KPI definition SQL file | Avoid confusion between rows, visits, stores, StoreCode | In progress | 2026-05-18 |
| Add role column or document demo role logic | Professional authentication story | Not started | 2026-05-21 |
| Review indexes | Show performance awareness for analytics queries | Not started | 2026-05-22 |
| Create database reset/load guide | Repeatable demo preparation | Not started | 2026-05-23 |

### WP3 - Spring Boot Backend

| Task | Why It Matters | Status | Deadline |
| --- | --- | --- | --- |
| Clean mobile API names | Professional API design | In progress | 2026-05-17 |
| Improve login from demo to professional | Master Spring Boot skill: BCrypt/JWT/roles | Not started | 2026-05-24 |
| Add role-based access | Admin sees all, supervisor sees assigned scope | In progress | 2026-05-24 |
| Add validation filtering endpoint | Web needs date/rule/severity filters | Not started | 2026-05-26 |
| Add merch monitoring endpoint | Web dashboard needs merchandiser performance table | Not started | 2026-05-27 |
| Add OSA analytics endpoint | Needed for prediction module and OSA dashboard | Not started | 2026-05-29 |
| Add backend tests | Jury-ready quality signal | Not started | 2026-05-31 |
| Add API documentation | Report annex and demo guide | Not started | 2026-06-02 |

### WP4 - Mobile Application

| Task | Why It Matters | Status | Deadline |
| --- | --- | --- | --- |
| Finalize login/logout | Account switching for admin/supervisor demo | In progress | 2026-05-14 |
| Finalize dashboard KPIs | Mobile replaces daily report | In progress | 2026-05-15 |
| Confirm KPI definitions | Stores, field visits, merchandisers, revisited stores | In progress | 2026-05-15 |
| Polish dashboard UI | Professional first impression | Not started | 2026-05-17 |
| Polish map screen | Store monitoring value | In progress | 2026-05-19 |
| Add store detail useful fields | Visit count, latest visit, coverage, OSA if reliable | In progress | 2026-05-21 |
| Add error/loading states | Demo stability | In progress | 2026-05-22 |
| Capture mobile screenshots | Needed for report and presentation | Not started | 2026-05-25 |
| Decide final mobile scope | Avoid adding too many features late | Not started | 2026-05-25 |

### WP5 - Web Dashboard

| Task | Why It Matters | Status | Deadline |
| --- | --- | --- | --- |
| Build admin dashboard overview | Global monitoring of execution | In progress | 2026-05-22 |
| Add validation dashboard | Web is the place for validation results | In progress | 2026-05-25 |
| Add validation issues table | Detail view for data quality analysis | Not started | 2026-05-27 |
| Add filters | Date, rule, severity, store, employee | Not started | 2026-05-29 |
| Add merchandiser monitoring view | Shows active merch, visits, stores covered | Not started | 2026-06-01 |
| Add store coverage view | Covered/not covered, region/city distribution | Not started | 2026-06-02 |
| Add OSA analytics view | OSA rate by product/store/banner | Not started | 2026-06-04 |
| Apply Smollan branding | Logo, colors, clean professional UI | Not started | 2026-06-05 |
| Capture web screenshots | Report and defense | Not started | 2026-06-06 |

### WP6 - OSA Prediction System

Goal: create a realistic PFE-level prediction module without overengineering.

Recommended MVP:

> Predict the probability that a product will be available (OSA = Oui) for a given store/product/context using historical survey responses.

| Task | Why It Matters | Status | Deadline |
| --- | --- | --- | --- |
| Define target variable | `Oui` = available, `Non` = unavailable | Not started | 2026-05-24 |
| Build OSA training dataset | From `survey_responses` and `visits` | Not started | 2026-05-26 |
| Select features | store, product, brand, category, city, region, day, merchandiser, history | Not started | 2026-05-27 |
| Create baseline model | Logistic Regression or Random Forest | Not started | 2026-05-30 |
| Add evaluation metrics | Accuracy, precision, recall, F1, confusion matrix | Not started | 2026-06-01 |
| Save model output | CSV/table with probability and risk level | Not started | 2026-06-03 |
| Add backend endpoint | Expose predictions to web dashboard | Not started | 2026-06-05 |
| Add web prediction card/table | "High risk of out-of-stock" products | Not started | 2026-06-07 |
| Document limits honestly | Prediction is academic MVP, depends on data volume/quality | Not started | 2026-06-08 |

Professional explanation:

> The prediction system adds a prescriptive analytics layer by estimating product availability risk based on historical field responses and contextual variables.

### WP7 - Power BI / Reporting Package

| Task | Why It Matters | Status | Deadline |
| --- | --- | --- | --- |
| Finalize Power BI or web dashboard choice | Avoid duplicate effort | Not started | 2026-05-20 |
| Export final BI screenshots | Evidence for report | Not started | 2026-06-08 |
| Explain KPI calculations | Data analyst credibility | Not started | 2026-06-08 |
| Add dashboard interpretation | Every chart must be interpreted, not just shown | Not started | 2026-06-09 |

## Report Structure

| Chapter | Content | Deadline |
| --- | --- | --- |
| Page de garde | Official BDIoT template, title, company, jury | 2026-06-07 |
| Remerciements | Thanks to ENSAM, Smollan, supervisors | 2026-06-07 |
| Resume trilingue | Arabic, French, English abstracts and keywords | 2026-06-07 |
| Introduction generale | Context, motivation, objectives, methodology | 2026-05-30 |
| Chapitre 1 - Contexte general | Smollan, Unilever project, retail execution, internship context | 2026-05-31 |
| Chapitre 2 - Analyse de l'existant | Manual reporting, Excel exports, data quality issues, limitations | 2026-06-01 |
| Chapitre 3 - Solution proposee | Architecture, technologies, retained approach, choice criteria | 2026-06-02 |
| Chapitre 4 - Conception | Database model, ETL flow, API design, validation flow, mobile/web design | 2026-06-03 |
| Chapitre 5 - Realisation | Implementation details, screenshots, algorithms, difficulties | 2026-06-05 |
| Chapitre 6 - Resultats et discussion | KPIs, dashboards, validation examples, OSA prediction results | 2026-06-07 |
| Conclusion et perspectives | What was achieved, limits, future improvements | 2026-06-08 |
| Bibliographie | Retail execution, BI, Spring Boot, ML/OSA references | 2026-06-08 |
| Annexes | SQL, API examples, screenshots, setup guide | 2026-06-10 |

## Figures And Diagrams To Prepare

| Figure | Purpose | Status | Deadline |
| --- | --- | --- | --- |
| General architecture diagram | Show end-to-end solution | Not started | 2026-05-28 |
| ETL data flow diagram | Show Big Data pipeline | Not started | 2026-05-28 |
| Database ER diagram | Show relational model | Not started | 2026-05-29 |
| Validation engine flow | Explain validation results | Not started | 2026-05-30 |
| Mobile dashboard screenshot | Show operational app | Not started | 2026-05-25 |
| Mobile map screenshot | Show store monitoring | Not started | 2026-05-25 |
| Web dashboard screenshot | Show admin analytics | Not started | 2026-06-06 |
| OSA prediction workflow | Show ML contribution | Not started | 2026-06-08 |
| Chronogram / Gantt | Required by school | Not started | 2026-06-04 |

## Jury-Ready Explanations To Prepare

| Question | Expected Answer Direction | Status |
| --- | --- | --- |
| Why StoreCode and not StoreName? | StoreName is not unique; StoreCode is the business key | To verify |
| Why mobile and web? | Mobile is operational; web is analytical and validation-oriented | To verify |
| Why MySQL? | Structured relational data, joins, reporting, integrity constraints | To verify |
| Why Spring Boot? | Professional REST API, separation between data and interfaces | To verify |
| Why Python ETL? | Pandas is effective for Excel cleaning and transformation | To verify |
| What is the Big Data/analytics value? | ETL, data quality, KPI monitoring, OSA prediction | To verify |
| What are the limits? | Data quality, simple auth currently, prediction depends on history | To verify |
| What is future work? | JWT auth, richer ML, real-time pipeline, Docker, CI/CD | To verify |

## Weekly Execution Plan

### Week 1 - 2026-05-11 to 2026-05-17

Objective: stabilize project scope and mobile/dashboard basics.

- [ ] Confirm final PFE title.
- [ ] Confirm final deliverables: ETL + backend + web + mobile + prediction MVP.
- [ ] Finish mobile dashboard KPI definitions.
- [ ] Remove confusing KPIs from mobile.
- [ ] Verify admin/supervisor login.
- [ ] Create KPI definition notes.
- [ ] Start architecture diagram draft.
- [ ] Prepare first report outline.

### Week 2 - 2026-05-18 to 2026-05-24

Objective: strengthen data model and backend.

- [ ] Finalize ER diagram.
- [ ] Document ETL mapping.
- [ ] Add backend validation filter endpoints.
- [ ] Improve login/roles or clearly document demo authentication.
- [ ] Stabilize map/store details.
- [ ] Define OSA prediction target and dataset.
- [ ] Prepare mobile screenshots.

### Week 3 - 2026-05-25 to 2026-05-31

Objective: web analytics and OSA prediction base.

- [ ] Build web validation issue table.
- [ ] Add filters by date/rule/severity.
- [ ] Build merchandiser monitoring view.
- [ ] Prepare OSA training dataset.
- [ ] Train baseline OSA model.
- [ ] Start writing chapters 1, 2, and 3.

### Week 4 - 2026-06-01 to 2026-06-07

Objective: final feature package and first complete report draft.

- [ ] Add OSA prediction result view.
- [ ] Finish web dashboard design with Smollan branding.
- [ ] Capture final screenshots.
- [ ] Complete report first draft.
- [ ] Add diagrams and chronogram.
- [ ] Write trilingual abstracts.

### Week 5 - 2026-06-08 to 2026-06-14

Objective: polish and review.

- [ ] Format report according to school rules.
- [ ] Add bibliography.
- [ ] Add annexes.
- [ ] Send review package to supervisor/company if possible.
- [ ] Fix remaining bugs.
- [ ] Prepare defense presentation.
- [ ] Prepare demo script.

### Final Week - 2026-06-15 to 2026-06-20

Objective: final delivery and defense readiness.

- [ ] Freeze code.
- [ ] Freeze database demo dataset.
- [ ] Prepare final report PDF and DOCX.
- [ ] Prepare final presentation.
- [ ] Prepare backup screenshots.
- [ ] Prepare setup/demo guide.
- [ ] Rehearse defense story.
- [ ] Prepare answers for jury questions.

## Daily Routine Until Delivery

Use this every working day:

- [ ] 30 minutes: fix one technical task.
- [ ] 30 minutes: write or improve one report section.
- [ ] 15 minutes: update screenshots/evidence.
- [ ] 15 minutes: update this checklist.

Rule:

> Do not spend a full day only coding. Every feature must become report material: screenshot, explanation, diagram, or KPI definition.

## Final Demo Scenario

1. Show raw data source: Excel/portal export.
2. Run or explain ETL pipeline.
3. Show MySQL tables: stores, visits, survey responses, validation results.
4. Start Spring Boot backend.
5. Open web dashboard.
6. Show validation results and filters.
7. Show merchandiser monitoring dashboard.
8. Open mobile app as supervisor.
9. Show daily field KPIs and map.
10. Logout and login as admin.
11. Show admin/global scope.
12. Show OSA prediction result as advanced analytics contribution.

## Evidence Folder To Build

Create a folder later named:

```text
PFE_DELIVERY_EVIDENCE/
```

It should contain:

- Architecture diagram PNG.
- ETL flow diagram PNG.
- Database ER diagram PNG.
- Mobile screenshots.
- Web screenshots.
- API JSON examples.
- SQL validation examples.
- OSA prediction metrics.
- Final demo script.

## Risk List

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Too many features late | Project becomes unstable | Freeze scope by 2026-05-24 |
| Report delayed | PFE not ready even if app works | Write report every week |
| OSA prediction too complex | Time loss | Build simple MVP model first |
| Authentication consumes time | Backend delay | Do BCrypt/roles first, JWT only if time |
| Web/mobile bugs during demo | Bad impression | Prepare screenshots and stable dataset |
| Data mismatch questions | Jury confusion | Prepare KPI definitions and SQL checks |

## Minimum Viable PFE By 2026-06-20

The project is acceptable if these are ready:

- [ ] ETL pipeline works and is documented.
- [ ] MySQL schema is clean and explained.
- [ ] Spring Boot backend runs.
- [ ] Mobile app has login/logout, dashboard, map.
- [ ] Web app has dashboard and validation results.
- [ ] At least two validation rules are explained.
- [ ] OSA prediction MVP exists with metrics.
- [ ] Report follows school structure.
- [ ] Presentation and demo script are ready.


# PFE Agenda

This file records what we add and why.

## 2026-05-13 - Mobile App Reframing

### What We Decided

The mobile app is central to the company requirement.

The Unilever supervisor works in the field, so the app must make execution data easy to visualize while visiting stores.

### Why It Matters

The project problem is not only about analytics or validation. It is also about replacing/improving the daily report with a mobile operational tool for supervisors.

### What Was Added

- `docs/MOBILE_SUPERVISOR_APP_PLAN.md`
- `docs/AGENDA.md`

### Next Action

Update the report problematique and mobile requirements to reflect the mobile-first company need.

Then redesign the mobile dashboard screen first.

## 2026-05-13 - Documentation Alignment

### What Changed

- Updated `docs/START_HERE.md` so the first priority is the mobile supervisor workflow.
- Updated `README.md` so the repository introduction explains the mobile app as the main company-facing deliverable.
- Updated `Report/PROJECT_CONTEXT_FOR_AI_AGENTS.md` so another AI agent understands the mobile-first PFE direction.
- Recompiled `Report/main.pdf` after updating the report text.

### Why This Matters

The project now has one consistent story:

```text
Mobile app for Unilever supervisors
supported by ETL + MySQL + validation rules + backend + web dashboard + Power BI
```

This avoids presenting the PFE as disconnected parts.

## 2026-05-13 - Mobile Dashboard First Redesign

### What Changed

- Updated `mobile/App.js` so the dashboard can navigate directly to the Merch and Map screens.
- Updated `mobile/src/screens/DashboardScreen.js` with a supervisor-focused home screen:
  - coverage percentage
  - visited stores versus assigned stores
  - not visited stores
  - field visits
  - active merchandisers
  - latest visit date
  - quick actions to execution list and store map
- Updated `mobile/src/styles/appStyles.js` and `mobile/src/constants/colors.js` with the new dashboard styles.

### Why This Matters

The first screen now speaks the language of the Unilever supervisor in the field. It shows what is covered, what is still pending, and where to go next.

### Verification

Ran an Android Expo export. The mobile bundle compiled successfully.

Started the Expo mobile development server at:

```text
http://localhost:8081
```

### Report Update

Added a paragraph in `Report/main.tex` describing the improved mobile dashboard, then recompiled `Report/main.pdf`.

## 2026-05-13 - Dashboard KPI Correction

### What We Decided

The best dashboard coverage KPI for a field supervisor is:

```text
stores executed today / stores planned today
```

### Why This Matters

This is stronger than comparing today's visited stores to all assigned stores, because a supervisor is not expected to visit the full assigned perimeter every day.

### Logic Adopted

Based on the current Excel/portal logic, the `CALLCYCLE DEVIATION` response is interpreted as:

```text
Non = covered planned execution
Oui = callcycle deviation
Oui + Non = stores with a callcycle execution status for the selected period
```

So the dashboard coverage becomes:

```text
covered stores / planned callcycle stores
= Callcycle Non / (Callcycle Oui + Callcycle Non)
```

This makes the dashboard closer to the business language used in the field.

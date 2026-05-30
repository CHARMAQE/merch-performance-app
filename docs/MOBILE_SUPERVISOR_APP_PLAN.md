# Mobile Supervisor App Plan

## Why The Mobile App Is Central

For the company side, the mobile app is the main operational tool.

The Unilever supervisor works in the field and needs fast access to execution information while visiting stores. The app must help the supervisor answer practical questions:

- Which merchandisers are active?
- Which stores are assigned?
- Which stores were visited?
- Which stores were not visited?
- Which stores have execution deviation?
- Which merchandiser covered which store?
- Are there GPS or execution anomalies?
- What needs action today?

The web dashboard and Power BI are still important, but they have different roles:

- Web dashboard: deeper validation, monitoring, and control.
- Power BI: management reporting and business analysis.
- Mobile app: daily field supervision and operational decision support.

## Current Mobile Screens

The current mobile app already has four screens:

```text
LoginScreen
DashboardScreen
MerchandiserExecutionScreen
StoreMapScreen
```

Current backend mobile endpoints:

```text
POST /api/mobile/login
GET  /api/mobile/overview
GET  /api/mobile/merchandisers
GET  /api/mobile/merchandisers/{employeeCode}/stores
GET  /api/mobile/execution-stores
GET  /api/mobile/stores
GET  /api/mobile/stores/{storeCode}
```

## Main UX Problem

The app has the right technical base, but the supervisor workflow is not yet clear enough.

The design should feel like a field execution control app, not a generic dashboard. It should prioritize:

1. fast scanning,
2. clear action priorities,
3. store and merchandiser navigation,
4. simple KPIs,
5. anomaly visibility,
6. good mobile readability.

## Target Mobile Workflow

### 1. Login

Purpose:

Allow the supervisor to access only assigned stores and assigned merchandisers.

Important points:

- Login should be simple.
- Error messages should be clear.
- The app should show the supervisor name after login.

### 2. Home / Daily Execution Overview

Purpose:

Give the supervisor a quick answer to: "What is happening today or this month?"

Recommended KPIs:

- Daily execution coverage: stores executed today / stores planned today
- Planned stores not executed
- Executed stores outside the planned call cycle
- Month progress: stores visited in the selected month / assigned stores
- Field visits
- Active merchandisers
- Revisited stores
- Execution deviations
- Validation issues

Important data rule:

```text
The denominator for daily execution coverage comes from the CALLCYCLE DEVIATION task.
```

Current interpretation:

```text
Callcycle Deviation = Non -> covered planned execution
Callcycle Deviation = Oui -> deviation
Planned callcycle stores -> all stores with Oui or Non in the selected period
```

Dashboard formula:

```text
coverage = Callcycle Non stores / (Callcycle Non stores + Callcycle Oui stores)
```

Recommended design:

- concise header with supervisor name and period
- compact KPI grid
- action cards for "Not visited", "Deviations", and "Map"
- refresh button

### 3. Merchandiser Execution

Purpose:

Help the supervisor inspect merchandiser performance.

Required views:

- list of merchandisers
- search by name, code, city
- execution summary per merchandiser
- stores covered by a selected merchandiser
- deviations linked to a merchandiser

Recommended design:

- ranking/list format
- color states for covered/deviation/missing
- easy back navigation
- store list should be readable and tappable

### 4. Store Map

Purpose:

Help the supervisor visualize assigned stores geographically.

Required views:

- map of assigned stores
- search store by code/name/city/format
- store detail bottom sheet
- latest visit information
- merchandiser and anomaly context

Recommended design:

- search always accessible
- marker colors by status
- bottom sheet with key facts first
- avoid too much text in the first sheet state

### 5. Store Details

Purpose:

Give operational context for one selected store.

Important information:

- store code and name
- city/format
- latest visit date
- assigned merchandiser or last merchandiser
- number of visits
- validation issues
- OSA / GPS / execution anomaly summary

## Design Direction

The mobile app should be professional, simple, and operational.

Preferred style:

- clean white surfaces
- strong navy primary color
- small orange accents for attention
- status colors for success/warning/danger
- compact cards
- readable lists
- less decorative layout

Avoid:

- too much empty space
- oversized titles inside working screens
- too many generic cards
- unclear navigation
- dashboard-only thinking

## Implementation Order

### Phase 1 - Mobile Framing

Status: started.

Tasks:

1. Update report problematique to make the mobile app central.
2. Document the supervisor mobile workflow.
3. Define the design direction.

### Phase 2 - Dashboard Redesign

Tasks:

1. Make the home screen an operational overview.
2. Add clear period context.
3. Add action cards for not visited stores and deviations.
4. Improve KPI labels.

### Phase 3 - Merchandiser Screen Redesign

Tasks:

1. Improve merchandiser list readability.
2. Add clearer execution states.
3. Improve selected merchandiser store details.
4. Improve back navigation.

### Phase 4 - Store Map Redesign

Tasks:

1. Improve map search.
2. Improve store detail sheet.
3. Show store execution/anomaly status more clearly.
4. Add status-based marker design if backend data supports it.

### Phase 5 - Backend Data Gaps

Tasks:

1. Identify missing data needed by mobile screens.
2. Add API fields or endpoints only when necessary.
3. Keep mobile screens fast and simple.

## Report Usage

This plan feeds the following report sections:

```text
Introduction generale - problematique
Analyse des besoins - besoins fonctionnels mobile
Conception du systeme - architecture applicative
Realisation - developpement de l'application mobile
Tests et resultats - validation de l'application mobile
```

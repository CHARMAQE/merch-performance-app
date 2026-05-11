# Backend API

This folder contains the Spring Boot backend for the Merch Performance project.

The backend connects to the MySQL database loaded by the Python ETL and exposes API endpoints for the frontend dashboard.

## Stack

- Java
- Spring Boot
- Spring JDBC
- MySQL Connector/J
- Maven wrapper

## Current Configuration

Main config file:

```text
backend/src/main/resources/application.properties
```

Current database:

```text
unilever_db
```

Current backend port:

```text
9000
```

The backend now supports environment-variable overrides through:

- `DB_URL`
- `DB_USER`
- `DB_PASSWORD`
- `SERVER_PORT`

Local fallback values still exist in `application.properties` for development convenience.

## Run Locally

From this folder:

```bash
./mvnw spring-boot:run
```

On Windows:

```powershell
.\mvnw.cmd spring-boot:run
```

The API should run at:

```text
http://localhost:9000
```

## Current Endpoints

### Dashboard Overview

```text
GET /api/dashboard/overview
```

Returns the main dashboard snapshot:

- visit date coverage in the database
- counts of employees, stores, products, visits, and survey responses
- the latest validation run
- issue counts by rule
- issue counts by severity

### Latest Validation Issues

```text
GET /api/dashboard/latest-issues?limit=12
```

Returns the latest validation issues from the most recent validation run.

### Mobile Supervisor Login

```text
POST /api/mobile/login
```

Request:

```json
{
  "username": "casa_sup",
  "password": "1234"
}
```

Returns the active supervisor account when the demo credentials are valid.

### Mobile Supervisor Stores

```text
GET /api/mobile/stores?supervisorId=1
```

Returns only stores assigned to the supervisor through `supervisor_stores`.

### Mobile Supervisor Store Details

```text
GET /api/mobile/stores/{storeCode}?supervisorId=1
```

Returns store details only if the selected store is assigned to that supervisor.

## Current Java Packages

```text
com.smollan.backend.controller
com.smollan.backend.dto.dashboard
com.smollan.backend.dto.map
com.smollan.backend.dto.validation
com.smollan.backend.service
```

## Important Files

```text
BackendApplication.java
controller/DashboardController.java
controller/MobileController.java
controller/StoreMapController.java
service/DashboardService.java
service/MobileService.java
service/StoreMapService.java
dto/dashboard/DashboardOverviewResponse.java
dto/map/StoreMapMarkerResponse.java
dto/map/StoreMapDetailResponse.java
dto/mobile/MobileLoginRequest.java
dto/mobile/MobileSupervisorResponse.java
dto/validation/ValidationIssueResponse.java
```

## Known Issues

- Credentials now support environment-variable overrides, but the fallback defaults should still be hardened before deployment.
- There are no full API tests yet.
- Dashboard and map services use SQL queries, so column names must stay aligned with the ETL-created tables.

## How To Continue

Recommended next backend work:

1. Add endpoint(s) for validation run history and filtering by date or rule.
2. Add drill-down reporting around `validation_results`.
3. Add backend tests with an H2 or test MySQL profile.
4. Add clearer error handling for missing dynamic task tables.

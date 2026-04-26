# Backend API

This folder contains the Spring Boot backend for the Merch Performance project.

The backend connects to the MySQL database loaded by the Python ETL and exposes API endpoints for the frontend dashboard.

## Stack

- Java
- Spring Boot
- Spring Data JPA
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

Important: the database password is currently written directly in `application.properties`. Move this to environment variables before sharing or deploying the project.

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

### Employees

```text
GET /api/employees/
```

Returns all rows from the `employees` table.

### Call Cycle Deviation Summary

```text
GET /api/reports/deviation-summary
```

Returns summary counts by employee using:

- `task_callcycle_deviation`
- `visits`
- `employees`

The query counts:

- completed visits where `q_callcycle_deviation = 'Non'`
- deviated visits where `q_callcycle_deviation = 'Oui'`
- total rows

## Current Java Packages

```text
com.smollan.backend.controller
com.smollan.backend.entity
com.smollan.backend.repository
com.smollan.backend.service
```

## Important Files

```text
BackendApplication.java
controller/EmployeeController.java
controller/CallcycleDeviationController.java
repository/EmployeeRepository.java
repository/CallcycleDeviationRepository.java
entity/Employee.java
entity/TaskLocationChecin.java
```

## Known Issues

- Credentials are hardcoded in `application.properties`.
- The backend currently exposes only a small part of the database.
- `TaskLocationChecin` appears to have a typo in the class name.
- There are no full API tests yet.
- The report repository uses a native SQL query, so column names must stay aligned with the ETL-created tables.

## How To Continue

Recommended next backend work:

1. Move database credentials to environment variables.
2. Add endpoint(s) for `validation_results`.
3. Add endpoint(s) for `validation_run_log`.
4. Add dashboard summary DTOs instead of exposing raw projections only.
5. Add backend tests with an H2 or test MySQL profile.
6. Add clearer error handling for missing dynamic task tables.

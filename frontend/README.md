# Frontend

This folder contains the React frontend for the Merch Performance project.

The frontend is currently an early dashboard starting point. It connects to the backend and displays employees loaded from MySQL.

## Stack

- React
- React DOM
- React Scripts
- Testing Library

## Current Behavior

Current entry file:

```text
frontend/src/App.js
```

The app calls:

```text
http://localhost:9000/api/employees/
```

Then it displays:

```text
employee_code - username
```

## Requirements

Before running the frontend:

1. MySQL should be running.
2. The database should be created and loaded.
3. The backend should be running on port `9000`.

## Run Locally

From this folder:

```bash
npm install
npm start
```

The frontend usually opens at:

```text
http://localhost:3000
```

## Available Scripts

```bash
npm start
```

Starts the development server.

```bash
npm test
```

Runs frontend tests.

```bash
npm run build
```

Builds the frontend for production.

## Backend Dependency

The frontend currently expects this backend endpoint:

```text
GET http://localhost:9000/api/employees/
```

If the backend is not running, the page will load but the employee list will not appear.

## How To Continue

Recommended next frontend work:

1. Add a dashboard view for validation results.
2. Add a deviation summary view using `/api/reports/deviation-summary`.
3. Add loading and error states.
4. Move the backend API base URL into an environment variable.
5. Add basic styling and layout.
6. Add frontend tests for data loading states.

## Current Scope

This is not yet a full dashboard. It is the first frontend connection to the backend/API layer.

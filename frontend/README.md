# Frontend

This folder contains the React frontend for the Merch Performance project.

The frontend displays the store map and validation dashboard view backed by the Spring Boot API.

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
GET http://localhost:9000/api/map/stores
```

Then it displays:

```text
stores detected on the map with their current dashboard context
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

The frontend defaults to this backend URL:

```text
http://localhost:9000
```

Override it locally with:

```env
REACT_APP_API_BASE_URL=http://localhost:9000
```

If the backend is not running, the page will load but the map data will not appear.

## How To Continue

Recommended next frontend work:

1. Add validation issue drill-down filters.
2. Add OSA risk prediction views.
3. Add frontend tests for map and dashboard loading states.

## Current Scope

This is a first dashboard version focused on stores, validation, and the map.

# Frontend

## Overview

This folder contains the React frontend for the Merch Performance project.

The current UI is still minimal. Right now it fetches employee data from the backend and renders a simple list.

Current entry file:
- [App.js](/c:/Users/hamza/CHARMAQE/MySpace/merch-performance-app/frontend/src/App.js:1)

## Stack

- React
- React Scripts
- Testing Library

## Current Behavior

The current application:

- starts a React development server
- sends a request to `http://localhost:9000/api/employees/`
- displays returned employees on the page

So the frontend expects the backend to be running locally on port `9000`.

## Available Scripts

From the `frontend` folder:

```bash
npm install
npm start
```

Other available scripts:

```bash
npm test
npm run build
```

## Development Notes

Before running the frontend, make sure:

1. MySQL is running
2. the backend is running on `http://localhost:9000`
3. the backend can access the same data loaded by the ETL pipeline

## Current Scope

This frontend is still a starting point.

The repository structure suggests the longer-term goal is to evolve from a simple employee list into a richer merch performance dashboard backed by the ETL pipeline and the Spring Boot API.

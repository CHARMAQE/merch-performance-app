# Merch Performance App

## Project Overview
This project is developed during my internship at **Smollan Morocco** in the context of my **Master M2 Big Data & IoT at ENSAM Casablanca**.

The goal of the project is to build a **data-driven retail execution monitoring platform** for Unilever data, combining:

- automated data extraction
- ETL pipeline
- structured MySQL storage
- OSA validation logic
- data quality monitoring
- future analytics and machine learning integration

---

## Main Objectives

The project aims to:

- transform and load the exported Excel data into a structured MySQL database
- organize operational data by visits, stores, products, and business task families
- add a validation layer to detect suspicious or inconsistent OSA responses
- prepare the project for advanced analytics and future OOS prediction

---

## Current Architecture

```text
Extract (Playwright)
        ↓
Inbound Excel files
        ↓
Transform / ETL
        ↓
MySQL base tables + task tables
        ↓
survey_responses engineering layer
        ↓
validation layer
        ↓
future dashboards / ML / web app

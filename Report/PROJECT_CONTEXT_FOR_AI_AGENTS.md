# Complete Project Context - Merch Performance App
## For AI Agents: ChatGPT, Claude, or Technical Documentation Assistance

**Project Name:** Merch Performance App - Unilever Retail Execution Data Platform  
**Student:** CHARMAQE Hamza  
**School:** ENSAM Casablanca  
**Company:** Smollan / Unilever Morocco  
**Project Status:** Academic Prototype (Production-ready data layer, functional MVP frontend/mobile)  
**Target Readiness Date:** 2026-06-20  

---

## EXECUTIVE SUMMARY

This is a **complete data platform** (not just an app) that transforms raw field execution Excel data into operational and analytical insights for three stakeholders:
- **Supervisors**: Mobile app for daily execution follow-up
- **Data Analysts**: Web dashboard for validation and anomaly detection
- **Managers**: Power BI reporting and predictive analytics

**Full Stack:**
```
Excel export → Python ETL → MySQL database → Validation rules 
→ Spring Boot API → React web dashboard + Expo mobile app + Power BI
```

---

## PART 1: PROJECT ARCHITECTURE

### 1.1 Technology Stack

**Data Engineering:**
- Python (pandas, SQLAlchemy)
- Playwright (portal automation)
- Main file: `data-engineering/main.py`

**Database:**
- MySQL 8+
- Schema: `unilever_db`
- Dynamic table generation at runtime

**Backend:**
- Java Spring Boot
- Spring JDBC
- Port: 9000
- API endpoints: `/api/dashboard`, `/api/map`, `/api/mobile`

**Frontend:**
- React (web validation dashboard)
- Expo (mobile supervisor app)
- Development ports: 3000 (web), 8081 (mobile)

**Analytics:**
- Power BI (future management reporting)

### 1.2 Repository Structure

```
merch-performance-app/
├── backend/              # Spring Boot API server
├── data-engineering/     # Python ETL pipeline
│   ├── main.py          # Main entrypoint
│   ├── transform/       # Data cleaning and transformation
│   ├── load/            # Database loading logic
│   └── validation/      # Validation rules engine
├── database/            # MySQL schema and utilities
├── frontend/            # React web dashboard
├── mobile/              # Expo supervisor app
├── docs/                # Project documentation
└── requirements.txt     # Python dependencies
```

### 1.3 Current Project Status

**Strongest Component:** Data engineering and validation flow (production-ready)

**Functional Components:**
- ETL pipeline: ✓ Complete and tested
- Database schema: ✓ Stable
- Validation engine: ✓ 2 active rules, extensible framework
- Backend API: ✓ Basic endpoints working
- Frontend dashboard: ✓ First MVP version
- Mobile app: ✓ Core screens functional

**Still Needed for Production:**
- More validation rules
- Deeper reporting dashboards
- Comprehensive automated testing
- Docker containerization
- JWT authentication

---

## PART 2: DATA LAYER (Core Business Logic)

### 2.1 Data Source

**Input:** Excel file from Unilever/Smollan portal  
**Format:** Single sheet with 25+ columns  

**Key Raw Columns:**
- `dateid`, `date`, `responseDate` (temporal)
- `employeeCode`, `Username` (merchandiser identity)
- `StoreCode`, `StoreName`, `StoreCity`, `StoreState`, `StoreRegion`, `StoreFormat` (store identity)
- `ProductCode`, `ProductBarCode`, `ProductDescription`, `Category`, `BrandName` (product identity)
- `Task`, `Title`, `Question`, `Response` (survey data)
- `latitude`, `longitude` (GPS coordinates)

**Observed Sample Statistics (April 28-29, 2026):**
- Raw response rows: 57,065
- Unique visits: 550
- Active merchandisers: 112
- Distinct stores: 461
- Distinct products: 252
- Task types: 16
- OSA response rows: 43,883
- OSA Yes rate: 53.95%

### 2.2 Business Keys & Data Rules

| Entity | Business Key | Why This Matters |
|---|---|---|
| Store | `store_code` | Multiple stores may have similar names; code is unique |
| Merchandiser | `employee_code` | Employee may change username; code is stable |
| Product | `product_code` | Multiple products may have duplicate descriptions |
| Visit | `visit_date + employee_id + store_id` | One employee + one store + one date = one visit |
| Response | Individual row in Excel | Multiple responses per visit (different questions) |

**Critical Rule:** If the same employee visits the same store on the same day with 10 response rows, that is **1 visit** with 10 responses, NOT 10 visits.

### 2.3 Database Schema

**Core Tables:**

1. **employees**
   - `employee_id` (PK)
   - `employee_code` (business key)
   - `username`

2. **stores**
   - `store_id` (PK)
   - `store_code` (business key)
   - `store_name`, `city`, `state`, `region`, `format`

3. **products**
   - `product_id` (PK)
   - `product_code` (business key)
   - `description`, `barcode`, `category`, `brand`

4. **visits**
   - `visit_id` (PK)
   - `visit_date`, `employee_id`, `store_id`
   - Natural key: `(visit_date, employee_id, store_id)`
   - `latitude`, `longitude` (GPS at check-in)

5. **survey_responses** (Analytics table)
   - `response_id` (PK)
   - `visit_id`, `employee_code`, `store_code`, `product_code`
   - `task`, `title`, `question`, `response`
   - `response_datetime`, `latitude`, `longitude`
   - **Why:** This normalized table powers validation, web dashboard, and Power BI

6. **task_*** (Dynamic tables)
   - Examples: `task_location_checkin`, `task_osa_pack_coc_mh`, `task_sos`
   - Created automatically by ETL
   - Question names become columns
   - Useful for task-specific wide reporting

7. **validation_results**
   - `issue_id` (PK)
   - `visit_id`, `rule_code`, `severity`, `description`
   - Stores validation anomalies

8. **validation_run_log**
   - Tracks when validations run
   - `run_id`, `run_timestamp`, `visits_checked`, `issues_found`

9. **validation_rules**
   - Metadata about validation rules
   - `rule_code`, `description`, `active_flag`

### 2.4 ETL Pipeline Flow

**Step 1: Extract**
- User chooses local Excel file OR auto-download from Smollan portal
- Playwright automation for portal login and download

**Step 2: Prepare Source**
```python
Function: prepare_source_dataframe()
- Read Excel with pandas
- Convert column names to lowercase
- Build real `date` column from `dateid`
- Convert `responseDate` from Excel serial format
```

**Step 3: Build Base Tables**
```python
Function: run_etl()
- Extract unique employees, stores, products
- Create visits (unique: visit_date + employee_id + store_id)
- Load to MySQL with upsert logic:
  INSERT ... ON DUPLICATE KEY UPDATE
```

**Step 4: Build Dynamic Task Tables**
```python
Function: build_task_tables()
Mapping examples:
- LOCATION CHECK IN → task_location_checkin
- LOCATION CHECK OUT → task_location_checkout
- CALLCYCLE DEVIATION → task_callcycle_deviation
- OSA / COC / MH / PACK → task_osa_pack_coc_mh
- SOS → task_sos

Schema creation:
- Questions become column names
- Normalized to snake_case
- Table altered if new questions appear
```

**Step 5: Build Normalized Survey Responses**
```python
Function: build_survey_responses_dataframe()
Output: survey_responses table
- One row per response
- visit_id, employee_code, store_code, product_code
- task, title, question, response, datetime, GPS
- Used for: validation, web dashboard, analytics
```

**Step 6: Run Validation (if enabled)**
```python
Function: validation_runner.py
- Create validation run log
- Run active validation rules
- Insert issues into validation_results
- Only validates visits from uploaded file (daily load mode)
```

**Load Modes:**
- **History/Backfill:** Loads data, skips validation
- **Daily Load:** Loads data, runs validation on new visits

### 2.5 Active Validation Rules

**Rule 1: OSA_UNUSUAL_NON_BY_BANNER**
- **What it detects:** Suspicious "Non" (unavailable) answers for OSA questions
- **How:** Compares against weekly product/banner availability patterns
- **Flags:** "Non" responses when most similar stores/dates show "Oui"
- **Business value:** Prevents data entry errors (merchandiser marks product unavailable by mistake)

**Rule 2: GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH**
- **What it detects:** GPS check-ins far from the monthly pattern for same merchandiser + store
- **How:** Compares GPS cluster for repeated monthly visits
- **Flags:** Outlier GPS coordinates (e.g., store visit from wrong location)
- **Business value:** Detects GPS fraud or location errors

**Rule Framework (Extensible):**
All rules inherit from `BaseValidationRule`:
```python
class BaseValidationRule:
    def execute(self, visit_ids, connection) -> List[ValidationResult]
```
New rules added to: `data-engineering/validation/rules/`

---

## PART 3: BACKEND API

### 3.1 Current Endpoints

**Dashboard Overview:**
```
GET /api/dashboard/overview
Returns: {
  totalVisits, activeEmployees, distinctStores,
  revisitedStores, osaRate, validationIssuesCount
}
```

**Latest Issues:**
```
GET /api/dashboard/latest-issues
Returns: [
  { issue_id, rule_code, store_code, severity, description }
]
```

**Store Map:**
```
GET /api/map/stores
Returns: [ { store_id, store_code, store_name, latitude, longitude, city } ]
```

**Store Details:**
```
GET /api/map/stores/{storeCode}/details
Returns: {
  store_code, store_name, city, region, format,
  lastVisit, merchandiser, tasks, osaRate, validationIssues
}
```

**Mobile Endpoints (Under Development):**
```
GET /api/mobile/overview - Day/month overview
GET /api/mobile/merchandisers - Merchandiser list
GET /api/mobile/stores - Store list with coverage
GET /api/mobile/alerts - Active issues
```

### 3.2 Backend Architecture Notes

- **Framework:** Spring Boot with Spring JDBC (no JPA, uses raw SQL)
- **Database Connection:** MySQL (configured via environment variables)
- **API Documentation:** Endpoints currently not formally documented (TODO)
- **Authentication:** Currently disabled; JWT planned
- **Error Handling:** Basic; should be hardened before production

---

## PART 4: FRONTEND & MOBILE

### 4.1 React Web Dashboard

**Current Screens:**
1. **Dashboard Overview** - KPI cards (stores, visits, merchandisers, revisited stores)
2. **Store Map** - Leaflet map with store markers
3. **Store Details** - Store info, last visit, tasks, OSA rate
4. **Latest Issues** - Table of validation anomalies

**Technology:**
- React Hooks
- React Scripts (create-react-app)
- Leaflet for mapping
- Calls backend at `http://localhost:9000`

**Next Priorities:**
- Validation center with filters
- OSA analytics by category/brand/product
- Merchandiser performance tracking
- Store coverage analysis

### 4.2 Expo Mobile App

**Current Screens:**
1. **Login** - Username/password for supervisors and admin
2. **Daily Overview** - KPI cards matching web dashboard
3. **Merchandiser Execution** - List of merchandisers with visit counts
4. **Store Map** - Mobile-friendly map view
5. **Store Details** - Swipeable store information (new feature)

**Technology:**
- Expo (React Native)
- Calls backend at configurable API URL
- Stores auth token in AsyncStorage
- Responsive layout for mobile screens

**Recent Changes:**
- Added `MerchandiserExecutionScreen` for supervisor tracking
- Enhanced `StoreMapScreen` with role-based filters
- Added `StoreDetailsScreen` with execution context
- Integrated PFE analytics dashboard endpoint

---

## PART 5: KNOWN ISSUES & TECHNICAL DEBT

### Code Quality
- `requirements.txt` needs dependency cleanup (portal-export specific)
- Backend credentials should use environment variables consistently
- No Docker Compose setup yet (planned)
- Limited test coverage (data transformations tested manually)

### Data Quality
- `responseDate` in Excel is unreliable for timing analysis
  - **Cannot yet rely on:** first activity time, last activity time, visit duration
  - **Can be added later** when better timestamp source is confirmed

### Dashboard
- First MVP version only
- Needs more KPI filters and drill-down capabilities
- OSA prediction not yet implemented

### API Documentation
- Endpoints not formally documented
- Swagger/OpenAPI integration planned

---

## PART 6: HOW TO USE THIS PROJECT LOCALLY

### 6.1 First-Time Setup

```bash
# 1. Create MySQL database
mysql -u root -p < database/schema.sql

# 2. Create Python environment file
cp data-engineering/.env.example data-engineering/.env
# Fill in: DB_HOST, DB_USER, DB_PASSWORD, etc.

# 3. Install Python dependencies
pip install -r requirements.txt
pip install playwright
playwright install chromium

# 4. Install backend Maven (if not already installed)
# macOS: brew install maven
# Windows: included as ./mvnw.cmd

# 5. Install frontend dependencies
cd frontend
npm install
```

### 6.2 Running the Full Stack

**Terminal 1 - ETL & Data Load:**
```bash
python data-engineering/main.py
# Choose: local file or portal download
# Choose: history/backfill or daily load with validation
```

**Terminal 2 - Backend API:**
```bash
cd backend
./mvnw spring-boot:run
# Runs on http://localhost:9000
```

**Terminal 3 - React Dashboard:**
```bash
cd frontend
npm start
# Runs on http://localhost:3000
```

**Terminal 4 - Mobile (Optional):**
```bash
cd mobile
npm install
npx expo start
# Scan QR code with Expo Go app
```

### 6.3 Testing the Data Flow

1. **Load sample data:**
   ```bash
   python data-engineering/main.py
   # Choose: local Excel file with test data
   # Choose: daily load (runs validation)
   ```

2. **Verify database:**
   ```sql
   SELECT COUNT(*) FROM visits;
   SELECT * FROM survey_responses LIMIT 10;
   SELECT * FROM validation_results;
   ```

3. **Check backend:**
   ```bash
   curl http://localhost:9000/api/dashboard/overview
   ```

4. **View frontend:**
   Open browser: `http://localhost:3000`

---

## PART 7: EXTENDING THE PROJECT

### 7.1 Adding New Validation Rules

**File:** `data-engineering/validation/rules/`

**Template:**
```python
from .base import BaseValidationRule, ValidationResult

class MyNewRule(BaseValidationRule):
    rule_code = "MY_RULE_CODE"
    rule_name = "Human-Readable Rule Name"
    
    def execute(self, visit_ids: List[int], connection) -> List[ValidationResult]:
        # Query database
        # Find anomalies
        # Return list of ValidationResult objects
        pass
```

**Register in:** `data-engineering/validation/engine/registry.py`

### 7.2 Adding New Backend Endpoints

**File:** `backend/src/main/java/com/unilever/` (modify existing controllers)

**Pattern:**
```java
@RestController
@RequestMapping("/api/yourpath")
public class YourController {
    
    @GetMapping("/{param}")
    public ResponseEntity<?> yourMethod(@PathVariable String param) {
        // Query database
        // Return data
    }
}
```

### 7.3 Adding Frontend Dashboards

**File:** `frontend/src/components/` or `frontend/src/pages/`

**Pattern:**
```javascript
import React, { useState, useEffect } from 'react';

export function YourDashboard() {
  const [data, setData] = useState([]);
  
  useEffect(() => {
    fetch('http://localhost:9000/api/yourendpoint')
      .then(r => r.json())
      .then(setData);
  }, []);
  
  return <div>{/* Your UI */}</div>;
}
```

---

## PART 8: CURRENT PRIORITIES (As of 2026-05-13)

| Priority | Task | Status |
|---|---|---|
| 1 | Stabilize ETL and validation engine | ✓ Complete |
| 2 | Document data model and business rules | 🔄 In Progress |
| 3 | Strengthen mobile app (merchandiser screen, alerts) | 🔄 In Progress |
| 4 | Expand web validation dashboard | Pending |
| 5 | Build OSA analytics dashboard | Pending |
| 6 | Implement OSA prediction MVP | Pending |
| 7 | Create Power BI management reporting | Pending |
| 8 | Comprehensive testing and documentation | Pending |
| 9 | Final internship report and presentation | Pending |

---

## PART 9: KEY METRICS & KPIS

### Execution KPIs
- **Stores:** Distinct stores visited in period
- **Field Visits:** Total visit executions
- **Merchandisers:** Active merchandisers
- **Revisited Stores:** Stores visited by >1 merchandiser
- **Coverage Rate:** Visited / Assigned stores (%)

### Quality KPIs
- **OSA Rate:** "Oui" responses / Total OSA responses (%)
- **Validation Issues:** Data anomalies flagged by rules
- **Data Freshness:** Last upload timestamp

### Geographic KPIs
- **City Coverage:** Distinct cities visited
- **Regional Performance:** Execution by region
- **Store Format Mix:** Distribution by format

---

## PART 10: USEFUL SQL QUERIES

### Overview Stats
```sql
SELECT 
  DATE(v.visit_date) as date,
  COUNT(DISTINCT v.store_id) as stores,
  COUNT(DISTINCT v.employee_id) as merchandisers,
  COUNT(*) as visits
FROM visits v
GROUP BY DATE(v.visit_date)
ORDER BY date DESC;
```

### Validation Issues
```sql
SELECT vr.*, v.visit_date, e.employee_code, s.store_code
FROM validation_results vr
JOIN visits v ON vr.visit_id = v.visit_id
JOIN employees e ON v.employee_id = e.employee_id
JOIN stores s ON v.store_id = s.store_id
WHERE vr.created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
ORDER BY vr.created_at DESC;
```

### OSA Analysis
```sql
SELECT 
  p.product_code,
  p.description,
  COUNT(*) as responses,
  SUM(CASE WHEN sr.response = 'Oui' THEN 1 ELSE 0 END) as available,
  ROUND(100 * SUM(CASE WHEN sr.response = 'Oui' THEN 1 ELSE 0 END) / COUNT(*), 2) as osa_rate
FROM survey_responses sr
JOIN products p ON sr.product_code = p.product_code
WHERE sr.task LIKE '%OSA%'
GROUP BY p.product_code
ORDER BY osa_rate ASC;
```

---

## PART 11: CONTACT & CONTEXT

- **Student:** CHARMAQE Hamza  
- **Internship School:** ENSAM Casablanca  
- **Company:** Smollan / Unilever Morocco  
- **Project Repository:** `c:\Users\hamza\CHARMAQE\MySpace\merch-performance-app`  
- **Git Status:** Active development on `main` branch  

---

## PART 12: RECOMMENDED NEXT QUESTIONS FOR AI AGENTS

When giving this context to another AI agent, you can ask:

1. **"Help me write documentation for [specific component]"**
2. **"Generate a new validation rule for [business scenario]"**
3. **"Write a SQL query to analyze [specific metric]"**
4. **"Create a test suite for [feature]"**
5. **"Help me write a Python ETL transformation for [task]"**
6. **"Generate React component code for [dashboard feature]"**
7. **"Create the architecture chapter of my internship report"**
8. **"Help me document the data flow from Excel to API"**
9. **"Suggest improvements to [code section]"**
10. **"Write the methods chapter of my PFE report"**

---

*This document was auto-generated from project inspection on 2026-05-13.*
*Update this whenever project architecture changes significantly.*

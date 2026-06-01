# Cross-Platform Development Setup

This project is developed on Windows 11 and macOS Apple Silicon. Keep the same tool versions and startup order on both machines to avoid environment drift.

## Standard Versions

- Python: 3.12
- Node.js: 20.x, project marker: `20.11.0`
- npm: 10.x
- Java: 17
- Maven: use the backend Maven wrapper, not a global Maven install
- MySQL: 8.4 through Docker Compose
- Power BI: edit the `.pbix` file on Windows only

## One-Time Setup On Windows 11

Install these tools first:

- Python 3.12
- Node.js 20.x, or `nvm-windows` with Node `20.11.0`
- JDK 17
- Docker Desktop
- Git

Then from the repository root:

```powershell
git status --short --branch
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m playwright install chromium
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
if (-not (Test-Path data-engineering\.env)) { Copy-Item data-engineering\.env.example data-engineering\.env }
if (-not (Test-Path frontend\.env)) { Copy-Item frontend\.env.example frontend\.env }
if (-not (Test-Path mobile\.env)) { Copy-Item mobile\.env.example mobile\.env }
```

Use Node 20 before installing frontend or mobile dependencies:

```powershell
nvm use 20.11.0
node --version
npm --version
```

## One-Time Setup On MacBook Air M1

Install these tools first:

- Python 3.12
- Node.js 20.x, preferably through `nvm`
- JDK 17
- Docker Desktop
- Git

Then from the repository root:

```bash
git status --short --branch
/opt/homebrew/opt/python@3.12/bin/python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m playwright install chromium
[ -f .env ] || cp .env.example .env
[ -f data-engineering/.env ] || cp data-engineering/.env.example data-engineering/.env
[ -f frontend/.env ] || cp frontend/.env.example frontend/.env
[ -f mobile/.env ] || cp mobile/.env.example mobile/.env
```

Use Node 20 before installing frontend or mobile dependencies:

```bash
export NVM_DIR="$HOME/.nvm"
source "$NVM_DIR/nvm.sh"
nvm use 20.11.0
node --version
npm --version
```

## Environment Files

Each `.env.example` file is a safe template. Each copied `.env` file is local, ignored by Git, and must never be committed.

Templates:

- `.env.example`: Docker MySQL and Spring Boot backend defaults
- `data-engineering/.env.example`: Python ETL, MySQL, Playwright portal, and local file paths
- `frontend/.env.example`: React API base URL
- `mobile/.env.example`: Expo API base URL for a physical phone

Change only local `.env` files for:

- real portal credentials
- custom download, backup, or log paths
- mobile LAN IP address
- database password, if you changed the Docker default

The ETL supports the standardized `MYSQL_*`, `PORTAL_USERNAME`, `PORTAL_PASSWORD`, and `DOWNLOAD_DIR` names. Older local `DB_*`, `PORTAL_USER`, `PORTAL_PASS`, and `UNILEVER_DOWNLOAD_DIR` names are still accepted for compatibility.

## MySQL With Docker

Start the shared local MySQL service from the repository root:

```bash
docker compose up -d mysql
docker compose ps
```

The default local database is:

```text
unilever_db
```

The default local root password is:

```text
Root@123
```

Override these values in a local root `.env` file when needed. Do not commit `.env`.

## Initialize The Database

Run only after MySQL is up. These commands create schema objects; they do not run the destructive reset script.

On macOS/Linux:

```bash
docker exec -i unilever-mysql mysql -uroot -pRoot@123 < database/schema.sql
docker exec -i unilever-mysql mysql -uroot -pRoot@123 unilever_db < database/schema_fact_coverage.sql
```

On Windows PowerShell:

```powershell
cmd /c "docker exec -i unilever-mysql mysql -uroot -pRoot@123 < database\schema.sql"
cmd /c "docker exec -i unilever-mysql mysql -uroot -pRoot@123 unilever_db < database\schema_fact_coverage.sql"
```

After loading store data with the ETL, seed demo supervisors if needed:

```bash
docker exec -i unilever-mysql mysql -uroot -pRoot@123 unilever_db < database/seed_supervisors.sql
```

## Run The ETL

Activate the Python 3.12 environment first.

Local Data Dump:

```bash
python data-engineering/main.py --file "/path/to/Data Dump.xlsx" --run-validation
```

Local Coverage:

```bash
python data-engineering/main.py --coverage-file "/path/to/Coverage.xlsx"
```

Portal download requires real credentials in `data-engineering/.env`:

```bash
python data-engineering/main.py --portal
python data-engineering/main.py --coverage-portal
```

## Run The Backend

The backend uses the Maven wrapper, so global Maven is not required.

On macOS/Linux:

```bash
cd backend
./mvnw spring-boot:run
```

On Windows PowerShell:

```powershell
cd backend
.\mvnw.cmd spring-boot:run
```

The API runs at:

```text
http://localhost:9000
```

## Run The React Frontend

```bash
cd frontend
npm install
npm start
```

Optional local API override:

```text
REACT_APP_API_BASE_URL=http://localhost:9000
```

## Run The Expo Mobile App

The phone must call the computer LAN IP, not `localhost`.

```bash
cd mobile
npm install
EXPO_PUBLIC_API_BASE_URL=http://YOUR_COMPUTER_LAN_IP:9000 npm start
```

On Windows PowerShell:

```powershell
cd mobile
$env:EXPO_PUBLIC_API_BASE_URL="http://YOUR_COMPUTER_LAN_IP:9000"
npm start
```

Use tunnel mode if LAN discovery fails:

```bash
npm run start:tunnel
```

## Power BI

The Power BI file stays Windows-only for editing:

```text
BI/My Progress_BI.pbix
```

Use the Mac for code, ETL, backend, frontend, and mobile testing. Use Windows for Power BI Desktop work.

## Do Not Run During Normal Setup

Avoid these unless you intentionally want to wipe loaded data:

```text
database/reset_data.sql
database/clear_uploaded_data.py
```

Daily automation, Prefect, Masters integration, and ML are future work after the manual local setup is stable on both machines.

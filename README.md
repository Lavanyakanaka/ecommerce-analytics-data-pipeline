# E-Commerce Analytics Data Pipeline

Student Name: Kella Kanaka Lavanya
Roll Number: 23A91A1228
Submission Date: 2026-02-12

## Project Overview
End-to-end data pipeline for an e-commerce platform: synthetic data generation, ingestion, cleansing, quality checks, production and warehouse loads, analytics exports, and BI dashboards.

## Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Docker & Docker Compose (optional)
- Git
- Power BI Desktop or Tableau Public

## Quick Start (Local)
```bash
git clone <repository-url>
cd ecommerce-analytics-data-pipeline-23A91A1228
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file from `.env.example` and update credentials.

## Configuration
- Database and pipeline settings live in [config/config.yaml](config/config.yaml).
- Database credentials are loaded from `.env` or your environment.

## Pipeline Steps
1. Generate data to `data/raw/`
2. Ingest raw CSVs into `staging` schema
3. Validate data quality and produce `data/processed/quality_report.json`
4. Transform staging into `production`
5. Build warehouse dimensions/facts/aggregates
6. Export analytics CSVs

## Run the Pipeline
```bash
python -m scripts.pipeline_orchestrator
```

Run individual steps:
```bash
python -m scripts.data_generation.generate_data
python -m scripts.ingestion.ingest_to_staging
python -m scripts.quality_checks.validate_data
python -m scripts.transformation.staging_to_production
python -m scripts.transformation.load_warehouse
python -m scripts.transformation.generate_analytics
```

## Docker
```bash
docker compose -f docker/docker-compose.yml up --build
```

## Backend API
```bash
uvicorn src.api.app:app --reload
```

API docs: [docs/api_documentation.md](docs/api_documentation.md)

## Tests
```bash
pytest -q
```

## BI Dashboards
- Power BI: [dashboards/powerbi](dashboards/powerbi)
- Tableau: [dashboards/tableau](dashboards/tableau)
- Streamlit demo: [scripts/transformation/dashboard.py](scripts/transformation/dashboard.py)

## Documentation
- Architecture: [docs/architecture.md](docs/architecture.md)
- Dashboard guide: [docs/dashboard_guide.md](docs/dashboard_guide.md)
- API reference: [docs/api_documentation.md](docs/api_documentation.md)

## Troubleshooting
- Ensure PostgreSQL is running and credentials in `.env` match.
- If tables already exist, drop schemas or re-run DDL scripts.

## Repository Structure
- `scripts/`: pipeline scripts
- `sql/`: DDL/DML and analytical queries
- `data/`: raw, staging, processed outputs

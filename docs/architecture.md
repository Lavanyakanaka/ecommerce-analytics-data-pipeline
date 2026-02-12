## Architecture Overview

This project implements a layered analytics pipeline with clear schema boundaries:

1. **Raw** CSV data is generated in `data/raw/`.
2. **Staging** schema holds raw ingested tables.
3. **Production** schema stores cleansed, validated tables.
4. **Warehouse** schema contains dimensions, facts, and aggregates.
5. **Analytics exports** are written to `data/processed/analytics/` for BI tools.

### Components
- **Data generation**: `scripts/data_generation/generate_data.py`
- **Ingestion**: `scripts/ingestion/ingest_to_staging.py`
- **Quality checks**: `scripts/quality_checks/validate_data.py`
- **Transformation**: `scripts/transformation/staging_to_production.py`
- **Warehouse load**: `scripts/transformation/load_warehouse.py`
- **Analytics exports**: `scripts/transformation/generate_analytics.py`
- **Orchestration**: `scripts/pipeline_orchestrator.py`
- **Monitoring**: `scripts/monitoring/pipeline_monitor.py`

### Data Flow
1. Generate synthetic data.
2. Ingest CSVs into staging tables.
3. Run data quality checks and output a report.
4. Cleanse and load into production tables.
5. Build warehouse dimensions and facts with aggregates.
6. Export analytics to CSV for dashboards.

### Schema Highlights
- **Production**: normalized tables with audit columns and indexes.
- **Warehouse**: star schema with SCD Type 2 columns in dimensions.
- **Aggregates**: daily, monthly, and category summaries for BI.

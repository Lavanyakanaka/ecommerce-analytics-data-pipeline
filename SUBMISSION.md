## Submission Summary

This submission includes a complete analytics pipeline with orchestration, quality checks, warehouse modeling, BI exports, and documentation.

### Highlights
- Synthetic data generation with referential integrity checks
- Staging and production ETL with cleansing and business rules
- Warehouse star schema with aggregates
- Monitoring and analytics exports
- Docker deployment and CI

### How to Run
```bash
python -m scripts.pipeline_orchestrator
```

### Key Files
- `scripts/pipeline_orchestrator.py`
- `scripts/ingestion/ingest_to_staging.py`
- `scripts/transformation/staging_to_production.py`
- `scripts/transformation/load_warehouse.py`
- `sql/ddl/*.sql`
- `sql/queries/*.sql`

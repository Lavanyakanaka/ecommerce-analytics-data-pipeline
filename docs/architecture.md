# Data Pipeline Architecture

## System Overview

Three-tier architecture: Staging → Production → Warehouse

### Layer 1: Staging
- Raw CSV data ingestion
- Minimal constraints
- Quick bulk loading

### Layer 2: Production
- Normalized 3NF design
- Full validation
- Business rules applied

### Layer 3: Warehouse
- Star schema (Kimball)
- Denormalized for analytics
- SCD Type 2 dimensions

## Technology Stack
- PostgreSQL 14
- Python 3.9 (Pandas, SQLAlchemy)
- Docker & Docker Compose

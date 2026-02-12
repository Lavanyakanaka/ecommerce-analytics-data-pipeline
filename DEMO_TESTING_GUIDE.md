# 🎥 Demo Video & Backend Testing Guide

## ✅ Backend Verification Checklist

### 1. **Check Backend Service is Running**
```powershell
# Open browser to API documentation
start http://localhost:8000/docs
```

### 2. **Test All API Endpoints**

#### Health Check
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/health"
# Expected: {"status": "ok"}
```

#### Analytics Summary
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/analytics/summary"
# Expected: Total orders, quantity, revenue
# Current Output: 396 orders, 1,227 items, $322,731.20
```

#### Top Products
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/analytics/top-products?limit=5"
# Expected: Top 5 products by revenue with category
```

#### Monthly Sales Trend
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/analytics/monthly-trend"
# Expected: Year, month, revenue breakdown
```

#### Category Summary
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/analytics/category-summary"
# Expected: Orders, quantity, sales by category
```

---

## 🎬 Demo Video Script (5-8 minutes)

### **Part 1: Introduction (30 seconds)**
- **Show**: Project folder structure
- **Say**: "This is an end-to-end e-commerce analytics data pipeline with ETL, data warehouse, quality checks, and REST API"
- **Display**: README.md overview

### **Part 2: Database Setup (45 seconds)**
```powershell
# Show PostgreSQL is running
Get-Service postgresql-x64-15

# Show database structure
psql -U postgres -d ecommerce_db -c "\dn"  # Show schemas
psql -U postgres -d ecommerce_db -c "\dt staging.*"  # Staging tables
psql -U postgres -d ecommerce_db -c "\dt production.*"  # Production tables
psql -U postgres -d ecommerce_db -c "\dt warehouse.*"  # Warehouse tables
```

### **Part 3: Pipeline Execution (2 minutes)**
```powershell
# Run the full ETL pipeline
python -m scripts.pipeline_orchestrator
```

**Point out during execution:**
1. ✅ Data generation (customers, products, transactions)
2. ✅ Ingestion to staging
3. ✅ Quality checks (100% score)
4. ✅ Transformation to production
5. ✅ Warehouse build (star schema)
6. ✅ Analytics export

**Show the execution report:**
```powershell
cat data/processed/pipeline_execution_report.json | ConvertFrom-Json | ConvertTo-Json -Depth 10
```

### **Part 4: Data Quality Verification (1 minute)**
```powershell
# Show quality report
cat data/processed/quality_report.json | ConvertFrom-Json | ConvertTo-Json -Depth 10
```

**Highlight:**
- 100% quality score
- All checks passed (nulls, duplicates, referential integrity, data ranges)

### **Part 5: Database Inspection (1 minute)**
```powershell
# Show warehouse data
psql -U postgres -d ecommerce_db -c "SELECT * FROM warehouse.dim_customers LIMIT 5;"
psql -U postgres -d ecommerce_db -c "SELECT * FROM warehouse.fact_sales LIMIT 5;"
psql -U postgres -d ecommerce_db -c "SELECT * FROM warehouse.agg_sales_category;"
```

### **Part 6: Backend API Demo (2 minutes)**

#### Start Backend (if not running)
```powershell
# Show backend starting
python -m uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

#### Test API in Browser
```
Open: http://localhost:8000/docs
```

**Demonstrate each endpoint:**

1. **Health Check**
   - Click on `/health`
   - Click "Try it out" → "Execute"
   - Show response: `{"status": "ok"}`

2. **Analytics Summary**
   - Click on `/analytics/summary`
   - Execute
   - Show: Total orders, quantity, revenue

3. **Top Products**
   - Click on `/analytics/top-products`
   - Set limit = 5
   - Execute
   - Show: Top 5 products with revenue

4. **Monthly Trend**
   - Click on `/analytics/monthly-trend`
   - Execute
   - Show: Monthly sales breakdown

5. **Category Summary**
   - Click on `/analytics/category-summary`
   - Execute
   - Show: Sales by category (Clothing, Home, Electronics)

#### Alternative: Command Line Demo
```powershell
# All endpoints working
Invoke-RestMethod -Uri "http://localhost:8000/health"
Invoke-RestMethod -Uri "http://localhost:8000/analytics/summary"
Invoke-RestMethod -Uri "http://localhost:8000/analytics/top-products?limit=5"
Invoke-RestMethod -Uri "http://localhost:8000/analytics/monthly-trend"
Invoke-RestMethod -Uri "http://localhost:8000/analytics/category-summary"
```

### **Part 7: Testing Suite (45 seconds)**
```powershell
# Run all tests
python -m pytest -v

# Show test coverage
python -m pytest --cov=scripts --cov-report=term
```

**Highlight:**
- 9 tests passed
- Coverage across data generation, ingestion, transformation, quality checks

### **Part 8: Docker Demonstration (30 seconds)**
```powershell
# Show Docker configuration
cat docker/docker-compose.yml

# Optional: Build and run
cd docker
docker-compose up --build
```

### **Part 9: Documentation Tour (30 seconds)**

**Show files:**
- `README.md` - Setup and run instructions
- `SUBMISSION.md` - Project completion guide
- `docs/architecture.md` - Technical design
- `docs/api_documentation.md` - API reference
- `docs/dashboard_guide.md` - Dashboard integration
- `sql/queries/analytical_queries.sql` - Sample business queries

### **Part 10: Conclusion (30 seconds)**

**Summary checklist:**
- ✅ Complete ETL pipeline with orchestration
- ✅ Star schema data warehouse with SCD Type 2
- ✅ 100% quality score (10 automated checks)
- ✅ REST API with 5 analytics endpoints
- ✅ 9 passing tests
- ✅ Docker deployment ready
- ✅ Comprehensive documentation

---

## 🔧 Quick Backend Troubleshooting

### Check if backend is running
```powershell
Get-Process | Where-Object {$_.ProcessName -like "*python*"}
```

### Start backend manually
```powershell
python -m uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

### Check port availability
```powershell
netstat -ano | findstr :8000
```

### View backend logs
Check terminal where uvicorn is running for request logs

---

## 📊 Expected Results Summary

### Pipeline Metrics
- **Data Generated**: 100 customers, 50 products, 200 transactions, ~400 transaction items
- **Quality Score**: 100% (10/10 checks passed)
- **Warehouse Tables**: 
  - dim_customers: 100 rows
  - dim_products: 50 rows
  - dim_payment_method: 3 rows
  - dim_date: ~42 rows
  - fact_sales: ~400 rows
  - agg_sales_daily: ~42 rows
  - agg_sales_monthly: 2 rows
  - agg_sales_category: 3 rows

### API Response Times
- Health check: < 50ms
- Analytics endpoints: 100-300ms (depending on query complexity)

### Test Results
- Total tests: 9
- Passed: 9
- Failed: 0
- Execution time: < 3 seconds

---

## 📹 Recording Tips

### Software Recommendations
- **Screen Recording**: OBS Studio, Camtasia, or Windows Game Bar (Win+G)
- **Video Editing**: DaVinci Resolve (free), Camtasia, or Adobe Premiere
- **Resolution**: 1920x1080 (Full HD)
- **Frame Rate**: 30 FPS minimum

### Recording Best Practices
1. **Clean Desktop**: Close unnecessary windows
2. **Zoom Terminal**: Increase font size (Ctrl + +)
3. **Zoom Browser**: Increase zoom to 125-150%
4. **Narration**: Speak clearly and explain each step
5. **Pacing**: Don't rush - allow viewers to read outputs
6. **Cursor**: Use a cursor highlighter tool if possible
7. **Mistakes**: Edit out errors or pause and restart that section

### Presentation Style
- Start with a brief overview (30 seconds)
- Show code structure before execution
- Explain what you expect before running commands
- Point out key metrics and success indicators
- End with summary of achievements

---

## 📦 Submission Checklist

Before submitting:

### Code Quality
- [ ] All Python files have proper imports
- [ ] No hardcoded credentials (use config.yaml)
- [ ] Comments explain complex logic
- [ ] Functions have docstrings

### Functionality
- [ ] Pipeline runs end-to-end without errors
- [ ] All 6 steps complete successfully
- [ ] Quality score is 100%
- [ ] All API endpoints return data
- [ ] All tests pass

### Documentation
- [ ] README.md has setup instructions
- [ ] SUBMISSION.md exists
- [ ] API documentation is complete
- [ ] Architecture diagram or description exists

### Deliverables
- [ ] Complete source code
- [ ] requirements.txt with all dependencies
- [ ] Docker configuration
- [ ] SQL DDL scripts
- [ ] Test suite
- [ ] Demo video (5-8 minutes)
- [ ] Documentation files

---

## 🎯 Evaluation Criteria Coverage

| Criteria | Status | Evidence |
|----------|--------|----------|
| Data Generation | ✅ | `scripts/data_generation/generate_data.py` |
| Data Ingestion | ✅ | `scripts/ingestion/ingest_to_staging.py` |
| Data Transformation | ✅ | `scripts/transformation/staging_to_production.py` |
| Data Warehouse | ✅ | `scripts/transformation/load_warehouse.py` |
| Quality Checks | ✅ | `scripts/quality_checks/validate_data.py` |
| Orchestration | ✅ | `scripts/pipeline_orchestrator.py` |
| Backend API | ✅ | `src/api/app.py` |
| Testing | ✅ | `tests/test_*.py` (9 tests) |
| Docker | ✅ | `docker/docker-compose.yml` |
| Documentation | ✅ | README, SUBMISSION, API docs, architecture |

---

## 💡 Extra Points Opportunities

### Advanced Features to Highlight
1. **Retry Logic**: Pipeline orchestrator retries failed steps 4 times
2. **SCD Type 2**: Dimension tables track historical changes
3. **Data Quality Framework**: 10 automated checks with scoring
4. **Star Schema**: Optimized for analytics queries
5. **FK Constraints**: Referential integrity in warehouse
6. **Aggregate Tables**: Pre-computed analytics for performance
7. **Comprehensive Tests**: Unit tests for all major components
8. **API Documentation**: Auto-generated Swagger UI at /docs
9. **Containerization**: Docker deployment ready
10. **Monitoring**: Execution reports and quality reports

---

## 📞 Support

If you encounter issues during demo recording:

### Common Problems
1. **Pipeline fails**: Check PostgreSQL service is running
2. **API won't start**: Check port 8000 is available
3. **Tests fail**: Ensure all dependencies installed
4. **Database connection**: Verify credentials in config.yaml

### Quick Fixes
```powershell
# Restart PostgreSQL
Restart-Service postgresql-x64-15

# Reinstall dependencies
pip install -r requirements.txt

# Re-create database
python -c "from scripts.db_connection import get_connection; import psycopg2; conn = psycopg2.connect('host=localhost user=postgres password=YourNewPassword'); conn.autocommit = True; cur = conn.cursor(); cur.execute('DROP DATABASE IF EXISTS ecommerce_db'); cur.execute('CREATE DATABASE ecommerce_db'); cur.close(); conn.close()"

# Re-run DDL scripts
Get-ChildItem sql/ddl/*.sql | Sort-Object Name | ForEach-Object { psql -U postgres -d ecommerce_db -f $_.FullName }
```

---

**Good luck with your demo! You have a complete, production-ready data pipeline! 🚀**

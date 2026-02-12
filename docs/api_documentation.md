## API Documentation

Base URL: `http://localhost:8000`

### Run the API
```bash
uvicorn src.api.app:app --reload
```

### Endpoints

#### GET /health
Returns API health status.

Response:
```json
{"status": "ok"}
```

#### GET /analytics/top-products?limit=10
Top products by revenue.

Response fields:
- `product_name`
- `category`
- `total_revenue`

#### GET /analytics/monthly-trend
Monthly revenue trend.

Response fields:
- `year`
- `month`
- `revenue`

#### GET /analytics/category-summary
Aggregated sales by category.

Response fields:
- `category`
- `total_orders`
- `total_quantity`
- `total_sales`

#### GET /analytics/summary
Overall sales summary.

Response fields:
- `total_orders`
- `total_quantity`
- `total_revenue`

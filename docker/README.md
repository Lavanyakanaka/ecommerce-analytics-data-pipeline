## Docker Deployment

### Build and Run
```bash
docker compose -f docker/docker-compose.yml up --build
```

### Services
- `postgres`: PostgreSQL database with health check
- `pipeline`: Runs the full pipeline using `scripts.pipeline_orchestrator`

### Environment
Create `.env` from `.env.example` and set the database credentials.

### Volumes
- `data/` and `logs/` are mounted into the container for outputs.

# Backend

FastAPI API scaffold for the Bookmaker Mistake Detector.

## Run Locally
```bash
uvicorn bookmaker_detector_api.main:app --host 0.0.0.0 --port 8000
```

## Current Endpoints
- `GET /api/v1/health`

## PostgreSQL Schema Authority
- The current schema authority is the ordered SQL bootstrap chain under `infra/postgres/init/`.
- Alembic has not been introduced yet.
- Normal API and worker runtime must assume the schema is already prepared.
- Introduce migration tooling when an existing populated environment needs forward-only schema evolution that cannot be handled by rebuilding from the bootstrap SQL chain.

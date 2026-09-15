# Alerting API Service

A FastAPI service that ingests time-series infrastructure metrics, evaluates threshold alert rules asynchronously, and persists violations to Postgres.

## Setup

```bash
git clone <repo>
cd <repo>
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```
DATABASE_URL=postgresql://user:password@host:port/dbname
```

## Run

```bash
uvicorn main:app --reload
```

The app creates all four database tables on startup (`metric_readings`, `alert_rules`, `rule_states`, `alert_violations`).

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/metrics/ingest` | Ingest a batch of readings (partial-accept) |
| `GET` | `/metrics/query` | Query raw or aggregated readings over a time window |
| `POST` | `/alerts/rules` | Create an alert rule |
| `GET` | `/alerts/rules` | List all alert rules |
| `GET` | `/alerts/violations` | List all alert violations |

## Tests

```bash
pip install -r requirements-test.txt
pytest test_main.py -v
```

Tests run against a real Postgres database (uses `DATABASE_URL` from `.env`). CI spins up a throwaway `postgres:16` service container instead of using the dev database.

See [ARCHITECTURE.md](ARCHITECTURE.md) for design decisions.

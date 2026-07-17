# Cloudcost Analyzer

A multi-cloud FinOps pipeline that ingests cost data from AWS, Azure, and GCP
plus revenue data from Stripe, unifies it in Postgres, and exposes it through
a REST API and Rill dashboards. The goal is a single place to answer "what
are we spending, per cloud, per service, and how does that compare to
revenue" without stitching together three different billing consoles by
hand.

## Architecture

```
AWS CUR (S3 parquet)  ─┐
Azure Cost Mgmt API    ─┼─> dlt pipelines ─> Postgres (raw schemas) ─> dbt staging models ─┐
GCP BigQuery export    ─┤                                                                   ├─> unified_cost_model ─┬─> FastAPI (api/)
Stripe API             ─┘                                                                   ┘  stg_stripe_revenue ─┘   Rill dashboards (viz_rill/dashboards)
```

- **`pipelines/`** — one [dlt](https://dlthub.com/) script per source
  (`aws_pipeline.py`, `azure_pipeline.py`, `gcp_pipeline.py`,
  `stripe_pipeline.py`). Each loads raw records into its own Postgres schema
  (`aws_costs`, `azure_costs`, `gcp_costs`, `stripe_revenue`) using merge
  write disposition, so re-running a pipeline upserts rather than
  duplicates.
- **`viz_rill/`** — a dbt project. `models/staging/` normalizes each raw
  schema into a common shape (`cost_id`, `usage_date`, `service_name`,
  `cost_usd`); `models/unified_cost_model.sql` unions the three clouds into
  one table. `viz_rill/dashboards/` is a Rill project reading from the same
  Postgres database for exploratory dashboards.
- **`api/`** — a FastAPI app (`api/main.py`) that serves `unified_cost_model`
  and `stg_stripe_revenue` over HTTP (cost summaries, top services, unit
  economics), with Redis as a best-effort read-through cache and a static
  API key required on every route.
- **`.github/workflows/etl-pipeline.yml`** — runs all four pipelines daily
  against Postgres in CI.

## Prerequisites

- Python 3.10 (pinned in `.python-version`)
- [uv](https://docs.astral.sh/uv/) for dependency management
- PostgreSQL (tested against 14/16) and Redis, running locally
- Credentials for whichever cloud sources you actually want to pull from
  (AWS CUR bucket access, an Azure service principal with Cost Management
  Reader, a GCP service account with BigQuery access, a Stripe API key) —
  not required just to explore the repo, only to run the pipelines for real

## Setup

### 1. Install dependencies

```bash
uv sync
```

This creates `.venv/` and installs everything in `pyproject.toml`,
including `dlt[parquet]` (pulls in pyarrow, needed for the AWS/GCP
parquet-based pipelines).

### 2. Postgres

Create the database the pipelines and dbt both write to:

```bash
createdb finops
```

(Or point every config at whatever host/db you already have — see step 4.)

### 3. Redis

Start Redis locally (e.g. `brew services start redis` or `redis-server`).
Redis is optional in the sense that the API fails open on a cache miss or
outage — it just runs uncached.

### 4. Configure secrets

Two separate config systems need credentials: dlt (for the pipelines) and
the FastAPI app.

**`.dlt/secrets.toml`** (gitignored — create this file yourself; non-secret
defaults already live in the tracked `.dlt/config.toml`):

```toml
[destination.postgres.credentials]
database = "finops"
username = "postgres"
password = "<your-postgres-password>"
host = "localhost"
port = 5432

[sources.filesystem.credentials]
aws_access_key_id = "<your-aws-key>"
aws_secret_access_key = "<your-aws-secret>"

[sources.azure_cur]
tenant_id = "<azure-tenant-id>"
client_id = "<azure-app-client-id>"
client_secret = "<azure-app-client-secret>"

[sources.stripe]
api_key = "<stripe-secret-key>"
```

GCP auth doesn't go through `secrets.toml` — `gcp_pipeline.py` uses
Application Default Credentials, so run:

```bash
gcloud auth application-default login
```

**`.env`** (gitignored — copy `.env.example` and fill in values) for the
FastAPI app:

```bash
cp .env.example .env
```

```
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=<your-postgres-password>
DB_NAME=finops

REDIS_HOST=localhost
REDIS_PORT=6379

API_KEY=<pick-any-random-string-clients-must-send-in-X-API-Key>
```

### 5. dbt profile

dbt looks for a profile named `cloudcost_analyzer` (see
`viz_rill/dbt_project.yml`). Create `~/.dbt/profiles.yml`:

```yaml
cloudcost_analyzer:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      pass: <your-postgres-password>
      dbname: finops
      schema: analytics
      threads: 4
```

## Running it

**Ingest data** — run whichever pipelines you have credentials for (each is
independent):

```bash
uv run python pipelines/aws_pipeline.py
uv run python pipelines/azure_pipeline.py
uv run python pipelines/gcp_pipeline.py
uv run python pipelines/stripe_pipeline.py
```

**Build the dbt models** (needs at least one pipeline's raw schema to exist
in Postgres, since the staging models `source()` those tables):

```bash
cd viz_rill
uv run dbt run
cd ..
```

**Serve the API**:

```bash
uv run uvicorn api.main:app --reload
```

Every route requires an `X-API-Key` header matching your `.env`'s
`API_KEY`. Try it:

```bash
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/costs/summary"
```

**Rill dashboards** (optional, exploratory UI on top of the same Postgres
data):

```bash
cd viz_rill/dashboards
rill start
```

Requires the [Rill CLI](https://docs.rilldata.com/get-started/install) and
a `.env` in that directory with `connector.postgres.database_url` set (see
`viz_rill/dashboards/connectors/postgres.yaml`).

## Tests

```bash
uv run pytest
```

Tests cover CSV/schema parsing, merge-key and null-handling behavior for
each pipeline, the cache/database helpers, and the API routes — they mock
external services (Azure/GCP/Stripe HTTP calls, Redis, Postgres) rather
than requiring live infrastructure.

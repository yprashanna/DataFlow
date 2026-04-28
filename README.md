# 🔀 DataFlow

[![100% FREE](https://img.shields.io/badge/cost-100%25%20FREE-brightgreen?style=flat-square)](https://github.com)
[![No paid APIs](https://img.shields.io/badge/APIs-no%20paid%20APIs-blue?style=flat-square)](https://github.com)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue?style=flat-square)](https://python.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow?style=flat-square)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/YOUR_USERNAME/dataflow/ci.yml?style=flat-square)](https://github.com/YOUR_USERNAME/dataflow/actions)

**Scalable data pipeline orchestration platform — 100% FREE. No paid APIs, no paid databases, no paid cloud.**

Define pipelines in YAML. Run them on cron schedules. Monitor everything in a Streamlit dashboard. Powered by SQLite, Pandas, APScheduler, and FastAPI — all free, all local.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DataFlow Platform                        │
│                                                                 │
│  ┌──────────────┐    ┌───────────────────────────────────────┐  │
│  │  YAML Config │───▶│            Pipeline Runner            │  │
│  │  (configs/)  │    │                                       │  │
│  └──────────────┘    │  1. Ingest  ──▶ CSV / API / SQLite   │  │
│                      │  2. Validate ─▶ Nulls/Schema/Range   │  │
│  ┌──────────────┐    │  3. Clean   ──▶ Dedup/Cast/Filter    │  │
│  │  APScheduler │───▶│  4. Transform ▶ Columns/Dates/Bins   │  │
│  │  (cron jobs) │    │  5. Aggregate ▶ GroupBy/Agg          │  │
│  └──────────────┘    │  6. Load    ──▶ SQLite Warehouse      │  │
│                      └───────────────────────────────────────┘  │
│                                        │                        │
│  ┌──────────────┐    ┌─────────────────▼─────────────────────┐  │
│  │   FastAPI    │    │         SQLite (FREE)                  │  │
│  │  REST API    │    │  ┌─────────────┐  ┌────────────────┐  │  │
│  │  :8000       │    │  │ warehouse.db│  │  metadata.db   │  │  │
│  └──────────────┘    │  │ (your data) │  │ (run history)  │  │  │
│                      │  └─────────────┘  └────────────────┘  │  │
│  ┌──────────────┐    └───────────────────────────────────────┘  │
│  │  Streamlit   │                      │                        │
│  │  Dashboard   │◀─────────────────────┘                        │
│  │  :8501       │    Health · Latency · Quality · History        │
│  └──────────────┘                                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quick Start (3 commands)

```bash
# 1. Install
make install

# 2. Run the sample CSV pipeline
make run-pipeline

# 3. Open the monitoring dashboard
make dashboard
# → http://localhost:8501
```

That's it. You now have a running data pipeline with a monitoring dashboard.

---

## Full Stack Startup

```bash
# Terminal 1 — FastAPI (pipeline trigger API)
make api
# → http://localhost:8000/docs

# Terminal 2 — APScheduler (cron-based auto-runner)
make schedule

# Terminal 3 — Streamlit dashboard
make dashboard
# → http://localhost:8501

# Or run everything in Docker:
make docker-up
```

---

## Tech Stack (ALL FREE)

| Component | Technology | Cost |
|-----------|------------|------|
| Data manipulation | Pandas | FREE |
| Data warehouse | SQLite | FREE |
| Run metadata | SQLite | FREE |
| Data validation | Custom + Great Expectations | FREE |
| Scheduling | APScheduler | FREE |
| REST API | FastAPI + Uvicorn | FREE |
| Dashboard | Streamlit | FREE |
| API ingestion | Requests | FREE |
| Pipeline config | YAML | FREE |
| Containerization | Docker | FREE |
| CI/CD | GitHub Actions | FREE |
| Cloud API | [Open-Meteo](https://open-meteo.com) (no key needed) | FREE |
| Dashboard hosting | [Streamlit Cloud](https://streamlit.io/cloud) | FREE |
| API hosting | [Render free tier](https://render.com) | FREE |

---

## Directory Structure

```
dataflow/
├── pipelines/
│   ├── ingestion/
│   │   ├── csv_source.py       # CSV file ingestion
│   │   ├── api_source.py       # REST API ingestion (with retry/backoff)
│   │   └── db_source.py        # SQLite/SQL database ingestion
│   ├── transformation/
│   │   ├── cleaner.py          # Nulls, dedup, type casting, filters
│   │   ├── transformer.py      # Derived columns, normalization, binning
│   │   └── aggregator.py       # GroupBy aggregations
│   ├── loading/
│   │   └── sqlite_loader.py    # SQLite data warehouse loader
│   └── validation/
│       └── validator.py        # Data quality checks engine
├── orchestrator/
│   ├── scheduler.py            # APScheduler cron runner
│   ├── runner.py               # Full pipeline executor
│   └── config_parser.py        # YAML config loader
├── monitoring/
│   ├── health.py               # Health metric aggregation
│   ├── alerts.py               # Email/log alerting
│   └── metadata.py             # SQLite run history store
├── api/
│   └── main.py                 # FastAPI REST endpoints
├── ui/
│   └── app.py                  # Streamlit monitoring dashboard
├── configs/
│   ├── sample_csv_pipeline.yml         # Sample CSV pipeline
│   ├── sample_api_pipeline.yml         # Sample Open-Meteo API pipeline
│   └── pipeline_schema.yml             # Full config reference
├── data/
│   ├── sample_input.csv        # 2,250 rows of realistic sales data
│   ├── warehouse.db            # Auto-created SQLite warehouse
│   └── metadata.db             # Auto-created run history DB
├── tests/                      # pytest test suite
├── .github/workflows/ci.yml    # GitHub Actions CI
├── Dockerfile                  # Multi-stage Docker build
├── docker-compose.yml          # All services
├── Makefile                    # make install / run / test etc.
└── requirements.txt
```

---

## How to Create a New Pipeline

1. **Create a YAML config** in `configs/`:

```yaml
name: my_pipeline                        # unique name — used as table suffix
description: "What this pipeline does"

schedule:
  cron: "0 8 * * *"                     # 8 AM UTC daily (omit for manual-only)

source:
  type: csv                              # csv | api | sqlite
  file_path: data/my_data.csv

validation:
  checks:
    - type: not_null
      columns: [id, value]
      threshold: 0.99
    - type: range
      column: value
      min: 0
      max: 1000000
    - type: row_count
      min_rows: 10

transformation:
  cleaning:
    drop_nulls: [id]
    drop_duplicates:
      subset: [id]
      keep: first
    cast_types:
      value: float
      created_at: datetime

  transforms:
    - type: extract_date_parts
      column: created_at
      parts: [year, month, quarter]

  aggregation:
    group_by: [year, month]
    aggs:
      value: sum
    rename_aggs:
      value_sum: total_value

loading:
  destination: data/warehouse.db
  table: my_pipeline_output
  if_exists: append                      # append | replace | fail
```

2. **Run it manually:**
```bash
python -c "
from orchestrator.config_parser import load_pipeline_config
from orchestrator.runner import PipelineRunner
config = load_pipeline_config('configs/my_pipeline.yml')
PipelineRunner(config).run()
"
```

3. **Or trigger via API:**
```bash
curl -X POST http://localhost:8000/pipelines/my_pipeline/run
```

The scheduler picks up the cron schedule automatically on next restart.

---

## Sample Pipeline Walkthrough

### CSV Sales Pipeline (`sample_csv_pipeline.yml`)

```
Source: data/sample_input.csv (2,250 rows of sales data)
   │
   ▼
Validate: schema check, null check (order_id, customer_id), 
          range checks (unit_price > 0, quantity > 0),
          accepted values (status, region)
   │
   ▼
Clean:  drop null order_ids, fill null ratings with 3.0,
        deduplicate on order_id, cast types, filter unit_price > 0
   │
   ▼
Transform: add revenue column (quantity × unit_price),
           extract year/month/quarter from sale_date,
           uppercase region
   │
   ▼
Aggregate: GROUP BY region, category, year, month
           SUM revenue, SUM quantity, COUNT orders, AVG unit_price
   │
   ▼
Load → SQLite: data/warehouse.db → table: sales_regional_agg
```

Run time: ~200–400ms on a laptop. ~40 output rows from 2,250 input.

### Weather API Pipeline (`sample_api_pipeline.yml`)

Uses the [Open-Meteo API](https://open-meteo.com) — free, no API key, no rate limits at this scale.

```
Source: https://api.open-meteo.com/v1/forecast
        (hourly temperature + wind for NYC, 3-day forecast)
   │
   ▼
Validate: ≥24 rows, non-null time + temperature
   │
   ▼
Clean: cast types, fill null windspeed with 0
   │
   ▼
Transform: add temp_fahrenheit column,
           bin temperature into freezing/cold/mild/warm/hot
   │
   ▼
Load → SQLite: data/warehouse.db → table: weather_forecast_hourly
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | System health + pipeline stats |
| GET | `/pipelines` | List all pipeline configs |
| POST | `/pipelines/{name}/run` | Trigger a pipeline (async) |
| GET | `/pipelines/{name}/status` | Latest run status |
| GET | `/pipelines/{name}/runs` | Run history |
| GET | `/runs` | All recent runs |
| GET | `/stats` | Aggregated stats per pipeline |

Full interactive docs: `http://localhost:8000/docs`

---

## Validation Rules Reference

| Rule | Description | Config Example |
|------|-------------|----------------|
| `not_null` | Null rate below threshold | `threshold: 0.99` |
| `range` | Numeric min/max bounds | `min: 0, max: 9999` |
| `unique` | No duplicate values | `column: order_id` |
| `schema` | Required columns present | `expected_columns: [id, name]` |
| `accepted_values` | Allowlist of valid values | `values: [active, inactive]` |
| `regex` | Pattern match | `pattern: "^[A-Z]{2}\\d{4}$"` |
| `row_count` | Minimum rows | `min_rows: 100` |

---

## Deployment

### Streamlit Cloud (Dashboard — FREE)

1. Push your repo to GitHub
2. Go to [streamlit.io/cloud](https://streamlit.io/cloud) → New app
3. Set **Main file path**: `ui/app.py`
4. Set `DATAFLOW_API_URL` in Streamlit secrets to your Render API URL

### Render Free Tier (API — FREE)

1. Go to [render.com](https://render.com) → New Web Service
2. Connect your GitHub repo
3. Set **Build command**: `pip install -r requirements.txt && python generate_sample_data.py`
4. Set **Start command**: `uvicorn api.main:app --host 0.0.0.0 --port $PORT`
5. Add environment variables from `.env.example`

> **Note:** Render free tier spins down after 15 min inactivity. The first request after sleep takes ~30s. Use [UptimeRobot](https://uptimerobot.com) (free) to ping `/health` every 10 min to keep it awake.

### Docker (Self-hosted)

```bash
make docker-up
# API:       http://localhost:8000
# Dashboard: http://localhost:8501
# Docs:      http://localhost:8000/docs
```

---

## Running Tests

```bash
make test
# or
pytest tests/ -v
```

Tests cover: ingestion sources, validation rules, cleaning/transformation/aggregation, orchestrator config parsing, end-to-end pipeline run, and all FastAPI endpoints.

---

## Adding a New Data Source

1. Create `pipelines/ingestion/my_source.py` extending the same pattern as `CSVSource`
2. Register it in `pipelines/ingestion/__init__.py`
3. Add a `type: my_source` branch in `orchestrator/runner.py → _ingest()`
4. Add YAML config fields to `configs/pipeline_schema.yml`

---

## License

MIT — see [LICENSE](LICENSE)

---

*Built with ❤️ using 100% free, open-source tools.*

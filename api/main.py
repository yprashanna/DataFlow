"""DataFlow FastAPI — pipeline trigger and status API.

Endpoints:
  GET  /health                   → system health
  GET  /pipelines                → list all known pipelines
  POST /pipelines/{name}/run     → trigger a pipeline run
  GET  /pipelines/{name}/status  → latest status for a pipeline
  GET  /pipelines/{name}/runs    → run history
  GET  /runs                     → all recent runs

Deploy on Render free tier — this whole thing runs in a single uvicorn worker.
"""

import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from monitoring.metadata import MetadataStore
from monitoring.health import HealthMonitor
from orchestrator.config_parser import load_pipeline_config, list_pipeline_configs
from orchestrator.runner import PipelineRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="DataFlow API",
    description="Lightweight data pipeline orchestration — 100% free stack",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Allow all origins for local dev / Streamlit Cloud frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

metadata_store = MetadataStore()
health_monitor = HealthMonitor()

CONFIGS_DIR = Path("configs")


# ── Request / Response models ──────────────────────────────────────────────


class PipelineRunResponse(BaseModel):
    run_id: str
    pipeline_name: str
    status: str
    message: str


class RunResult(BaseModel):
    run_id: str
    pipeline_name: str
    status: str
    rows_ingested: Optional[int] = None
    rows_loaded: Optional[int] = None
    quality_score: Optional[float] = None
    total_latency_ms: Optional[float] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


# ── Routes ─────────────────────────────────────────────────────────────────


@app.get("/health", tags=["System"])
def health_check():
    """System health summary."""
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "health": health_monitor.get_overall_health(),
    }


@app.get("/pipelines", tags=["Pipelines"])
def list_pipelines():
    """List all pipeline configs discovered in the configs/ directory."""
    config_files = list_pipeline_configs(CONFIGS_DIR)
    pipelines = []

    for cf in config_files:
        try:
            cfg = load_pipeline_config(cf)
            schedule = cfg.get("schedule", {}).get("cron", "manual")
            pipelines.append({
                "name": cfg["name"],
                "description": cfg.get("description", ""),
                "schedule": schedule,
                "config_file": cf.name,
            })
        except Exception as exc:
            logger.warning("Could not load config %s: %s", cf, exc)

    return {"pipelines": pipelines, "count": len(pipelines)}


@app.post("/pipelines/{pipeline_name}/run", tags=["Pipelines"])
def trigger_pipeline(pipeline_name: str, background_tasks: BackgroundTasks):
    """Trigger a pipeline run asynchronously. Returns immediately with run_id."""
    config_file = CONFIGS_DIR / f"{pipeline_name}.yml"

    # Also try sample_ prefix
    if not config_file.exists():
        config_file = CONFIGS_DIR / f"sample_{pipeline_name}_pipeline.yml"
    if not config_file.exists():
        # Search all configs for matching name
        for cf in list_pipeline_configs(CONFIGS_DIR):
            try:
                cfg = load_pipeline_config(cf)
                if cfg.get("name") == pipeline_name:
                    config_file = cf
                    break
            except Exception:
                continue

    if not config_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"No config found for pipeline '{pipeline_name}'. "
                   f"Expected: configs/{pipeline_name}.yml",
        )

    run_id = f"{pipeline_name}_{int(datetime.now(timezone.utc).timestamp())}"

    def _run():
        try:
            pipeline_config = load_pipeline_config(config_file)
            runner = PipelineRunner(pipeline_config)
            runner.run()
        except Exception as exc:
            logger.error("Background pipeline run failed: %s", exc)

    background_tasks.add_task(_run)

    return PipelineRunResponse(
        run_id=run_id,
        pipeline_name=pipeline_name,
        status="triggered",
        message=f"Pipeline '{pipeline_name}' triggered. Poll /pipelines/{pipeline_name}/status for updates.",
    )


@app.get("/pipelines/{pipeline_name}/status", tags=["Pipelines"])
def pipeline_status(pipeline_name: str):
    """Latest status for a specific pipeline."""
    health = health_monitor.get_pipeline_health(pipeline_name)
    recent = metadata_store.get_recent_runs(pipeline_name=pipeline_name, limit=1)

    if recent.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No runs found for pipeline '{pipeline_name}'",
        )

    last = recent.iloc[0].to_dict()
    return {"health": health, "last_run": last}


@app.get("/pipelines/{pipeline_name}/runs", tags=["Pipelines"])
def pipeline_runs(pipeline_name: str, limit: int = 20):
    """Run history for a specific pipeline."""
    runs_df = metadata_store.get_recent_runs(pipeline_name=pipeline_name, limit=limit)
    if runs_df.empty:
        return {"runs": [], "count": 0}
    return {"runs": runs_df.to_dict(orient="records"), "count": len(runs_df)}


@app.get("/runs", tags=["Runs"])
def all_runs(limit: int = 50):
    """All recent pipeline runs across all pipelines."""
    runs_df = metadata_store.get_recent_runs(limit=limit)
    return {"runs": runs_df.to_dict(orient="records"), "count": len(runs_df)}


@app.get("/stats", tags=["System"])
def pipeline_stats():
    """Aggregated stats per pipeline — useful for dashboards."""
    stats_df = metadata_store.get_pipeline_stats()
    return {"stats": stats_df.to_dict(orient="records")}

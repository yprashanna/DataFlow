"""DataFlow FastAPI — pipeline trigger and status API.

Endpoints:
  GET  /health                   → system health
  GET  /pipelines                → list all known pipelines
  POST /pipelines/{name}/run     → trigger a pipeline run
  GET  /pipelines/{name}/status  → latest status for a pipeline
  GET  /pipelines/{name}/runs    → run history
  GET  /runs                     → all recent runs
  GET  /stats                    → aggregated stats per pipeline
  GET  /test/env                 → check env vars are set
  GET  /test/email               → send a test alert email
  GET  /test/pipeline            → run sample pipeline synchronously
"""

import logging
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from monitoring.metadata import MetadataStore
from monitoring.health import HealthMonitor
from monitoring.alerts import AlertManager
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


# ── Core routes ────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health_check():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "health": health_monitor.get_overall_health(),
    }


@app.get("/pipelines", tags=["Pipelines"])
def list_pipelines():
    config_files = list_pipeline_configs(CONFIGS_DIR)
    pipelines = []
    for cf in config_files:
        try:
            cfg = load_pipeline_config(cf)
            pipelines.append({
                "name": cfg["name"],
                "description": cfg.get("description", ""),
                "schedule": cfg.get("schedule", {}).get("cron", "manual"),
                "config_file": cf.name,
            })
        except Exception as exc:
            logger.warning("Could not load config %s: %s", cf, exc)
    return {"pipelines": pipelines, "count": len(pipelines)}


@app.post("/pipelines/{pipeline_name}/run", tags=["Pipelines"])
def trigger_pipeline(pipeline_name: str, background_tasks: BackgroundTasks):
    config_file = CONFIGS_DIR / f"{pipeline_name}.yml"
    if not config_file.exists():
        config_file = CONFIGS_DIR / f"sample_{pipeline_name}_pipeline.yml"
    if not config_file.exists():
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
            detail=f"No config found for pipeline '{pipeline_name}'.",
        )

    run_id = f"{pipeline_name}_{int(datetime.now(timezone.utc).timestamp())}"

    def _run():
        try:
            pipeline_config = load_pipeline_config(config_file)
            PipelineRunner(pipeline_config).run()
        except Exception as exc:
            logger.error("Background pipeline run failed: %s", exc)

    background_tasks.add_task(_run)
    return PipelineRunResponse(
        run_id=run_id,
        pipeline_name=pipeline_name,
        status="triggered",
        message=f"Pipeline '{pipeline_name}' triggered.",
    )


@app.get("/pipelines/{pipeline_name}/status", tags=["Pipelines"])
def pipeline_status(pipeline_name: str):
    health = health_monitor.get_pipeline_health(pipeline_name)
    recent = metadata_store.get_recent_runs(pipeline_name=pipeline_name, limit=1)
    if recent.empty:
        raise HTTPException(status_code=404, detail=f"No runs found for pipeline '{pipeline_name}'")
    return {"health": health, "last_run": recent.iloc[0].to_dict()}


@app.get("/pipelines/{pipeline_name}/runs", tags=["Pipelines"])
def pipeline_runs(pipeline_name: str, limit: int = 20):
    runs_df = metadata_store.get_recent_runs(pipeline_name=pipeline_name, limit=limit)
    if runs_df.empty:
        return {"runs": [], "count": 0}
    return {"runs": runs_df.to_dict(orient="records"), "count": len(runs_df)}


@app.get("/runs", tags=["Runs"])
def all_runs(limit: int = 50):
    runs_df = metadata_store.get_recent_runs(limit=limit)
    return {"runs": runs_df.to_dict(orient="records"), "count": len(runs_df)}


@app.get("/stats", tags=["System"])
def pipeline_stats():
    stats_df = metadata_store.get_pipeline_stats()
    return {"stats": stats_df.to_dict(orient="records")}


# ── Test / Debug endpoints ─────────────────────────────────────────────────

@app.get("/test/env", tags=["Debug"])
def test_env_check():
    """Shows which environment variables are SET. Safe to share — never shows values."""
    vars_to_check = [
        "ALERT_EMAIL_FROM", "ALERT_EMAIL_TO",
        "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD",
        "DATAFLOW_API_URL",
    ]
    return {v: "✅ SET" if os.getenv(v) else "❌ MISSING" for v in vars_to_check}


@app.get("/test/email", tags=["Debug"])
def test_email_alert():
    """
    Sends a test alert email and returns the exact result — never returns 500.
    Open in browser: https://your-api.onrender.com/test/email
    """
    try:
        alert = AlertManager()

        config_info = {
            "email_from_set": bool(alert.email_from),
            "email_to_set":   bool(alert.email_to),
            "smtp_host":      alert.smtp_host,
            "smtp_port":      alert.smtp_port,
            "smtp_user_set":  bool(alert.smtp_user),
            "smtp_pass_set":  bool(alert.smtp_password),
            "email_enabled":  alert.email_enabled,
        }

        if not alert.email_enabled:
            return {
                "status": "skipped",
                "reason": "One or more required env vars are missing",
                "config": config_info,
                "required_vars": ["ALERT_EMAIL_FROM", "ALERT_EMAIL_TO", "SMTP_USER", "SMTP_PASSWORD"],
            }

        subject = "[DataFlow] Test Alert Email"
        body = (
            f"This is a TEST email from DataFlow.\n\n"
            f"If you received this, your email alerts are working! ✅\n\n"
            f"Sent at: {datetime.now(timezone.utc).isoformat()}\n"
            f"From: {alert.email_from}\n"
            f"To: {alert.email_to}"
        )

        result = alert.send_email_direct(subject, body)

        if result["sent"]:
            return {
                "status": "sent ✅",
                "message": f"Email delivered to {alert.email_to}",
                "next_step": "Check your inbox + spam folder for subject: [DataFlow] Test Alert Email",
                "config": config_info,
            }
        else:
            return {
                "status": "failed ❌",
                "error": result.get("error"),
                "fix": result.get("fix", "Check the error message above"),
                "config": config_info,
            }

    except Exception as exc:
        # This outer catch means /test/email NEVER returns HTTP 500
        return {
            "status": "crashed ❌",
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "note": "This is an unexpected error — please report this",
        }


@app.get("/test/pipeline", tags=["Debug"])
def test_pipeline_run():
    """Runs sample_csv_sales synchronously and returns the result."""
    config_file = CONFIGS_DIR / "sample_csv_pipeline.yml"
    if not config_file.exists():
        return {"status": "error", "reason": "sample_csv_pipeline.yml not found in configs/"}
    try:
        pipeline_config = load_pipeline_config(config_file)
        result = PipelineRunner(pipeline_config).run()
        return {
            "status": result["status"],
            "rows_ingested": result.get("rows_ingested"),
            "rows_loaded": result.get("rows_loaded"),
            "quality_score": result.get("quality_score"),
            "total_latency_ms": result.get("total_latency_ms"),
            "error": result.get("error"),
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc), "traceback": traceback.format_exc()}

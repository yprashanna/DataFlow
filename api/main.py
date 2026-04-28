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


# ── Test / Debug endpoints (safe to call from browser) ────────────────────────

@app.get("/test/email", tags=["Debug"])
def test_email_alert():
    """
    Sends a test alert email to ALERT_EMAIL_TO.
    Call this from your browser to verify SMTP config works:
      GET https://your-api.onrender.com/test/email

    Returns JSON telling you if email is configured and whether send succeeded.
    """
    import os
    alert = AlertManager()

    config_status = {
        "ALERT_EMAIL_FROM": bool(os.getenv("ALERT_EMAIL_FROM")),
        "ALERT_EMAIL_TO":   bool(os.getenv("ALERT_EMAIL_TO")),
        "SMTP_HOST":        os.getenv("SMTP_HOST", "smtp.gmail.com"),
        "SMTP_PORT":        os.getenv("SMTP_PORT", "587"),
        "SMTP_USER":        bool(os.getenv("SMTP_USER")),
        "SMTP_PASSWORD":    bool(os.getenv("SMTP_PASSWORD")),
        "email_enabled":    alert.email_enabled,
    }

    if not alert.email_enabled:
        return {
            "status": "skipped",
            "reason": "Email not configured — one or more env vars missing",
            "config": config_status,
            "fix": "Set ALERT_EMAIL_FROM, ALERT_EMAIL_TO, SMTP_USER, SMTP_PASSWORD in Render environment",
        }

    try:
        alert.send_failure_alert(
            pipeline_name="test_pipeline",
            error_message=(
                "This is a TEST alert from DataFlow.\n"
                "If you received this, your email alerts are working correctly! ✅\n\n"
                f"Sent at: {datetime.now(timezone.utc).isoformat()}"
            ),
        )
        return {
            "status": "sent",
            "message": f"Test email sent to {os.getenv('ALERT_EMAIL_TO')}",
            "check": "Check your inbox (and spam folder) for subject: [DataFlow] Pipeline FAILED: test_pipeline",
            "config": config_status,
        }
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
            "likely_cause": "Wrong app password, or Gmail 2FA not enabled, or less-secure apps blocked",
            "config": config_status,
        }


@app.get("/test/pipeline", tags=["Debug"])
def test_pipeline_run(background_tasks: BackgroundTasks):
    """
    Runs the sample_csv_sales pipeline and returns the result synchronously.
    Use this to verify the full pipeline works end-to-end from your browser.
      GET https://your-api.onrender.com/test/pipeline
    """
    config_file = CONFIGS_DIR / "sample_csv_pipeline.yml"
    if not config_file.exists():
        return {"status": "error", "reason": "sample_csv_pipeline.yml not found in configs/"}

    try:
        from orchestrator.config_parser import load_pipeline_config
        from orchestrator.runner import PipelineRunner
        pipeline_config = load_pipeline_config(config_file)
        runner = PipelineRunner(pipeline_config)
        result = runner.run()
        return {
            "status": result["status"],
            "rows_ingested": result.get("rows_ingested"),
            "rows_loaded": result.get("rows_loaded"),
            "quality_score": result.get("quality_score"),
            "total_latency_ms": result.get("total_latency_ms"),
            "error": result.get("error"),
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.get("/test/env", tags=["Debug"])
def test_env_check():
    """
    Shows which environment variables are SET (not their values — safe to share).
    Use this to debug missing config on Render.
      GET https://your-api.onrender.com/test/env
    """
    import os
    vars_to_check = [
        "ALERT_EMAIL_FROM", "ALERT_EMAIL_TO",
        "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD",
        "DATAFLOW_API_URL",
    ]
    return {
        var: "✅ SET" if os.getenv(var) else "❌ MISSING"
        for var in vars_to_check
    }

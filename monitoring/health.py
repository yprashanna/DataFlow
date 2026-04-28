"""Pipeline health metrics computation.

Aggregates run history into health scores used by the dashboard and API.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from monitoring.metadata import MetadataStore

logger = logging.getLogger(__name__)


class HealthMonitor:
    """Computes health metrics for pipelines from run history."""

    def __init__(self):
        self.store = MetadataStore()

    def get_overall_health(self) -> dict:
        """System-wide health summary."""
        stats_df = self.store.get_pipeline_stats()

        if stats_df.empty:
            return {
                "total_pipelines": 0,
                "total_runs": 0,
                "overall_success_rate": None,
                "avg_quality_score": None,
                "pipelines": [],
            }

        total_runs = int(stats_df["total_runs"].sum())
        successful_runs = int(stats_df["successful_runs"].sum())
        success_rate = round(successful_runs / total_runs * 100, 1) if total_runs > 0 else 0.0
        avg_quality = round(stats_df["avg_quality_score"].mean(), 1)

        return {
            "total_pipelines": len(stats_df),
            "total_runs": total_runs,
            "overall_success_rate": success_rate,
            "avg_quality_score": avg_quality,
            "pipelines": stats_df.to_dict(orient="records"),
        }

    def get_pipeline_health(self, pipeline_name: str) -> dict:
        """Health metrics for a single pipeline."""
        recent = self.store.get_recent_runs(pipeline_name=pipeline_name, limit=50)

        if recent.empty:
            return {"pipeline_name": pipeline_name, "status": "no_data"}

        last_run = recent.iloc[0]
        total = len(recent)
        successes = int((recent["status"] == "success").sum())
        success_rate = round(successes / total * 100, 1) if total > 0 else 0.0

        return {
            "pipeline_name": pipeline_name,
            "last_run_status": last_run.get("status"),
            "last_run_at": last_run.get("started_at"),
            "success_rate_pct": success_rate,
            "avg_latency_ms": round(recent["total_latency_ms"].mean(), 1),
            "avg_quality_score": round(recent["quality_score"].mean(), 2),
            "total_runs": total,
        }

    def is_pipeline_healthy(self, pipeline_name: str, min_success_rate: float = 80.0) -> bool:
        """Quick boolean health check for alerting."""
        health = self.get_pipeline_health(pipeline_name)
        rate = health.get("success_rate_pct", 0.0)
        return rate >= min_success_rate

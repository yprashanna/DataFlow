"""Pipeline run metadata store — all history in SQLite.

We use SQLite for metadata too — one less dependency, works great for
thousands of pipeline runs, and you can query it with any SQLite client.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

METADATA_DB_PATH = Path("data/metadata.db")

CREATE_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    status TEXT,                 -- success | failed | running
    rows_ingested INTEGER,
    rows_loaded INTEGER,
    quality_score REAL,
    total_latency_ms REAL,
    ingest_latency_ms REAL,
    transform_latency_ms REAL,
    load_latency_ms REAL,
    error TEXT,
    extra_json TEXT              -- any extra metadata as JSON blob
)
"""

CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_runs_pipeline_name
ON pipeline_runs (pipeline_name, started_at DESC)
"""


class MetadataStore:
    """Stores and retrieves pipeline run history from SQLite."""

    def __init__(self, db_path: str | Path = METADATA_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(CREATE_RUNS_TABLE)
            conn.execute(CREATE_INDEX)
            conn.commit()

    def record_run(self, run_meta: dict):
        """Upsert a pipeline run record."""
        # Pull out known columns; everything else goes into extra_json
        known_cols = {
            "run_id", "pipeline_name", "started_at", "finished_at",
            "status", "rows_ingested", "rows_loaded", "quality_score",
            "total_latency_ms", "ingest_latency_ms", "transform_latency_ms",
            "load_latency_ms", "error",
        }
        extra = {k: v for k, v in run_meta.items() if k not in known_cols}

        row = {k: run_meta.get(k) for k in known_cols}
        row["extra_json"] = json.dumps(extra) if extra else None

        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO pipeline_runs
                    (run_id, pipeline_name, started_at, finished_at, status,
                     rows_ingested, rows_loaded, quality_score,
                     total_latency_ms, ingest_latency_ms, transform_latency_ms,
                     load_latency_ms, error, extra_json)
                VALUES
                    (:run_id, :pipeline_name, :started_at, :finished_at, :status,
                     :rows_ingested, :rows_loaded, :quality_score,
                     :total_latency_ms, :ingest_latency_ms, :transform_latency_ms,
                     :load_latency_ms, :error, :extra_json)
                """,
                row,
            )
            conn.commit()
        logger.debug("Recorded run: %s — %s", run_meta.get("run_id"), run_meta.get("status"))

    def get_recent_runs(self, pipeline_name: Optional[str] = None, limit: int = 100) -> pd.DataFrame:
        """Get recent pipeline runs as a DataFrame."""
        query = "SELECT * FROM pipeline_runs"
        params: list = []
        if pipeline_name:
            query += " WHERE pipeline_name = ?"
            params.append(pipeline_name)
        query += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_pipeline_stats(self) -> pd.DataFrame:
        """Aggregated stats per pipeline — used by the dashboard."""
        query = """
        SELECT
            pipeline_name,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
            ROUND(AVG(total_latency_ms), 1) AS avg_latency_ms,
            ROUND(AVG(quality_score), 2) AS avg_quality_score,
            MAX(started_at) AS last_run_at,
            SUM(rows_loaded) AS total_rows_loaded
        FROM pipeline_runs
        GROUP BY pipeline_name
        ORDER BY last_run_at DESC
        """
        with self._connect() as conn:
            return pd.read_sql_query(query, conn)

    def get_latency_trend(self, pipeline_name: str, limit: int = 30) -> pd.DataFrame:
        """Time-series latency data for a specific pipeline — for charting."""
        query = """
        SELECT started_at, total_latency_ms, status, quality_score, rows_loaded
        FROM pipeline_runs
        WHERE pipeline_name = ?
        ORDER BY started_at DESC
        LIMIT ?
        """
        with self._connect() as conn:
            df = pd.read_sql_query(query, conn, params=[pipeline_name, limit])
        # Reverse so charts go chronologically left-to-right
        return df.iloc[::-1].reset_index(drop=True)

    def get_all_pipeline_names(self) -> list[str]:
        """Unique pipeline names in the metadata store."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT pipeline_name FROM pipeline_runs ORDER BY pipeline_name"
            ).fetchall()
        return [r["pipeline_name"] for r in rows]

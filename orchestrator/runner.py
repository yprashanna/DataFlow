"""Pipeline runner — executes a full pipeline from a YAML config dict.

Flow: Ingest → Validate (pre) → Clean → Transform → Aggregate → Validate (post) → Load
Each step is logged and timed. Results are written to the metadata store.
"""

import logging
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from pipelines.ingestion.csv_source import CSVSource
from pipelines.ingestion.api_source import APISource
from pipelines.ingestion.db_source import DBSource
from pipelines.validation.validator import DataValidator
from pipelines.transformation.cleaner import DataCleaner
from pipelines.transformation.transformer import DataTransformer
from pipelines.transformation.aggregator import DataAggregator
from pipelines.loading.sqlite_loader import SQLiteLoader
from monitoring.metadata import MetadataStore
from monitoring.alerts import AlertManager

logger = logging.getLogger(__name__)


class PipelineRunner:
    """Executes a DataFlow pipeline end-to-end given a parsed YAML config dict.

    Usage:
        config = load_pipeline_config("configs/sample_csv_pipeline.yml")
        runner = PipelineRunner(config)
        result = runner.run()
    """

    def __init__(self, pipeline_config: dict):
        self.config = pipeline_config
        self.pipeline_name = pipeline_config["name"]
        self.metadata_store = MetadataStore()
        self.alert_manager = AlertManager()

    def run(self) -> dict:
        """Execute the pipeline. Returns a run result dict."""
        run_id = f"{self.pipeline_name}_{int(time.time())}"
        started_at = datetime.now(timezone.utc)
        logger.info("=" * 60)
        logger.info("Starting pipeline: %s (run_id: %s)", self.pipeline_name, run_id)
        logger.info("=" * 60)

        run_meta = {
            "run_id": run_id,
            "pipeline_name": self.pipeline_name,
            "started_at": started_at.isoformat(),
            "status": "running",
            "rows_ingested": 0,
            "rows_loaded": 0,
            "quality_score": None,
            "error": None,
        }

        try:
            # ── Step 1: Ingest ────────────────────────────────────────
            t0 = time.perf_counter()
            raw_df = self._ingest()
            run_meta["rows_ingested"] = len(raw_df)
            run_meta["ingest_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            logger.info("Ingested %d rows in %.0fms", len(raw_df), run_meta["ingest_latency_ms"])

            # ── Step 2: Pre-load validation ───────────────────────────
            pre_validation_cfg = self.config.get("validation", {})
            if pre_validation_cfg:
                t0 = time.perf_counter()
                pre_report = DataValidator(pre_validation_cfg, self.pipeline_name).validate(raw_df)
                run_meta["quality_score"] = pre_report.quality_score
                run_meta["validation_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
                logger.info("Pre-validation score: %.1f%%", pre_report.quality_score)

                if not pre_report.passed:
                    logger.warning(
                        "Validation failures: %s",
                        [r.message for r in pre_report.failed_checks],
                    )
                    # Don't abort — continue with warnings unless config says to halt
                    if self.config.get("halt_on_validation_failure", False):
                        raise RuntimeError(
                            f"Pipeline halted: validation score {pre_report.quality_score:.1f}% below threshold"
                        )

            # ── Step 3: Clean ─────────────────────────────────────────
            transform_cfg = self.config.get("transformation", {})
            t0 = time.perf_counter()
            cleaner = DataCleaner(transform_cfg)
            clean_df = cleaner.clean(raw_df)
            run_meta["rows_after_cleaning"] = len(clean_df)
            run_meta["clean_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)

            # ── Step 4: Transform ─────────────────────────────────────
            t0 = time.perf_counter()
            transformer = DataTransformer(transform_cfg)
            transformed_df = transformer.transform(clean_df)
            run_meta["transform_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)

            # ── Step 5: Aggregate ─────────────────────────────────────
            t0 = time.perf_counter()
            aggregator = DataAggregator(transform_cfg)
            final_df = aggregator.aggregate(transformed_df)
            run_meta["rows_final"] = len(final_df)
            run_meta["aggregate_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)

            # ── Step 6: Load ──────────────────────────────────────────
            loading_cfg = self.config["loading"]
            t0 = time.perf_counter()
            loader = SQLiteLoader(loading_cfg)
            load_result = loader.load(final_df)
            run_meta["rows_loaded"] = load_result["rows_written"]
            run_meta["load_latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)

            # ── Done ──────────────────────────────────────────────────
            run_meta["status"] = "success"

        except Exception as exc:
            run_meta["status"] = "failed"
            run_meta["error"] = str(exc)
            run_meta["traceback"] = traceback.format_exc()
            logger.error("Pipeline %s FAILED: %s", self.pipeline_name, exc)
            self.alert_manager.send_failure_alert(self.pipeline_name, str(exc))

        finally:
            finished_at = datetime.now(timezone.utc)
            run_meta["finished_at"] = finished_at.isoformat()
            run_meta["total_latency_ms"] = round(
                (finished_at - started_at).total_seconds() * 1000, 1
            )
            # Always persist run metadata — even on failure
            self.metadata_store.record_run(run_meta)

        logger.info(
            "Pipeline %s finished: %s in %.0fms",
            self.pipeline_name,
            run_meta["status"],
            run_meta["total_latency_ms"],
        )
        return run_meta

    def _ingest(self) -> pd.DataFrame:
        """Route to the correct ingestion source based on config."""
        source_cfg = self.config["source"]
        source_type = source_cfg.get("type", "csv")

        if source_type == "csv":
            return CSVSource(source_cfg).ingest()
        elif source_type == "api":
            return APISource(source_cfg).ingest()
        elif source_type in ("sqlite", "db"):
            return DBSource(source_cfg).ingest()
        else:
            raise ValueError(f"Unknown source type: {source_type}")

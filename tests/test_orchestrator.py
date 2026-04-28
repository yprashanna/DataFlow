"""Tests for the pipeline orchestrator — config parser and runner."""

import pytest
import tempfile
from pathlib import Path
import yaml

from orchestrator.config_parser import load_pipeline_config, get_pipeline_schedule


VALID_CONFIG = {
    "name": "test_pipeline",
    "description": "A test pipeline",
    "source": {
        "type": "csv",
        "file_path": "data/sample_input.csv",
    },
    "loading": {
        "destination": "data/warehouse.db",
        "table": "test_output",
        "if_exists": "replace",
    },
}

SCHEDULED_CONFIG = {**VALID_CONFIG, "schedule": {"cron": "0 6 * * *"}}


def write_yaml(tmp_path, data, filename="pipeline.yml"):
    p = tmp_path / filename
    p.write_text(yaml.dump(data))
    return p


class TestConfigParser:
    def test_load_valid_config(self, tmp_path):
        p = write_yaml(tmp_path, VALID_CONFIG)
        config = load_pipeline_config(p)
        assert config["name"] == "test_pipeline"

    def test_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_pipeline_config("/nonexistent/config.yml")

    def test_raises_missing_keys(self, tmp_path):
        p = write_yaml(tmp_path, {"name": "no_source"})  # missing source + loading
        with pytest.raises(ValueError, match="missing required keys"):
            load_pipeline_config(p)

    def test_raises_empty_config(self, tmp_path):
        p = tmp_path / "empty.yml"
        p.write_text("")
        with pytest.raises(ValueError, match="Empty pipeline config"):
            load_pipeline_config(p)

    def test_get_schedule(self, tmp_path):
        p = write_yaml(tmp_path, SCHEDULED_CONFIG)
        config = load_pipeline_config(p)
        assert get_pipeline_schedule(config) == "0 6 * * *"

    def test_get_schedule_none(self, tmp_path):
        p = write_yaml(tmp_path, VALID_CONFIG)
        config = load_pipeline_config(p)
        assert get_pipeline_schedule(config) is None


class TestPipelineRunner:
    """Integration test — runs the full CSV pipeline against sample data."""

    def test_csv_pipeline_end_to_end(self, tmp_path):
        """Run a minimal CSV pipeline from a temp config."""
        import pandas as pd
        import sqlalchemy as sa

        # Write a small CSV
        csv_path = tmp_path / "data.csv"
        df = pd.DataFrame({
            "id": ["A1", "A2", "A3"],
            "region": ["North", "South", "East"],
            "amount": [100.0, 200.0, 300.0],
        })
        df.to_csv(csv_path, index=False)

        db_path = tmp_path / "warehouse.db"

        config = {
            "name": "e2e_test",
            "source": {"type": "csv", "file_path": str(csv_path)},
            "loading": {
                "destination": str(db_path),
                "table": "e2e_output",
                "if_exists": "replace",
            },
        }

        from orchestrator.runner import PipelineRunner
        runner = PipelineRunner(config)
        result = runner.run()

        assert result["status"] == "success"
        assert result["rows_ingested"] == 3
        assert result["rows_loaded"] == 3

        # Verify data actually landed in SQLite
        engine = sa.create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            loaded = pd.read_sql("SELECT * FROM e2e_output", conn)
        assert len(loaded) == 3
        assert "id" in loaded.columns

    def test_pipeline_handles_missing_source(self, tmp_path):
        """Runner should fail gracefully if source file doesn't exist."""
        config = {
            "name": "fail_test",
            "source": {"type": "csv", "file_path": "/nonexistent/file.csv"},
            "loading": {
                "destination": str(tmp_path / "warehouse.db"),
                "table": "fail_output",
                "if_exists": "replace",
            },
        }

        from orchestrator.runner import PipelineRunner
        runner = PipelineRunner(config)
        result = runner.run()

        assert result["status"] == "failed"
        assert result["error"] is not None

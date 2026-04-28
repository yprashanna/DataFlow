"""YAML pipeline config parser.

Reads pipeline definition files and returns structured dicts.
We validate the config minimally here — full validation happens in the runner.
"""

import logging
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

REQUIRED_TOP_LEVEL_KEYS = {"name", "source", "loading"}


def load_pipeline_config(config_path: str | Path) -> dict:
    """Load and parse a YAML pipeline config file.

    Returns a dict with pipeline_config ready to pass to PipelineRunner.
    Raises ValueError for obviously broken configs.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Pipeline config not found: {config_path}")

    with open(config_path, "r") as f:
        pipeline_config = yaml.safe_load(f)

    if not pipeline_config:
        raise ValueError(f"Empty pipeline config: {config_path}")

    # Check required keys
    missing = REQUIRED_TOP_LEVEL_KEYS - set(pipeline_config.keys())
    if missing:
        raise ValueError(f"Pipeline config missing required keys: {missing}")

    logger.info("Loaded pipeline config: %s", pipeline_config.get("name", "unknown"))
    return pipeline_config


def list_pipeline_configs(configs_dir: str | Path = "configs") -> list[Path]:
    """Discover all YAML pipeline configs in the configs directory."""
    configs_dir = Path(configs_dir)
    if not configs_dir.exists():
        return []
    return sorted(
        p for p in configs_dir.glob("*.yml")
        if not p.name.startswith("pipeline_schema")
    )


def get_pipeline_schedule(pipeline_config: dict) -> Optional[str]:
    """Extract the cron schedule string from a pipeline config, or None."""
    return pipeline_config.get("schedule", {}).get("cron", None)

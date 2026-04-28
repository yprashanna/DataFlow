"""Column-level transformation step — derived columns, normalization, etc.

Runs after cleaning, before aggregation.
"""

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Applies column transformations defined in YAML config.

    YAML example:
        transformation:
          transforms:
            - type: add_column
              name: revenue_after_discount
              expression: "total_amount * (1 - discount)"
            - type: normalize
              column: unit_price
              method: minmax    # or zscore
            - type: bin
              column: customer_age
              bins: [0, 25, 35, 50, 65, 100]
              labels: [Gen-Z, Millennial, Gen-X, Boomer, Senior]
              output_column: age_group
            - type: extract_date_parts
              column: sale_date
              parts: [year, month, day_of_week]
            - type: upper / lower
              column: region
    """

    def __init__(self, config: dict):
        self.transforms_cfg = config.get("transforms", [])

    def transform(self, clean_df: pd.DataFrame) -> pd.DataFrame:
        """Apply all configured transforms and return transformed DataFrame."""
        df = clean_df.copy()

        for t_cfg in self.transforms_cfg:
            t_type = t_cfg.get("type")
            try:
                if t_type == "add_column":
                    df = self._add_column(df, t_cfg)
                elif t_type == "normalize":
                    df = self._normalize(df, t_cfg)
                elif t_type == "bin":
                    df = self._bin(df, t_cfg)
                elif t_type == "extract_date_parts":
                    df = self._extract_date_parts(df, t_cfg)
                elif t_type == "upper":
                    df[t_cfg["column"]] = df[t_cfg["column"]].str.upper()
                elif t_type == "lower":
                    df[t_cfg["column"]] = df[t_cfg["column"]].str.lower()
                elif t_type == "map_values":
                    df = self._map_values(df, t_cfg)
                elif t_type == "drop_columns":
                    cols_to_drop = [c for c in t_cfg.get("columns", []) if c in df.columns]
                    df = df.drop(columns=cols_to_drop)
                else:
                    logger.warning("Unknown transform type: %s — skipping", t_type)
            except Exception as e:
                logger.error("Transform '%s' failed: %s", t_type, e)

        logger.info("Transformation complete: %d rows × %d cols", len(df), len(df.columns))
        return df

    def _add_column(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        """Evaluate an expression and store as a new column.

        Uses pandas eval — safe for arithmetic over column names.
        """
        col_name = cfg["name"]
        expr = cfg["expression"]
        try:
            df[col_name] = df.eval(expr)
        except Exception:
            # Fall back to python eval with df context — slightly less safe but more flexible
            df[col_name] = eval(expr, {"df": df, "np": np, "pd": pd})  # noqa: S307
        return df

    def _normalize(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        """Min-max or z-score normalization."""
        col = cfg["column"]
        method = cfg.get("method", "minmax")
        out_col = cfg.get("output_column", f"{col}_normalized")

        series = pd.to_numeric(df[col], errors="coerce")
        if method == "minmax":
            mn, mx = series.min(), series.max()
            df[out_col] = (series - mn) / (mx - mn) if mx > mn else 0.0
        elif method == "zscore":
            df[out_col] = (series - series.mean()) / series.std()
        else:
            logger.warning("Unknown normalization method: %s", method)
        return df

    def _bin(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        """Cut a numeric column into labeled bins."""
        col = cfg["column"]
        bins = cfg["bins"]
        labels = cfg.get("labels", None)
        out_col = cfg.get("output_column", f"{col}_bin")
        df[out_col] = pd.cut(df[col], bins=bins, labels=labels, right=False)
        return df

    def _extract_date_parts(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        """Extract year/month/quarter/day_of_week from a datetime column."""
        col = cfg["column"]
        parts = cfg.get("parts", ["year", "month"])

        if df[col].dtype == object:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        part_map = {
            "year": lambda s: s.dt.year,
            "month": lambda s: s.dt.month,
            "day": lambda s: s.dt.day,
            "quarter": lambda s: s.dt.quarter,
            "day_of_week": lambda s: s.dt.day_name(),
            "week": lambda s: s.dt.isocalendar().week,
        }
        for part in parts:
            if part in part_map:
                df[f"{col}_{part}"] = part_map[part](df[col])
        return df

    def _map_values(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        """Replace values using a mapping dict."""
        col = cfg["column"]
        mapping = cfg.get("mapping", {})
        df[col] = df[col].map(mapping).fillna(df[col])
        return df

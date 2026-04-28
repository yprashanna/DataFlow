"""Aggregation step — groupby + joins.

This is optional — pipelines that just want row-level output skip this step.
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataAggregator:
    """Applies groupby aggregations and optionally joins a secondary DataFrame.

    YAML example:
        transformation:
          aggregation:
            group_by: [region, category]
            aggs:
              total_amount: sum
              quantity: sum
              order_id: count
              unit_price: mean
            rename_aggs:
              order_id_count: total_orders
              unit_price_mean: avg_unit_price
    """

    def __init__(self, config: dict):
        agg_cfg = config.get("aggregation", {})
        self.group_by = agg_cfg.get("group_by", [])
        self.aggs = agg_cfg.get("aggs", {})
        self.rename_aggs = agg_cfg.get("rename_aggs", {})

    def aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run groupby aggregation if configured; otherwise return df unchanged."""
        if not self.group_by or not self.aggs:
            logger.debug("No aggregation configured — passing through")
            return df

        # Build the named aggregation dict for pandas — cleaner than the old tuple style
        named_aggs = {}
        for col, func in self.aggs.items():
            if col in df.columns:
                out_name = self.rename_aggs.get(f"{col}_{func}", f"{col}_{func}")
                named_aggs[out_name] = pd.NamedAgg(column=col, aggfunc=func)
            else:
                logger.warning("Aggregation column '%s' not found in DataFrame", col)

        valid_group_cols = [c for c in self.group_by if c in df.columns]
        agg_df = df.groupby(valid_group_cols, as_index=False).agg(**named_aggs)

        logger.info(
            "Aggregation complete: %d groups from %d rows (grouped by %s)",
            len(agg_df),
            len(df),
            valid_group_cols,
        )
        return agg_df

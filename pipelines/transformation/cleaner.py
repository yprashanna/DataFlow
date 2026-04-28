"""Data cleaning step — nulls, whitespace, type coercion, deduplication.

This is always the first transformation step. clean_df → transform → aggregate.
"""

import logging
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DataCleaner:
    """Applies cleaning rules from YAML config to a raw DataFrame.

    YAML example:
        transformation:
          cleaning:
            drop_nulls: [order_id, customer_id]      # drop rows where these are null
            fill_nulls:
              rating: 0
              email: "unknown@placeholder.com"
            strip_whitespace: [product_name, region]  # trim leading/trailing spaces
            drop_duplicates:
              subset: [order_id]
              keep: first
            cast_types:
              quantity: int
              unit_price: float
              sale_date: datetime
            rename_columns:
              "Product Name": product_name
    """

    def __init__(self, config: dict):
        cleaning_cfg = config.get("cleaning", {})
        self.drop_null_cols = cleaning_cfg.get("drop_nulls", [])
        self.fill_nulls = cleaning_cfg.get("fill_nulls", {})
        self.strip_cols = cleaning_cfg.get("strip_whitespace", [])
        self.dedup_config = cleaning_cfg.get("drop_duplicates", None)
        self.cast_types = cleaning_cfg.get("cast_types", {})
        self.rename_map = cleaning_cfg.get("rename_columns", {})
        self.filter_conditions = cleaning_cfg.get("filter_conditions", [])

    def clean(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Apply all cleaning steps and return clean_df."""
        clean_df = raw_df.copy()
        original_len = len(clean_df)

        clean_df = self._rename_columns(clean_df)
        clean_df = self._strip_whitespace(clean_df)
        clean_df = self._drop_nulls(clean_df)
        clean_df = self._fill_nulls(clean_df)
        clean_df = self._cast_types(clean_df)
        clean_df = self._deduplicate(clean_df)
        clean_df = self._apply_filters(clean_df)

        rows_removed = original_len - len(clean_df)
        logger.info(
            "Cleaning complete: %d → %d rows (%d removed)",
            original_len,
            len(clean_df),
            rows_removed,
        )
        return clean_df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.rename_map:
            return df
        return df.rename(columns=self.rename_map)

    def _strip_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.strip_cols:
            if col not in df.columns:
                continue
            # pandas 3.x uses StringDtype instead of object for string columns
            dtype = df[col].dtype
            if dtype == object or hasattr(dtype, 'name') and 'str' in str(dtype).lower():
                df[col] = df[col].str.strip()
            elif str(dtype) in ('string', 'StringDtype', 'object'):
                df[col] = df[col].str.strip()
        return df

    def _drop_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.drop_null_cols:
            return df
        # Only drop on columns that actually exist
        valid_cols = [c for c in self.drop_null_cols if c in df.columns]
        before = len(df)
        df = df.dropna(subset=valid_cols)
        logger.debug("Dropped %d rows with nulls in %s", before - len(df), valid_cols)
        return df

    def _fill_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        for col, fill_val in self.fill_nulls.items():
            if col in df.columns:
                df[col] = df[col].fillna(fill_val)
        return df

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        for col, target_type in self.cast_types.items():
            if col not in df.columns:
                continue
            try:
                if target_type == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif target_type == "int":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif target_type == "float":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif target_type == "str":
                    df[col] = df[col].astype(str)
                elif target_type == "bool":
                    df[col] = df[col].astype(bool)
                else:
                    df[col] = df[col].astype(target_type)
            except Exception as e:
                logger.warning("Could not cast column '%s' to %s: %s", col, target_type, e)
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.dedup_config:
            return df
        subset = self.dedup_config.get("subset", None)
        keep = self.dedup_config.get("keep", "first")
        before = len(df)
        df = df.drop_duplicates(subset=subset, keep=keep)
        logger.debug("Deduplication removed %d rows", before - len(df))
        return df

    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply row-level filter conditions from config.

        Each condition is a string like: "unit_price > 0" or "status != 'cancelled'"
        We eval these with pandas .query() which is reasonably safe for internal configs.
        """
        for condition in self.filter_conditions:
            try:
                before = len(df)
                df = df.query(condition)
                logger.debug(
                    "Filter '%s' removed %d rows", condition, before - len(df)
                )
            except Exception as e:
                logger.warning("Filter condition '%s' failed: %s", condition, e)
        return df

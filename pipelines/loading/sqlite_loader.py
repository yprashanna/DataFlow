"""SQLite data warehouse loader.

SQLite is honestly perfect for this scale — handles millions of rows,
zero server cost, trivially backed up with `cp warehouse.db backup.db`.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Default warehouse location — can be overridden per pipeline
DEFAULT_WAREHOUSE_PATH = Path("data/warehouse.db")


class SQLiteLoader:
    """Loads a DataFrame into a SQLite table with configurable write modes.

    YAML example:
        loading:
          destination: data/warehouse.db
          table: sales_daily_agg
          if_exists: replace    # append | replace | fail
          index: false
          dtype_overrides:      # optional SQLAlchemy type hints
            sale_date: DATE
    """

    def __init__(self, config: dict):
        db_path = Path(config.get("destination", str(DEFAULT_WAREHOUSE_PATH)))
        # Make sure the parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.table_name = config.get("table", "dataflow_output")
        self.if_exists = config.get("if_exists", "append")  # append keeps history
        self.write_index = config.get("index", False)
        self.chunksize = config.get("chunksize", 10_000)  # write in chunks for large DFs

        self.engine = sa.create_engine(
            f"sqlite:///{db_path}",
            connect_args={"check_same_thread": False},
        )
        logger.info("SQLite loader initialised — db: %s, table: %s", db_path, self.table_name)

    def load(self, df: pd.DataFrame) -> dict:
        """Write DataFrame to SQLite. Returns load metadata."""
        if df.empty:
            logger.warning("Empty DataFrame — nothing to load")
            return {"rows_written": 0, "table": self.table_name, "status": "skipped"}

        # Convert pandas NA types that SQLite doesn't handle well
        df = df.copy()
        for col in df.select_dtypes(include=["Int64", "boolean"]).columns:
            df[col] = df[col].astype(object).where(df[col].notna(), None)

        rows_before = self._get_row_count()

        df.to_sql(
            name=self.table_name,
            con=self.engine,
            if_exists=self.if_exists,
            index=self.write_index,
            chunksize=self.chunksize,
            method="multi",  # faster multi-row inserts
        )

        rows_after = self._get_row_count()
        rows_written = rows_after - rows_before if self.if_exists == "append" else len(df)

        logger.info(
            "Load complete: %d rows written to '%s' (%s mode)",
            rows_written,
            self.table_name,
            self.if_exists,
        )
        return {
            "rows_written": rows_written,
            "table": self.table_name,
            "if_exists": self.if_exists,
            "status": "success",
        }

    def _get_row_count(self) -> int:
        """Current row count for the target table — 0 if it doesn't exist yet."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.table_name}")
                )
                return result.scalar() or 0
        except Exception:
            return 0

    def get_table_info(self) -> dict:
        """Basic stats about the destination table."""
        return {
            "table": self.table_name,
            "row_count": self._get_row_count(),
        }

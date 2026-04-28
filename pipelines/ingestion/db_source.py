"""SQLite / SQL database ingestion source.

SQLite is honestly perfect for this scale — no server, no cost, fast enough
for millions of rows with a decent index.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlalchemy as sa

logger = logging.getLogger(__name__)


class DBSource:
    """Reads from a SQLite database using a SQL query or table name.

    Uses SQLAlchemy so it would be trivial to swap the connection string
    to Postgres or MySQL later — just change the URI in the YAML config.
    # TODO: add support for parameterized queries (for incremental loads)
    """

    def __init__(self, config: dict):
        self.connection_string = config["connection_string"]
        # Either provide a raw query OR a table name — not both
        self.query = config.get("query", None)
        self.table = config.get("table", None)
        self.chunk_size = config.get("chunk_size", None)  # rows per chunk for large tables

        if not self.query and not self.table:
            raise ValueError("DBSource config must have either 'query' or 'table'")

        self.engine = sa.create_engine(self.connection_string)

    def ingest(self) -> pd.DataFrame:
        """Execute query/table read and return DataFrame."""
        sql = self.query if self.query else f"SELECT * FROM {self.table}"
        logger.info("Ingesting from DB: %s (query: %s...)", self.connection_string, sql[:60])

        with self.engine.connect() as conn:
            if self.chunk_size:
                chunks = []
                for chunk in pd.read_sql(sql, conn, chunksize=self.chunk_size):
                    chunks.append(chunk)
                raw_df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
            else:
                raw_df = pd.read_sql(sql, conn)

        logger.info(
            "DB ingestion complete: %d rows × %d cols", len(raw_df), len(raw_df.columns)
        )
        return raw_df

    def get_source_info(self) -> dict:
        return {
            "type": "database",
            "connection": self.connection_string,
            "query_or_table": self.query or self.table,
        }

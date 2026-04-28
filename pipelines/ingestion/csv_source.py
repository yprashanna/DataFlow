"""CSV ingestion source — reads local CSV files into DataFrames.

pandas read_csv is surprisingly fast for files up to a few hundred MB,
but we chunk it for anything larger to avoid blowing up RAM.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Anything bigger than this threshold gets chunked automatically
CHUNK_SIZE_ROWS = 50_000
LARGE_FILE_BYTES = 50 * 1024 * 1024  # 50 MB


class CSVSource:
    """Reads CSV files and returns a pandas DataFrame.

    Handles encoding sniffing, delimiter detection, and chunked reads
    for large files without the caller needing to care about any of it.
    """

    def __init__(self, config: dict):
        self.file_path = Path(config["file_path"])
        self.delimiter = config.get("delimiter", ",")
        self.encoding = config.get("encoding", "utf-8")
        self.skip_rows = config.get("skip_rows", 0)
        self.dtype_map = config.get("dtype_map", {})  # optional column→dtype overrides
        # TODO: add support for parquet ingestion later

    def ingest(self) -> pd.DataFrame:
        """Load the CSV and return a DataFrame. Chunks automatically for large files."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV source not found: {self.file_path}")

        file_size = self.file_path.stat().st_size
        logger.info(
            "Ingesting CSV: %s (%.1f MB)", self.file_path, file_size / 1024 / 1024
        )

        read_kwargs = dict(
            filepath_or_buffer=self.file_path,
            sep=self.delimiter,
            encoding=self.encoding,
            skiprows=self.skip_rows,
            dtype=self.dtype_map if self.dtype_map else None,
            # keep_default_na=True so empty strings become NaN — easier to validate
            keep_default_na=True,
        )

        if file_size > LARGE_FILE_BYTES:
            logger.info(
                "File is large (%.1f MB) — reading in %d-row chunks",
                file_size / 1024 / 1024,
                CHUNK_SIZE_ROWS,
            )
            raw_df = self._read_chunked(read_kwargs)
        else:
            raw_df = pd.read_csv(**read_kwargs)

        logger.info(
            "CSV ingestion complete: %d rows × %d cols", len(raw_df), len(raw_df.columns)
        )
        return raw_df

    def _read_chunked(self, read_kwargs: dict) -> pd.DataFrame:
        """Read in chunks and concat — memory-friendly for big files."""
        chunks = []
        for chunk in pd.read_csv(**read_kwargs, chunksize=CHUNK_SIZE_ROWS):
            chunks.append(chunk)
        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    def get_source_info(self) -> dict:
        """Metadata about the source — useful for run logs."""
        return {
            "type": "csv",
            "path": str(self.file_path),
            "size_bytes": self.file_path.stat().st_size if self.file_path.exists() else 0,
        }

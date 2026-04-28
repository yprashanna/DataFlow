"""Tests for ingestion sources."""

import os
import tempfile
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

from pipelines.ingestion.csv_source import CSVSource
from pipelines.ingestion.api_source import APISource
from pipelines.ingestion.db_source import DBSource


# ── CSV Source ─────────────────────────────────────────────────────────────


def test_csv_ingest_basic(tmp_path):
    """CSVSource should read a simple CSV and return a DataFrame."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n3,Carol,300\n")

    source = CSVSource({"file_path": str(csv_file)})
    df = source.ingest()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "value"]


def test_csv_ingest_file_not_found():
    """CSVSource should raise FileNotFoundError for missing files."""
    source = CSVSource({"file_path": "/nonexistent/path/file.csv"})
    with pytest.raises(FileNotFoundError):
        source.ingest()


def test_csv_ingest_with_delimiter(tmp_path):
    """CSVSource should handle custom delimiters."""
    tsv_file = tmp_path / "test.tsv"
    tsv_file.write_text("id\tname\tvalue\n1\tAlice\t100\n")

    source = CSVSource({"file_path": str(tsv_file), "delimiter": "\t"})
    df = source.ingest()
    assert len(df) == 1
    assert "name" in df.columns


def test_csv_source_info(tmp_path):
    """CSVSource.get_source_info() should return type and path."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("a,b\n1,2\n")
    source = CSVSource({"file_path": str(csv_file)})
    info = source.get_source_info()
    assert info["type"] == "csv"
    assert "path" in info


# ── API Source ─────────────────────────────────────────────────────────────


def test_api_source_list_response():
    """APISource should handle API returning a top-level list."""
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
    ]
    mock_response.raise_for_status = MagicMock()

    with patch("requests.Session.request", return_value=mock_response):
        source = APISource({"url": "https://jsonplaceholder.typicode.com/posts"})
        df = source.ingest()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "id" in df.columns


def test_api_source_nested_json_path():
    """APISource should extract records from a json_path."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": {"items": [{"id": 1}, {"id": 2}]}
    }
    mock_response.raise_for_status = MagicMock()

    with patch("requests.Session.request", return_value=mock_response):
        source = APISource({
            "url": "https://example.com/api",
            "json_path": "data.items",
        })
        df = source.ingest()

    assert len(df) == 2


def test_api_source_empty_response():
    """APISource should return empty DataFrame for empty API response."""
    mock_response = MagicMock()
    mock_response.json.return_value = []
    mock_response.raise_for_status = MagicMock()

    with patch("requests.Session.request", return_value=mock_response):
        source = APISource({"url": "https://example.com/api"})
        df = source.ingest()

    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ── DB Source ─────────────────────────────────────────────────────────────


def test_db_source_sqlite(tmp_path):
    """DBSource should read from SQLite using a table name."""
    import sqlalchemy as sa

    db_path = tmp_path / "test.db"
    engine = sa.create_engine(f"sqlite:///{db_path}")
    test_df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    test_df.to_sql("test_table", engine, if_exists="replace", index=False)

    source = DBSource({
        "connection_string": f"sqlite:///{db_path}",
        "table": "test_table",
    })
    result = source.ingest()

    assert len(result) == 3
    assert "id" in result.columns


def test_db_source_requires_query_or_table():
    """DBSource should raise ValueError if neither query nor table provided."""
    with pytest.raises(ValueError, match="either 'query' or 'table'"):
        DBSource({"connection_string": "sqlite:///test.db"})

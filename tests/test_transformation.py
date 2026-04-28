"""Tests for cleaning, transformation, and aggregation."""

import pytest
import pandas as pd
import numpy as np
from pipelines.transformation.cleaner import DataCleaner
from pipelines.transformation.transformer import DataTransformer
from pipelines.transformation.aggregator import DataAggregator


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": ["A1", "A2", "A3", "A3", "A4"],  # A3 is duplicate
        "name": ["  Alice  ", "Bob", "Carol", "Carol", "Dave"],
        "amount": [100.0, -5.0, 200.0, 200.0, 300.0],
        "category": ["X", "Y", "X", "X", "Z"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-03", None],
        "score": [5.0, None, 3.0, 3.0, 4.0],
    })


# ── Cleaner ─────────────────────────────────────────────────────────────────

class TestDataCleaner:
    def test_strip_whitespace(self, sample_df):
        cleaner = DataCleaner({"cleaning": {"strip_whitespace": ["name"]}})
        result = cleaner.clean(sample_df)
        assert result["name"].iloc[0] == "Alice"

    def test_drop_nulls(self, sample_df):
        cleaner = DataCleaner({"cleaning": {"drop_nulls": ["date"]}})
        result = cleaner.clean(sample_df)
        assert result["date"].isna().sum() == 0
        assert len(result) == 4

    def test_fill_nulls(self, sample_df):
        cleaner = DataCleaner({"cleaning": {"fill_nulls": {"score": 0.0}}})
        result = cleaner.clean(sample_df)
        assert result["score"].isna().sum() == 0
        assert result["score"].iloc[1] == 0.0

    def test_deduplication(self, sample_df):
        cleaner = DataCleaner({
            "cleaning": {"drop_duplicates": {"subset": ["id"], "keep": "first"}}
        })
        result = cleaner.clean(sample_df)
        assert result["id"].duplicated().sum() == 0
        assert len(result) == 4

    def test_cast_types(self, sample_df):
        cleaner = DataCleaner({
            "cleaning": {"cast_types": {"date": "datetime", "amount": "float"}}
        })
        result = cleaner.clean(sample_df)
        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_filter_conditions(self, sample_df):
        cleaner = DataCleaner({
            "cleaning": {"filter_conditions": ["amount > 0"]}
        })
        result = cleaner.clean(sample_df)
        assert (result["amount"] > 0).all()
        assert len(result) == 4  # -5.0 row removed

    def test_no_config_returns_copy(self, sample_df):
        cleaner = DataCleaner({})
        result = cleaner.clean(sample_df)
        assert len(result) == len(sample_df)


# ── Transformer ─────────────────────────────────────────────────────────────

class TestDataTransformer:
    def test_add_column(self, sample_df):
        transformer = DataTransformer({
            "transforms": [{"type": "add_column", "name": "double_amount", "expression": "amount * 2"}]
        })
        result = transformer.transform(sample_df)
        assert "double_amount" in result.columns
        assert result["double_amount"].iloc[0] == pytest.approx(200.0)

    def test_upper(self, sample_df):
        transformer = DataTransformer({
            "transforms": [{"type": "upper", "column": "category"}]
        })
        result = transformer.transform(sample_df)
        assert result["category"].iloc[0] == "X"  # already upper

    def test_lower(self, sample_df):
        df = pd.DataFrame({"col": ["Hello", "WORLD"]})
        transformer = DataTransformer({
            "transforms": [{"type": "lower", "column": "col"}]
        })
        result = transformer.transform(df)
        assert result["col"].tolist() == ["hello", "world"]

    def test_normalize_minmax(self):
        df = pd.DataFrame({"x": [0.0, 50.0, 100.0]})
        transformer = DataTransformer({
            "transforms": [{"type": "normalize", "column": "x", "method": "minmax"}]
        })
        result = transformer.transform(df)
        assert "x_normalized" in result.columns
        assert result["x_normalized"].min() == pytest.approx(0.0)
        assert result["x_normalized"].max() == pytest.approx(1.0)

    def test_extract_date_parts(self):
        df = pd.DataFrame({"date": pd.to_datetime(["2024-03-15", "2024-06-20"])})
        transformer = DataTransformer({
            "transforms": [{
                "type": "extract_date_parts",
                "column": "date",
                "parts": ["year", "month"]
            }]
        })
        result = transformer.transform(df)
        assert "date_year" in result.columns
        assert "date_month" in result.columns
        assert result["date_year"].iloc[0] == 2024
        assert result["date_month"].iloc[0] == 3

    def test_drop_columns(self, sample_df):
        transformer = DataTransformer({
            "transforms": [{"type": "drop_columns", "columns": ["score"]}]
        })
        result = transformer.transform(sample_df)
        assert "score" not in result.columns


# ── Aggregator ─────────────────────────────────────────────────────────────

class TestDataAggregator:
    def test_basic_groupby(self, sample_df):
        aggregator = DataAggregator({
            "aggregation": {
                "group_by": ["category"],
                "aggs": {"amount": "sum"},
                "rename_aggs": {"amount_sum": "total_amount"},
            }
        })
        result = aggregator.aggregate(sample_df)
        assert "category" in result.columns
        assert "total_amount" in result.columns
        assert len(result) == 3  # X, Y, Z

    def test_no_aggregation_passthrough(self, sample_df):
        aggregator = DataAggregator({})
        result = aggregator.aggregate(sample_df)
        assert len(result) == len(sample_df)

    def test_count_aggregation(self, sample_df):
        aggregator = DataAggregator({
            "aggregation": {
                "group_by": ["category"],
                "aggs": {"id": "count"},
                "rename_aggs": {"id_count": "num_records"},
            }
        })
        result = aggregator.aggregate(sample_df)
        x_row = result[result["category"] == "X"].iloc[0]
        assert x_row["num_records"] == 3  # A1, A3, A3

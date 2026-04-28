"""Tests for data validation engine."""

import pytest
import pandas as pd
from pipelines.validation.validator import DataValidator, ValidationReport


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "order_id": ["A1", "A2", "A3", "A4", "A5"],
        "customer_id": ["C1", "C2", "C3", None, "C5"],
        "amount": [100.0, 200.0, -5.0, 300.0, 99999.0],
        "status": ["completed", "pending", "unknown_status", "cancelled", "refunded"],
        "email": ["a@b.com", "bad-email", "c@d.com", "e@f.com", "g@h.com"],
    })


def make_validator(checks, name="test_pipeline"):
    return DataValidator({"checks": checks}, pipeline_name=name)


class TestNotNullCheck:
    def test_passes_no_nulls(self, sample_df):
        v = make_validator([{"type": "not_null", "columns": ["order_id"]}])
        report = v.validate(sample_df)
        result = next(r for r in report.results if r.rule_type == "not_null")
        assert result.passed

    def test_fails_with_nulls(self, sample_df):
        v = make_validator([{"type": "not_null", "columns": ["customer_id"], "threshold": 1.0}])
        report = v.validate(sample_df)
        result = next(r for r in report.results if "customer_id" in r.check_name)
        assert not result.passed
        assert result.failed_count == 1

    def test_passes_with_threshold(self, sample_df):
        # 1 null out of 5 = 80% non-null; threshold 0.75 should pass
        v = make_validator([{"type": "not_null", "columns": ["customer_id"], "threshold": 0.75}])
        report = v.validate(sample_df)
        result = next(r for r in report.results if "customer_id" in r.check_name)
        assert result.passed

    def test_missing_column(self, sample_df):
        v = make_validator([{"type": "not_null", "columns": ["nonexistent_col"]}])
        report = v.validate(sample_df)
        result = report.results[0]
        assert not result.passed


class TestRangeCheck:
    def test_passes_valid_range(self):
        df = pd.DataFrame({"price": [1.0, 50.0, 100.0]})
        v = make_validator([{"type": "range", "column": "price", "min": 0, "max": 200}])
        report = v.validate(df)
        assert report.results[0].passed

    def test_fails_below_min(self, sample_df):
        v = make_validator([{"type": "range", "column": "amount", "min": 0, "max": 10000}])
        report = v.validate(sample_df)
        result = report.results[0]
        assert not result.passed
        assert result.failed_count == 1  # -5.0 is out of range

    def test_no_max(self):
        df = pd.DataFrame({"x": [1, 2, 3, 1000000]})
        v = make_validator([{"type": "range", "column": "x", "min": 0}])
        report = v.validate(df)
        assert report.results[0].passed


class TestUniqueCheck:
    def test_passes_unique(self):
        df = pd.DataFrame({"id": [1, 2, 3, 4]})
        v = make_validator([{"type": "unique", "column": "id"}])
        report = v.validate(df)
        assert report.results[0].passed

    def test_fails_duplicates(self):
        df = pd.DataFrame({"id": [1, 2, 2, 3]})
        v = make_validator([{"type": "unique", "column": "id"}])
        report = v.validate(df)
        result = report.results[0]
        assert not result.passed
        assert result.failed_count == 1


class TestSchemaCheck:
    def test_passes_all_columns_present(self, sample_df):
        v = make_validator([{
            "type": "schema",
            "expected_columns": ["order_id", "status"]
        }])
        report = v.validate(sample_df)
        assert report.results[0].passed

    def test_fails_missing_column(self, sample_df):
        v = make_validator([{
            "type": "schema",
            "expected_columns": ["order_id", "missing_col"]
        }])
        report = v.validate(sample_df)
        assert not report.results[0].passed


class TestAcceptedValuesCheck:
    def test_passes_all_valid(self):
        df = pd.DataFrame({"status": ["active", "inactive", "active"]})
        v = make_validator([{
            "type": "accepted_values",
            "column": "status",
            "values": ["active", "inactive", "pending"]
        }])
        report = v.validate(df)
        assert report.results[0].passed

    def test_fails_invalid_value(self, sample_df):
        v = make_validator([{
            "type": "accepted_values",
            "column": "status",
            "values": ["completed", "pending", "cancelled", "refunded"]
        }])
        report = v.validate(sample_df)
        result = report.results[0]
        assert not result.passed  # "unknown_status" should fail


class TestRowCountCheck:
    def test_passes_enough_rows(self, sample_df):
        v = make_validator([{"type": "row_count", "min_rows": 3}])
        report = v.validate(sample_df)
        assert report.results[0].passed

    def test_fails_too_few_rows(self):
        df = pd.DataFrame({"a": [1, 2]})
        v = make_validator([{"type": "row_count", "min_rows": 100}])
        report = v.validate(df)
        assert not report.results[0].passed


class TestQualityScore:
    def test_all_pass_score_100(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        v = make_validator([{"type": "row_count", "min_rows": 1}])
        report = v.validate(df)
        assert report.quality_score == 100.0

    def test_empty_checks_score_100(self, sample_df):
        v = make_validator([])
        report = v.validate(sample_df)
        assert report.quality_score == 100.0

    def test_partial_failures_reduce_score(self, sample_df):
        v = make_validator([
            {"type": "not_null", "columns": ["order_id"], "threshold": 1.0},  # pass
            {"type": "unique", "column": "order_id"},  # pass
            {"type": "row_count", "min_rows": 1000},   # fail
        ])
        report = v.validate(sample_df)
        assert 0 < report.quality_score < 100

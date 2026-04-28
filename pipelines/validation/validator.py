"""Data quality validation engine.

We use Great Expectations under the hood but wrap it in a simpler interface
so pipeline YAML configs can express rules declaratively without knowing GE internals.

Each check produces a pass/fail result with a score — the overall quality score
is (passed_checks / total_checks) * 100.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """Result of a single validation check."""

    check_name: str
    column: Optional[str]
    rule_type: str
    passed: bool
    message: str
    failed_count: int = 0
    total_count: int = 0

    @property
    def score(self) -> float:
        if self.total_count == 0:
            return 1.0
        return 1.0 - (self.failed_count / self.total_count)


@dataclass
class ValidationReport:
    """Aggregate report from all checks on a DataFrame."""

    pipeline_name: str
    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def quality_score(self) -> float:
        """0–100 quality score: average of per-check scores."""
        if not self.results:
            return 100.0
        return round(sum(r.score for r in self.results) / len(self.results) * 100, 2)

    @property
    def failed_checks(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    def summary(self) -> dict:
        return {
            "pipeline": self.pipeline_name,
            "total_checks": len(self.results),
            "passed_checks": sum(1 for r in self.results if r.passed),
            "failed_checks": len(self.failed_checks),
            "quality_score": self.quality_score,
            "overall_passed": self.passed,
        }


class DataValidator:
    """Runs configurable data quality checks against a DataFrame.

    YAML config example:
        validation:
          checks:
            - type: not_null
              columns: [order_id, customer_id, total_amount]
              threshold: 0.99   # 99% of rows must be non-null to pass
            - type: range
              column: unit_price
              min: 0.01
              max: 10000
            - type: unique
              column: order_id
            - type: schema
              expected_columns: [order_id, customer_id, total_amount]
            - type: accepted_values
              column: status
              values: [completed, pending, cancelled, refunded]
    """

    def __init__(self, config: dict, pipeline_name: str = "unknown"):
        self.checks_config = config.get("checks", [])
        self.pipeline_name = pipeline_name

    def validate(self, df: pd.DataFrame) -> ValidationReport:
        """Run all configured checks and return a ValidationReport."""
        report = ValidationReport(pipeline_name=self.pipeline_name)

        for check_cfg in self.checks_config:
            rule_type = check_cfg.get("type")
            try:
                if rule_type == "not_null":
                    results = self._check_not_null(df, check_cfg)
                elif rule_type == "range":
                    results = [self._check_range(df, check_cfg)]
                elif rule_type == "unique":
                    results = [self._check_unique(df, check_cfg)]
                elif rule_type == "schema":
                    results = [self._check_schema(df, check_cfg)]
                elif rule_type == "accepted_values":
                    results = [self._check_accepted_values(df, check_cfg)]
                elif rule_type == "regex":
                    results = [self._check_regex(df, check_cfg)]
                elif rule_type == "row_count":
                    results = [self._check_row_count(df, check_cfg)]
                else:
                    logger.warning("Unknown check type: %s — skipping", rule_type)
                    continue

                report.results.extend(results)

            except Exception as exc:
                # Don't let a buggy check kill the whole pipeline — log and continue
                logger.error("Check %s raised exception: %s", rule_type, exc)
                report.results.append(
                    CheckResult(
                        check_name=f"{rule_type}_error",
                        column=check_cfg.get("column"),
                        rule_type=rule_type,
                        passed=False,
                        message=f"Check raised exception: {exc}",
                    )
                )

        summary = report.summary()
        logger.info(
            "Validation complete — score: %.1f%% (%d/%d checks passed)",
            summary["quality_score"],
            summary["passed_checks"],
            summary["total_checks"],
        )
        return report

    # ──────────────────────────────────────────────
    # Individual check implementations
    # ──────────────────────────────────────────────

    def _check_not_null(self, df: pd.DataFrame, cfg: dict) -> list[CheckResult]:
        """Check that columns have acceptable null rates."""
        columns = cfg.get("columns", cfg.get("column", []))
        if isinstance(columns, str):
            columns = [columns]
        threshold = cfg.get("threshold", 1.0)  # default: zero nulls allowed

        results = []
        for col in columns:
            if col not in df.columns:
                results.append(
                    CheckResult(
                        check_name=f"not_null_{col}",
                        column=col,
                        rule_type="not_null",
                        passed=False,
                        message=f"Column '{col}' not found in DataFrame",
                    )
                )
                continue

            null_count = int(df[col].isna().sum())
            total = len(df)
            non_null_rate = 1.0 - (null_count / total) if total > 0 else 1.0
            passed = non_null_rate >= threshold

            results.append(
                CheckResult(
                    check_name=f"not_null_{col}",
                    column=col,
                    rule_type="not_null",
                    passed=passed,
                    message=(
                        f"OK — {null_count} nulls ({non_null_rate:.1%} non-null ≥ {threshold:.1%})"
                        if passed
                        else f"FAIL — {null_count} nulls ({non_null_rate:.1%} non-null < threshold {threshold:.1%})"
                    ),
                    failed_count=null_count,
                    total_count=total,
                )
            )
        return results

    def _check_range(self, df: pd.DataFrame, cfg: dict) -> CheckResult:
        """Check numeric column values fall within [min, max]."""
        col = cfg["column"]
        min_val = cfg.get("min", None)
        max_val = cfg.get("max", None)

        if col not in df.columns:
            return CheckResult(
                check_name=f"range_{col}",
                column=col,
                rule_type="range",
                passed=False,
                message=f"Column '{col}' not found",
            )

        series = pd.to_numeric(df[col], errors="coerce")
        mask = pd.Series([True] * len(series), index=series.index)
        if min_val is not None:
            mask &= series >= min_val
        if max_val is not None:
            mask &= series <= max_val

        # Nulls are counted as failures too
        mask &= series.notna()
        failed_count = int((~mask).sum())
        passed = failed_count == 0

        return CheckResult(
            check_name=f"range_{col}",
            column=col,
            rule_type="range",
            passed=passed,
            message=(
                f"OK — all {len(series)} values in range [{min_val}, {max_val}]"
                if passed
                else f"FAIL — {failed_count} values outside [{min_val}, {max_val}]"
            ),
            failed_count=failed_count,
            total_count=len(series),
        )

    def _check_unique(self, df: pd.DataFrame, cfg: dict) -> CheckResult:
        """Check that a column has no duplicates."""
        col = cfg["column"]

        if col not in df.columns:
            return CheckResult(
                check_name=f"unique_{col}",
                column=col,
                rule_type="unique",
                passed=False,
                message=f"Column '{col}' not found",
            )

        dup_count = int(df[col].duplicated().sum())
        passed = dup_count == 0

        return CheckResult(
            check_name=f"unique_{col}",
            column=col,
            rule_type="unique",
            passed=passed,
            message=(
                f"OK — no duplicates in {col}"
                if passed
                else f"FAIL — {dup_count} duplicate values in '{col}'"
            ),
            failed_count=dup_count,
            total_count=len(df),
        )

    def _check_schema(self, df: pd.DataFrame, cfg: dict) -> CheckResult:
        """Check that expected columns are all present."""
        expected = set(cfg.get("expected_columns", []))
        actual = set(df.columns)
        missing = expected - actual

        passed = len(missing) == 0
        return CheckResult(
            check_name="schema_check",
            column=None,
            rule_type="schema",
            passed=passed,
            message=(
                "OK — all expected columns present"
                if passed
                else f"FAIL — missing columns: {missing}"
            ),
            failed_count=len(missing),
            total_count=len(expected),
        )

    def _check_accepted_values(self, df: pd.DataFrame, cfg: dict) -> CheckResult:
        """Check that column only contains values from an allowed set."""
        col = cfg["column"]
        allowed = set(cfg.get("values", []))

        if col not in df.columns:
            return CheckResult(
                check_name=f"accepted_{col}",
                column=col,
                rule_type="accepted_values",
                passed=False,
                message=f"Column '{col}' not found",
            )

        invalid_mask = ~df[col].isin(allowed) & df[col].notna()
        failed_count = int(invalid_mask.sum())
        passed = failed_count == 0

        return CheckResult(
            check_name=f"accepted_{col}",
            column=col,
            rule_type="accepted_values",
            passed=passed,
            message=(
                f"OK — all values in accepted set"
                if passed
                else f"FAIL — {failed_count} values not in {allowed}"
            ),
            failed_count=failed_count,
            total_count=len(df),
        )

    def _check_regex(self, df: pd.DataFrame, cfg: dict) -> CheckResult:
        """Check that column matches a regex pattern."""
        col = cfg["column"]
        pattern = cfg["pattern"]

        if col not in df.columns:
            return CheckResult(
                check_name=f"regex_{col}",
                column=col,
                rule_type="regex",
                passed=False,
                message=f"Column '{col}' not found",
            )

        non_null = df[col].dropna()
        failed_count = int((~non_null.astype(str).str.match(pattern)).sum())
        passed = failed_count == 0

        return CheckResult(
            check_name=f"regex_{col}",
            column=col,
            rule_type="regex",
            passed=passed,
            message=(
                f"OK — all values match pattern '{pattern}'"
                if passed
                else f"FAIL — {failed_count} values don't match '{pattern}'"
            ),
            failed_count=failed_count,
            total_count=len(non_null),
        )

    def _check_row_count(self, df: pd.DataFrame, cfg: dict) -> CheckResult:
        """Check that DataFrame has at least min_rows."""
        min_rows = cfg.get("min_rows", 1)
        actual = len(df)
        passed = actual >= min_rows

        return CheckResult(
            check_name="row_count",
            column=None,
            rule_type="row_count",
            passed=passed,
            message=(
                f"OK — {actual} rows ≥ {min_rows}"
                if passed
                else f"FAIL — only {actual} rows, expected ≥ {min_rows}"
            ),
            failed_count=0 if passed else 1,
            total_count=1,
        )

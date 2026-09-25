"""Configuration-driven data quality engine.

Expectations are declared in the pipeline YAML, not written as ad-hoc asserts in
notebooks. Every run appends its results to an ops table so CAS can evidence
that controls over the data pipeline itself were operating -- auditors get
audited too.

Severity:
    error  -> fail the run; Data Factory surfaces the failure and stops downstream
    warn   -> record and continue
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, asdict
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dataclass
class CheckResult:
    run_id: str
    pipeline: str
    check_type: str
    columns: str
    severity: str
    passed: bool
    observed: int
    detail: str
    checked_at: dt.datetime


RESULT_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("pipeline", T.StringType()),
    T.StructField("check_type", T.StringType()),
    T.StructField("columns", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("passed", T.BooleanType()),
    T.StructField("observed", T.LongType()),
    T.StructField("detail", T.StringType()),
    T.StructField("checked_at", T.TimestampType()),
])


class DataQualityError(RuntimeError):
    """Raised when one or more error-severity expectations fail."""


# --------------------------------------------------------------------------
# Individual checks. Each returns (passed, observed_count, detail).
# --------------------------------------------------------------------------

def _check_not_null(df: DataFrame, spec: dict[str, Any]) -> tuple[bool, int, str]:
    cols = spec["columns"]
    cond = F.lit(False)
    for c in cols:
        cond = cond | F.col(c).isNull()
    n = df.filter(cond).count()
    return n == 0, n, f"{n} rows with NULL in {cols}"


def _check_unique(df: DataFrame, spec: dict[str, Any]) -> tuple[bool, int, str]:
    """Duplicate detection on a business key.

    Counts surplus rows (total - distinct) rather than duplicate groups, so the
    number reads directly as "rows that should not be here".
    """
    cols = spec["columns"]
    total = df.count()
    distinct = df.select(*cols).distinct().count()
    surplus = total - distinct
    return surplus == 0, surplus, f"{surplus} surplus rows on key {cols} ({total} total, {distinct} distinct)"


def _check_row_count_min(df: DataFrame, spec: dict[str, Any]) -> tuple[bool, int, str]:
    n = df.count()
    minimum = int(spec["value"])
    return n >= minimum, n, f"row count {n} vs minimum {minimum}"


def _check_accepted_values(df: DataFrame, spec: dict[str, Any]) -> tuple[bool, int, str]:
    col, allowed = spec["columns"][0], spec["values"]
    n = df.filter(~F.col(col).isin(allowed) & F.col(col).isNotNull()).count()
    return n == 0, n, f"{n} rows in {col} outside accepted values"


def _check_non_negative(df: DataFrame, spec: dict[str, Any]) -> tuple[bool, int, str]:
    col = spec["columns"][0]
    n = df.filter(F.col(col) < 0).count()
    return n == 0, n, f"{n} negative values in {col}"


CHECKS: dict[str, Callable[[DataFrame, dict[str, Any]], tuple[bool, int, str]]] = {
    "not_null": _check_not_null,
    "unique": _check_unique,
    "row_count_min": _check_row_count_min,
    "accepted_values": _check_accepted_values,
    "non_negative": _check_non_negative,
}


def run_expectations(df: DataFrame, expectations: list[dict[str, Any]], *,
                     run_id: str, pipeline: str) -> list[CheckResult]:
    """Evaluate every declared expectation and return structured results."""
    results: list[CheckResult] = []
    now = dt.datetime.now()

    for spec in expectations:
        kind = spec["type"]
        if kind not in CHECKS:
            raise ValueError(f"Unknown expectation type '{kind}'. Known: {sorted(CHECKS)}")
        severity = spec.get("severity", "error")
        passed, observed, detail = CHECKS[kind](df, spec)
        results.append(CheckResult(
            run_id=run_id,
            pipeline=pipeline,
            check_type=kind,
            columns=",".join(spec.get("columns", [])) or "-",
            severity=severity,
            passed=passed,
            observed=int(observed),
            detail=detail,
            checked_at=now,
        ))
    return results


def persist_results(spark: SparkSession, results: list[CheckResult], path: str) -> None:
    """Append results to the ops table that evidences pipeline control operation."""
    if not results:
        return
    rows = [tuple(asdict(r).values()) for r in results]
    df = spark.createDataFrame(rows, schema=RESULT_SCHEMA)
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def enforce(results: list[CheckResult]) -> None:
    """Raise if any error-severity expectation failed."""
    failures = [r for r in results if not r.passed and r.severity == "error"]
    if failures:
        lines = "\n".join(f"  - [{r.check_type}] {r.detail}" for r in failures)
        raise DataQualityError(f"{len(failures)} data quality check(s) failed:\n{lines}")


def summarise(results: list[CheckResult]) -> str:
    passed = sum(1 for r in results if r.passed)
    return f"{passed}/{len(results)} checks passed"

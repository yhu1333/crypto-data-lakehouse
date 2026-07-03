import json
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ONE_MINUTE_MS = 60_000

COMPLETENESS_FAIL_THRESHOLD = 0.90
COMPLETENESS_WARN_THRESHOLD = 0.98

# Source OHLCV fields only. Derived analytics columns (returns, moving averages, vwap,
# rolling volatility, gap flags) are legitimately null at the first row(s) of each
# symbol's window -- that's a window-function boundary condition, not a data defect.
KEY_FIELDS = ["symbol", "open_time", "open", "high", "low", "close", "volume"]


def validity_check(df: DataFrame) -> dict:
    key_fields = [c for c in KEY_FIELDS if c in df.columns]
    null_counts = (
        df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in key_fields]).collect()[0].asDict()
    )

    invalid_conditions = [F.col("close") <= 0, F.col("high") < F.col("low")]
    if "open" in df.columns:
        invalid_conditions.append(F.col("open") <= 0)
    if "low" in df.columns:
        invalid_conditions.append(F.col("low") <= 0)

    invalid_price_count = df.filter(reduce(lambda left, right: left | right, invalid_conditions)).count()
    duplicate_count = df.groupBy("symbol", "open_time").count().filter(F.col("count") > 1).count()
    total_nulls = sum(null_counts.values())

    return {
        "null_counts": {k: v for k, v in null_counts.items() if v > 0},
        "total_nulls": total_nulls,
        "invalid_price_count": invalid_price_count,
        "duplicate_count": duplicate_count,
    }


def completeness_check(df: DataFrame, interval_ms: int = ONE_MINUTE_MS) -> list[dict]:
    """Expected vs actual row counts per symbol, assuming a contiguous 1-min series."""
    rows = (
        df.groupBy("symbol")
        .agg(
            F.count("*").alias("actual_rows"),
            F.min("open_time").alias("min_open_time"),
            F.max("open_time").alias("max_open_time"),
        )
        .withColumn(
            "expected_rows",
            ((F.col("max_open_time") - F.col("min_open_time")) / F.lit(interval_ms) + 1).cast("long"),
        )
        .withColumn("completeness_ratio", (F.col("actual_rows") / F.col("expected_rows")).cast("double"))
        .collect()
    )
    return [r.asDict() for r in rows]


def continuity_check(df: DataFrame) -> list[dict]:
    """Sums the gap flags produced by transforms.refined.detect_time_gaps, per symbol."""
    if "gap_minutes_before" not in df.columns:
        return []
    rows = (
        df.groupBy("symbol")
        .agg(
            F.sum("gap_minutes_before").alias("total_missing_minutes"),
            F.sum(F.col("has_gap_before").cast("long")).alias("gap_events"),
        )
        .collect()
    )
    return [r.asDict() for r in rows]


def reconciliation_check(raw_df: DataFrame, refined_df: DataFrame) -> list[dict]:
    """Raw-to-refined row-count reconciliation per symbol: what got dropped and why."""
    raw_counts = raw_df.groupBy("symbol").count().withColumnRenamed("count", "raw_rows")
    refined_counts = refined_df.groupBy("symbol").count().withColumnRenamed("count", "refined_rows")
    rows = (
        raw_counts.join(refined_counts, on="symbol", how="outer")
        .fillna(0, subset=["raw_rows", "refined_rows"])
        .withColumn("dropped_rows", F.col("raw_rows") - F.col("refined_rows"))
        .collect()
    )
    return [r.asDict() for r in rows]


def _determine_status(validity: dict, completeness: list[dict]) -> str:
    if validity["invalid_price_count"] > 0 or validity["duplicate_count"] > 0 or validity["total_nulls"] > 0:
        return "fail"
    if any(row["completeness_ratio"] < COMPLETENESS_FAIL_THRESHOLD for row in completeness):
        return "fail"
    if any(row["completeness_ratio"] < COMPLETENESS_WARN_THRESHOLD for row in completeness):
        return "warning"
    return "pass"


def build_quality_report(
    curated_df: DataFrame, raw_df: DataFrame | None = None, refined_df: DataFrame | None = None
) -> dict:
    validity = validity_check(curated_df)
    completeness = completeness_check(curated_df)
    continuity = continuity_check(curated_df)
    reconciliation = reconciliation_check(raw_df, refined_df) if raw_df is not None and refined_df is not None else []

    return {
        "row_count": curated_df.count(),
        "validity": validity,
        "completeness": completeness,
        "continuity": continuity,
        "reconciliation": reconciliation,
        "status": _determine_status(validity, completeness),
    }


def write_quality_report(report: dict, output_path: str) -> None:
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "quality_report.json"
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")


def run_quality_checks(
    curated_df: DataFrame,
    output_path: str,
    raw_df: DataFrame | None = None,
    refined_df: DataFrame | None = None,
) -> dict:
    report = build_quality_report(curated_df, raw_df=raw_df, refined_df=refined_df)
    write_quality_report(report, output_path)
    return report

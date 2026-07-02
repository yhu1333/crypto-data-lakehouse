import json
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def quality_summary(df: DataFrame) -> dict:
    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    ).collect()[0].asDict()

    invalid_conditions = [
        (F.col("close") <= 0),
        (F.col("high") < F.col("low")),
    ]
    if "open" in df.columns:
        invalid_conditions.append(F.col("open") <= 0)

    invalid_price_count = (
        df.filter(reduce(lambda left, right: left | right, invalid_conditions)).count()
        if invalid_conditions
        else 0
    )

    duplicate_count = df.groupBy("symbol", "open_time").count().filter(F.col("count") > 1).count()
    negative_return_count = df.filter(F.col("returns") < 0).count()

    total_nulls = sum(null_counts.values())
    status = "pass"
    if invalid_price_count > 0 or duplicate_count > 0 or total_nulls > 0:
        status = "warning"
    if invalid_price_count > 10 or duplicate_count > 10 or total_nulls > 10:
        status = "fail"

    return {
        "row_count": df.count(),
        "null_counts": null_counts,
        "invalid_price_count": invalid_price_count,
        "duplicate_count": duplicate_count,
        "negative_return_count": negative_return_count,
        "total_nulls": total_nulls,
        "data_availability_status": status,
    }


def write_quality_report(report: dict, output_path: str) -> None:
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "quality_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def run_quality_checks(df: DataFrame, output_path: str) -> dict:
    report = quality_summary(df)
    write_quality_report(report, output_path)
    return report

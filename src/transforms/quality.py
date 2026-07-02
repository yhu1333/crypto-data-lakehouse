import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def quality_summary(df: DataFrame) -> dict:
    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    ).collect()[0].asDict()

    invalid_price_count = df.filter(
        (F.col("close") <= 0)
        | (F.col("high") < F.col("low"))
        | (F.col("open") <= 0)
    ).count()

    return {
        "row_count": df.count(),
        "null_counts": null_counts,
        "invalid_price_count": invalid_price_count,
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

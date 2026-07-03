from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ONE_MINUTE_MS = 60_000


def read_raw(spark: SparkSession, input_path: str) -> DataFrame:
    return spark.read.parquet(input_path)


def deduplicate(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["symbol", "open_time"])


def filter_invalid_rows(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("symbol").isNotNull())
        & (F.col("open_time").isNotNull())
        & (F.col("open").isNotNull())
        & (F.col("close").isNotNull())
        & (F.col("high").isNotNull())
        & (F.col("low").isNotNull())
        & (F.col("volume").isNotNull())
        & (F.col("open") > 0)
        & (F.col("close") > 0)
        & (F.col("low") > 0)
        & (F.col("high") >= F.col("low"))
        & (F.col("volume") >= 0)
    )


def detect_time_gaps(df: DataFrame, interval_ms: int = ONE_MINUTE_MS) -> DataFrame:
    """Flag missing 1-minute buckets per symbol by comparing consecutive open_time values."""
    window_spec = Window.partitionBy("symbol").orderBy("open_time")
    prev_open_time = F.lag("open_time", 1).over(window_spec)
    return (
        df.withColumn(
            "gap_minutes_before",
            F.when(
                prev_open_time.isNotNull(),
                ((F.col("open_time") - prev_open_time) / F.lit(interval_ms) - 1).cast("long"),
            ).otherwise(F.lit(0)),
        )
        .withColumn("has_gap_before", F.col("gap_minutes_before") > 0)
    )


def write_refined(df: DataFrame, output_path: str) -> None:
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.repartition("symbol", "year_month").write.mode("overwrite").partitionBy("symbol", "year_month").parquet(
        str(output_dir)
    )


def run_refined_pipeline(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    df = read_raw(spark, input_path)
    df = deduplicate(df)
    df = filter_invalid_rows(df)
    df = detect_time_gaps(df)
    write_refined(df, output_path)
    return df

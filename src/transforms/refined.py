from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_raw(spark: SparkSession, input_path: str) -> DataFrame:
    return spark.read.parquet(input_path)


def deduplicate(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["symbol", "open_time"])


def filter_invalid_rows(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("symbol").isNotNull())
        & (F.col("open_time").isNotNull())
        & (F.col("close").isNotNull())
        & (F.col("high").isNotNull())
        & (F.col("low").isNotNull())
        & (F.col("volume").isNotNull())
        & (F.col("close") > 0)
        & (F.col("high") >= F.col("low"))
    )


def add_quality_flags(df: DataFrame) -> DataFrame:
    return df.withColumn("is_valid", F.lit(True))


def write_refined(df: DataFrame, output_path: str) -> None:
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").partitionBy("symbol").parquet(str(output_dir))


def run_refined_pipeline(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    df = read_raw(spark, input_path)
    df = deduplicate(df)
    df = filter_invalid_rows(df)
    df = add_quality_flags(df)
    write_refined(df, output_path)
    return df
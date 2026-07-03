import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))
from models.schema import RAW_KLINE_SCHEMA


def read_landing_csv(spark: SparkSession, csv_path: str) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .schema(RAW_KLINE_SCHEMA)
        .load(csv_path)
    )


MICROSECOND_THRESHOLD = 10**14  # any real epoch-ms value is well below this; epoch-us values are well above it


def _to_epoch_millis(col: str):
    """Binance's kline dumps switched open_time/close_time from millisecond to
    microsecond precision starting with 2025 data (data.binance.vision), with no
    flag in the file to tell you which one you got. Normalize both to milliseconds
    so every downstream stage (gap detection, completeness) can assume one unit."""
    return F.when(F.col(col) > F.lit(MICROSECOND_THRESHOLD), (F.col(col) / 1000).cast("long")).otherwise(F.col(col))


def transform_raw(df: DataFrame, symbol: str) -> DataFrame:
    return (
        df.withColumn("symbol", F.lit(symbol))
        .withColumn("open_time", _to_epoch_millis("open_time"))
        .withColumn("close_time", _to_epoch_millis("close_time"))
        .withColumn("open_time_dt", F.from_unixtime(F.col("open_time") / 1000).cast("timestamp"))
        .withColumn("close_time_dt", F.from_unixtime(F.col("close_time") / 1000).cast("timestamp"))
        .withColumn("date", F.to_date("open_time_dt"))
        .withColumn("year_month", F.date_format("open_time_dt", "yyyy-MM"))
        .select(
            "symbol",
            "date",
            "year_month",
            "open_time",
            "open_time_dt",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "close_time_dt",
            "quote_asset_volume",
            "num_trades",
            "taker_buy_base",
            "taker_buy_quote",
        )
    )


def write_raw(df: DataFrame, output_dir: str) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").partitionBy("symbol", "year_month").parquet(str(output_path))


def run_raw_pipeline(spark: SparkSession, csv_path: str, output_dir: str, symbol: str) -> DataFrame:
    raw_df = read_landing_csv(spark, csv_path)
    transformed_df = transform_raw(raw_df, symbol)
    write_raw(transformed_df, output_dir)
    return transformed_df


if __name__ == "__main__":
    from spark_session import create_spark_session

    spark = create_spark_session()
    csv_path = "data/landing/btcusdt_2023_01/BTCUSDT-1m-2023-01.csv"
    output_dir = "data/raw"
    symbol = "BTCUSDT"

    df = run_raw_pipeline(spark, csv_path, output_dir, symbol)
    print(f"Raw rows written: {df.count()}")
    spark.stop()

from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


RAW_SCHEMA = StructType(
    [
        StructField("open_time", LongType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("close_time", LongType(), True),
        StructField("quote_asset_volume", DoubleType(), True),
        StructField("num_trades", LongType(), True),
        StructField("taker_buy_base", DoubleType(), True),
        StructField("taker_buy_quote", DoubleType(), True),
        StructField("ignore", StringType(), True),
    ]
)


def read_landing_csv(spark: SparkSession, csv_path: str) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .schema(RAW_SCHEMA)
        .load(csv_path)
    )


def transform_raw(df: DataFrame, symbol: str) -> DataFrame:
    return (
        df.withColumn("symbol", F.lit(symbol))
        .withColumn("open_time_dt", F.from_unixtime(F.col("open_time") / 1000).cast("timestamp"))
        .withColumn("close_time_dt", F.from_unixtime(F.col("close_time") / 1000).cast("timestamp"))
        .select(
            "symbol",
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
    df.write.mode("overwrite").partitionBy("symbol").parquet(str(output_path))


def run_raw_pipeline(spark: SparkSession, csv_path: str, output_dir: str, symbol: str) -> DataFrame:
    raw_df = read_landing_csv(spark, csv_path)
    transformed_df = transform_raw(raw_df, symbol)
    write_raw(transformed_df, output_dir)
    return transformed_df


if __name__ == "__main__":
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from spark_session import create_spark_session

    spark = create_spark_session()
    csv_path = "data/landing/btcusdt_2023_01/BTCUSDT-1m-2023-01.csv"
    output_dir = "data/raw"
    symbol = "BTCUSDT"

    df = run_raw_pipeline(spark, csv_path, output_dir, symbol)
    print(f"Raw rows written: {df.count()}")
    spark.stop()

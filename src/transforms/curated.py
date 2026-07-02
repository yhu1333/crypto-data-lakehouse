from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def read_refined(spark: SparkSession, input_path: str) -> DataFrame:
    return spark.read.parquet(input_path)


def add_time_ordering(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("symbol").orderBy("open_time")
    return (
        df.withColumn("prev_close", F.lag("close", 1).over(window_spec))
        .withColumn("returns", F.when(F.col("prev_close").isNotNull(), (F.col("close") / F.col("prev_close") - 1).cast("double")).otherwise(F.lit(None)))
    )


def add_moving_averages(df: DataFrame, window_sizes: list[int]) -> DataFrame:
    window_spec = Window.partitionBy("symbol").orderBy("open_time")

    result = df
    for window_size in window_sizes:
        result = result.withColumn(
            f"close_ma_{window_size}",
            F.avg("close").over(window_spec.rowsBetween(-(window_size - 1), 0)),
        )

    return result


def add_vwap(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("symbol").orderBy("open_time").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    price_volume = F.col("close") * F.col("volume")
    cum_volume = F.sum("volume").over(window_spec)
    cum_price_volume = F.sum(price_volume).over(window_spec)

    return df.withColumn("vwap", (cum_price_volume / cum_volume).cast("double"))


def write_curated(df: DataFrame, output_path: str) -> None:
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").partitionBy("symbol").parquet(str(output_dir))


def run_curated_pipeline(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    df = read_refined(spark, input_path)
    df = add_time_ordering(df)
    df = add_moving_averages(df, [20, 50])
    df = add_vwap(df)
    write_curated(df, output_path)
    return df


if __name__ == "__main__":
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from spark_session import create_spark_session

    spark = create_spark_session()
    input_path = "data/refined"
    output_path = "data/curated"

    df = run_curated_pipeline(spark, input_path, output_path)
    print(f"Curated rows written: {df.count()}")
    spark.stop()


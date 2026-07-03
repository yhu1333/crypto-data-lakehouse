from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

RAW_KLINE_SCHEMA = StructType(
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

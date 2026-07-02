from pyspark.sql import Row

from src.transforms.curated import add_moving_averages, add_time_ordering


def test_add_time_ordering_creates_returns(spark):
    rows = [
        Row(symbol="BTCUSDT", open_time=1, close=100.0, high=110.0, low=90.0, volume=10.0),
        Row(symbol="BTCUSDT", open_time=2, close=110.0, high=115.0, low=95.0, volume=10.0),
    ]
    df = spark.createDataFrame(rows)

    result = add_time_ordering(df)

    actual = result.select("returns").collect()[1][0]
    assert abs(actual - 0.1) < 1e-9


def test_add_moving_averages_creates_window_columns(spark):
    rows = [
        Row(symbol="BTCUSDT", open_time=1, close=100.0, high=110.0, low=90.0, volume=10.0),
        Row(symbol="BTCUSDT", open_time=2, close=110.0, high=115.0, low=95.0, volume=10.0),
        Row(symbol="BTCUSDT", open_time=3, close=120.0, high=125.0, low=100.0, volume=10.0),
    ]
    df = spark.createDataFrame(rows)

    result = add_moving_averages(df, [2])

    assert "close_ma_2" in result.columns

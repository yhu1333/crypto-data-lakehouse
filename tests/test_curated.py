from pyspark.sql import Row

from src.transforms.curated import add_moving_averages, add_rolling_volatility, add_time_ordering


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


def test_add_rolling_volatility_is_null_for_first_row_and_set_after(spark):
    rows = [
        Row(symbol="BTCUSDT", open_time=1, close=100.0, high=110.0, low=90.0, volume=10.0, returns=None),
        Row(symbol="BTCUSDT", open_time=2, close=110.0, high=115.0, low=95.0, volume=10.0, returns=0.1),
        Row(symbol="BTCUSDT", open_time=3, close=99.0, high=120.0, low=95.0, volume=10.0, returns=-0.1),
    ]
    df = spark.createDataFrame(rows)

    result = add_rolling_volatility(df, window_size=3).orderBy("open_time").collect()

    assert result[0]["volatility_3"] is None
    assert result[2]["volatility_3"] is not None

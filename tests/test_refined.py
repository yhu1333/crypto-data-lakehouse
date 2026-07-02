from pyspark.sql import Row

from src.transforms.refined import deduplicate, filter_invalid_rows


def test_deduplicate_removes_duplicate_rows(spark):
    rows = [
        Row(symbol="BTCUSDT", open_time=1, close=100.0, high=110.0, low=90.0, volume=10.0),
        Row(symbol="BTCUSDT", open_time=1, close=100.0, high=110.0, low=90.0, volume=10.0),
        Row(symbol="BTCUSDT", open_time=2, close=101.0, high=111.0, low=91.0, volume=11.0),
    ]
    df = spark.createDataFrame(rows)

    result = deduplicate(df)

    assert result.count() == 2


def test_filter_invalid_rows_removes_bad_records(spark):
    rows = [
        Row(symbol="BTCUSDT", open_time=1, close=100.0, high=110.0, low=90.0, volume=10.0),
        Row(symbol="BTCUSDT", open_time=2, close=0.0, high=110.0, low=90.0, volume=10.0),
        Row(symbol="BTCUSDT", open_time=3, close=120.0, high=100.0, low=110.0, volume=10.0),
    ]
    df = spark.createDataFrame(rows)

    result = filter_invalid_rows(df)

    assert result.count() == 1

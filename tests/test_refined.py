from pyspark.sql import Row

from src.transforms.refined import deduplicate, detect_time_gaps, filter_invalid_rows


def _row(symbol="BTCUSDT", open_time=1, open=100.0, close=100.0, high=110.0, low=90.0, volume=10.0):
    return Row(symbol=symbol, open_time=open_time, open=open, close=close, high=high, low=low, volume=volume)


def test_deduplicate_removes_duplicate_rows(spark):
    rows = [
        _row(open_time=1),
        _row(open_time=1),
        _row(open_time=2),
    ]
    df = spark.createDataFrame(rows)

    result = deduplicate(df)

    assert result.count() == 2


def test_filter_invalid_rows_removes_bad_records(spark):
    rows = [
        _row(open_time=1),
        _row(open_time=2, close=0.0),
        _row(open_time=3, high=100.0, low=110.0),
        _row(open_time=4, open=-5.0),
    ]
    df = spark.createDataFrame(rows)

    result = filter_invalid_rows(df)

    assert result.count() == 1


def test_detect_time_gaps_flags_missing_minutes(spark):
    rows = [
        _row(open_time=0),
        _row(open_time=60_000),
        _row(open_time=240_000),
    ]
    df = spark.createDataFrame(rows)

    result = detect_time_gaps(df).orderBy("open_time").collect()

    assert result[0]["has_gap_before"] is False
    assert result[1]["has_gap_before"] is False
    assert result[2]["has_gap_before"] is True
    assert result[2]["gap_minutes_before"] == 2

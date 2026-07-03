from pyspark.sql import Row

from src.transforms.quality import build_quality_report


def _row(open_time, symbol="BTCUSDT", close=100.0, high=110.0, low=90.0, open=100.0):
    return Row(symbol=symbol, open_time=open_time, close=close, high=high, low=low, open=open)


def test_build_quality_report_marks_clean_contiguous_data_as_pass(spark):
    rows = [_row(0), _row(60_000), _row(120_000)]
    df = spark.createDataFrame(rows)

    report = build_quality_report(df)

    assert report["status"] == "pass"


def test_build_quality_report_marks_sparse_data_as_fail(spark):
    rows = [_row(0), _row(60_000 * 1000)]
    df = spark.createDataFrame(rows)

    report = build_quality_report(df)

    assert report["status"] == "fail"
    assert report["completeness"][0]["completeness_ratio"] < 0.9

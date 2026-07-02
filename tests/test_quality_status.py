from pyspark.sql import Row

from src.transforms.quality import quality_summary


def test_quality_summary_marks_data_availability_status(spark):
    rows = [
        Row(symbol="BTCUSDT", open_time=1, close=100.0, high=110.0, low=90.0, volume=10.0, returns=0.0),
        Row(symbol="BTCUSDT", open_time=2, close=110.0, high=115.0, low=95.0, volume=10.0, returns=0.1),
    ]
    df = spark.createDataFrame(rows)

    report = quality_summary(df)

    assert report["data_availability_status"] == "pass"

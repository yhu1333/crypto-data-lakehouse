from pathlib import Path

from pyspark.sql import Row

from src.pipeline_config import load_pipeline_config
from src.transforms.quality import quality_summary


def test_quality_summary_includes_duplicate_and_negative_return_counts(spark):
    rows = [
        Row(symbol="BTCUSDT", open_time=1, close=100.0, high=110.0, low=90.0, volume=10.0, returns=0.0),
        Row(symbol="BTCUSDT", open_time=2, close=110.0, high=115.0, low=95.0, volume=10.0, returns=0.1),
        Row(symbol="BTCUSDT", open_time=2, close=111.0, high=116.0, low=96.0, volume=11.0, returns=-0.1),
    ]
    df = spark.createDataFrame(rows)

    report = quality_summary(df)

    assert report["duplicate_count"] == 1
    assert report["negative_return_count"] == 1


def test_load_pipeline_config_reads_yaml(tmp_path):
    config_path = tmp_path / "pipeline_config.yaml"
    config_path.write_text("source:\n  symbol: ETHUSDT\n", encoding="utf-8")

    config = load_pipeline_config(config_path)

    assert config["source"]["symbol"] == "ETHUSDT"

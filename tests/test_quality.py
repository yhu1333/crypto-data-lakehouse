from pyspark.sql import Row

from src.pipeline_config import load_pipeline_config
from src.transforms.quality import completeness_check, reconciliation_check, validity_check


def test_validity_check_counts_duplicates_and_invalid_prices(spark):
    rows = [
        Row(symbol="BTCUSDT", open_time=1, close=100.0, high=110.0, low=90.0, volume=10.0),
        Row(symbol="BTCUSDT", open_time=2, close=110.0, high=115.0, low=95.0, volume=10.0),
        Row(symbol="BTCUSDT", open_time=2, close=111.0, high=116.0, low=96.0, volume=11.0),
        Row(symbol="BTCUSDT", open_time=3, close=-1.0, high=116.0, low=96.0, volume=11.0),
    ]
    df = spark.createDataFrame(rows)

    result = validity_check(df)

    assert result["duplicate_count"] == 1
    assert result["invalid_price_count"] == 1


def test_completeness_check_ratio_for_contiguous_series(spark):
    rows = [
        Row(symbol="BTCUSDT", open_time=0),
        Row(symbol="BTCUSDT", open_time=60_000),
        Row(symbol="BTCUSDT", open_time=120_000),
    ]
    df = spark.createDataFrame(rows)

    result = completeness_check(df)

    assert result[0]["completeness_ratio"] == 1.0


def test_reconciliation_check_reports_dropped_rows(spark):
    raw_rows = [Row(symbol="BTCUSDT"), Row(symbol="BTCUSDT"), Row(symbol="BTCUSDT")]
    refined_rows = [Row(symbol="BTCUSDT")]
    raw_df = spark.createDataFrame(raw_rows)
    refined_df = spark.createDataFrame(refined_rows)

    result = reconciliation_check(raw_df, refined_df)

    assert result[0]["raw_rows"] == 3
    assert result[0]["refined_rows"] == 1
    assert result[0]["dropped_rows"] == 2


def test_load_pipeline_config_reads_yaml(tmp_path):
    config_path = tmp_path / "pipeline_config.yaml"
    config_path.write_text("source:\n  symbol: ETHUSDT\n", encoding="utf-8")

    config = load_pipeline_config(config_path)

    assert config["source"]["symbol"] == "ETHUSDT"

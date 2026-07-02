from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ingest.download import download_kline_zip, extract_zip
from spark_session import create_spark_session
from transforms.curated import run_curated_pipeline
from transforms.raw import run_raw_pipeline
from transforms.refined import run_refined_pipeline
from transforms.quality import run_quality_checks


def run_pipeline() -> None:
    landing_dir = Path("data/landing")
    raw_output = "data/raw"
    refined_output = "data/refined"
    curated_output = "data/curated"

    symbol = "BTCUSDT"
    interval = "1m"
    year = 2023
    month = 1

    zip_path = download_kline_zip(symbol, interval, year, month, landing_dir, kind="monthly")
    extract_zip(zip_path, landing_dir / "btcusdt_2023_01")

    spark = create_spark_session()
    try:
        csv_path = str(landing_dir / "btcusdt_2023_01" / "BTCUSDT-1m-2023-01.csv")
        run_raw_pipeline(spark, csv_path, raw_output, symbol)
        run_refined_pipeline(spark, raw_output, refined_output)
        curated_df = run_curated_pipeline(spark, refined_output, curated_output)
        report = run_quality_checks(curated_df, "data/quality")
        print("Pipeline completed successfully")
        print(report)
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()

from pathlib import Path
import sys
import time

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ingest.download import download_kline_zip, extract_zip
from pipeline_config import load_pipeline_config, normalize_sources
from spark_session import create_spark_session
from transforms.curated import run_curated_pipeline
from transforms.raw import run_raw_pipeline
from transforms.refined import run_refined_pipeline
from transforms.quality import run_quality_checks


def build_source_key(symbol: str, year: int, month: int, interval: str) -> str:
    return f"{symbol.lower()}_{year:04d}_{month:02d}_{interval}"


def print_progress(index: int, total: int, label: str) -> None:
    filled = int((index / total) * 20) if total else 20
    bar = "#" * filled + "-" * (20 - filled)
    print(f"[{bar}] {index}/{total} {label}")


def run_pipeline() -> None:
    config = load_pipeline_config()
    paths_cfg = config.get("paths", {})

    landing_root = Path(paths_cfg.get("landing_dir", "data/landing"))
    raw_root = Path(paths_cfg.get("raw_output", "data/raw"))
    refined_root = Path(paths_cfg.get("refined_output", "data/refined"))
    curated_root = Path(paths_cfg.get("curated_output", "data/curated"))
    quality_root = Path(paths_cfg.get("quality_output", "data/quality"))

    sources = normalize_sources(config)
    total_sources = len(sources)
    print(f"Starting pipeline for {total_sources} source(s)")
    spark = create_spark_session(app_name=config.get("spark", {}).get("app_name", "crypto-data-lakehouse"), master=config.get("spark", {}).get("master", "local[1]"), executor_memory=config.get("spark", {}).get("executor_memory", "1g"))
    try:
        for idx, source_cfg in enumerate(sources, start=1):
            symbol = source_cfg.get("symbol", "BTCUSDT")
            interval = source_cfg.get("interval", "1m")
            year = int(source_cfg.get("year", 2023))
            month = int(source_cfg.get("month", 1))
            source_key = build_source_key(symbol, year, month, interval)

            landing_dir = landing_root / source_key
            raw_output = str(raw_root / source_key)
            refined_output = str(refined_root / source_key)
            curated_output = str(curated_root / source_key)
            quality_output = str(quality_root / source_key)

            print_progress(idx, total_sources, f"Processing {symbol} {year}-{month:02d}")
            print(f"Landing dir: {landing_dir}")
            zip_path = download_kline_zip(symbol, interval, year, month, landing_dir, kind="monthly")
            if zip_path is None:
                print(f"Skipping {symbol} {year}-{month:02d} because the source file is unavailable")
                continue

            extract_zip(zip_path, landing_dir)

            csv_path = str(landing_dir / f"{symbol.upper()}-{interval}-{year:04d}-{month:02d}.csv")
            print("Stage 1/5: raw transform")
            run_raw_pipeline(spark, csv_path, raw_output, symbol)
            print("Stage 2/5: refined transform")
            run_refined_pipeline(spark, raw_output, refined_output)
            print("Stage 3/5: curated transform")
            curated_df = run_curated_pipeline(spark, refined_output, curated_output)
            print("Stage 4/5: quality report")
            report = run_quality_checks(curated_df, quality_output)
            print("Stage 5/5: complete")
            print(f"Outputs written to: {raw_output}, {refined_output}, {curated_output}, {quality_output}")
            print(f"Completed source {symbol} {year}-{month:02d}")
            print(report)

        print("Pipeline completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()

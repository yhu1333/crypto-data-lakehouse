import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ingest.download import download_kline_zip, extract_zip
from pipeline_config import load_pipeline_config, normalize_sources
from spark_session import create_spark_session
from transforms.curated import run_curated_pipeline
from transforms.quality import run_quality_checks
from transforms.raw import run_raw_pipeline
from transforms.refined import run_refined_pipeline


class QualityCheckFailed(RuntimeError):
    pass


def print_progress(index: int, total: int, label: str) -> None:
    filled = int((index / total) * 20) if total else 20
    bar = "#" * filled + "-" * (20 - filled)
    print(f"[{bar}] {index}/{total} {label}", flush=True)


def timed_stage(label: str, fn, *args, **kwargs):
    """Run fn, printing a start marker and the elapsed time on completion so a
    long-running Spark stage never looks hung on stdout (Spark's own live task
    progress is at http://localhost:4040 while a stage is running)."""
    print(f"{label}...", flush=True)
    start = time.monotonic()
    result = fn(*args, **kwargs)
    print(f"{label} done in {time.monotonic() - start:.1f}s", flush=True)
    return result


def ingest_sources(spark, sources: list[dict], landing_root: Path, raw_output: str) -> None:
    """Download + land + raw-transform every configured symbol/month. Each source only
    overwrites its own (symbol, year_month) partitions in data/raw thanks to dynamic
    partition overwrite mode, so this is safe to re-run incrementally."""
    total = len(sources)
    for idx, source_cfg in enumerate(sources, start=1):
        symbol = source_cfg.get("symbol", "BTCUSDT")
        interval = source_cfg.get("interval", "1m")
        year = int(source_cfg.get("year", 2023))
        month = int(source_cfg.get("month", 1))

        landing_dir = landing_root / f"{symbol.lower()}_{year:04d}_{month:02d}_{interval}"
        print_progress(idx, total, f"Ingesting {symbol} {year}-{month:02d}")

        zip_path = download_kline_zip(symbol, interval, year, month, landing_dir, kind="monthly")
        if zip_path is None:
            print(f"Skipping {symbol} {year}-{month:02d}: source file unavailable", flush=True)
            continue

        extract_zip(zip_path, landing_dir)
        csv_path = str(landing_dir / f"{symbol.upper()}-{interval}-{year:04d}-{month:02d}.csv")
        timed_stage(
            f"  raw transform {symbol} {year}-{month:02d}", run_raw_pipeline, spark, csv_path, raw_output, symbol
        )


def run_pipeline() -> None:
    config = load_pipeline_config()
    paths_cfg = config.get("paths", {})

    landing_root = Path(paths_cfg.get("landing_dir", "data/landing"))
    raw_output = str(paths_cfg.get("raw_output", "data/raw"))
    refined_output = str(paths_cfg.get("refined_output", "data/refined"))
    curated_output = str(paths_cfg.get("curated_output", "data/curated"))
    quality_output = str(paths_cfg.get("quality_output", "data/quality"))

    sources = normalize_sources(config)
    spark_cfg = config.get("spark", {})
    spark = create_spark_session(
        app_name=spark_cfg.get("app_name", "crypto-data-lakehouse"),
        master=spark_cfg.get("master", "local[*]"),
        driver_memory=spark_cfg.get("driver_memory", "4g"),
        shuffle_partitions=str(spark_cfg.get("shuffle_partitions", "16")),
    )
    try:
        timed_stage(
            f"Stage 1/4: ingest ({len(sources)} source(s))", ingest_sources, spark, sources, landing_root, raw_output
        )

        raw_df = spark.read.parquet(raw_output)
        refined_df = timed_stage(
            "Stage 2/4: refined transform (unified golden copy)",
            run_refined_pipeline,
            spark,
            raw_output,
            refined_output,
        )

        curated_df = timed_stage(
            "Stage 3/4: curated transform (unified golden copy)",
            run_curated_pipeline,
            spark,
            refined_output,
            curated_output,
        )

        report = timed_stage(
            "Stage 4/4: quality report",
            run_quality_checks,
            curated_df,
            quality_output,
            raw_df=raw_df,
            refined_df=refined_df,
        )
        print(f"Quality status: {report['status']}", flush=True)
        print(f"Outputs written to: {raw_output}, {refined_output}, {curated_output}, {quality_output}", flush=True)

        if report["status"] == "fail":
            raise QualityCheckFailed(f"Quality checks failed, see {quality_output}/quality_report.json")

        print("Pipeline completed successfully", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()

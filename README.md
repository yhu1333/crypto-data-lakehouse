# Crypto Data Lakehouse

This project implements a small PySpark-based data engineering pipeline for Binance OHLCV data.

## What it does

The pipeline performs the following stages:

1. Download Binance 1-minute kline data
2. Convert raw CSV files into raw Parquet data
3. Clean and refine the data
4. Generate curated analytics features such as returns, moving averages, and VWAP
5. Produce a lightweight quality report

## Project structure

- `src/ingest/` - download logic
- `src/transforms/` - raw, refined, curated, and quality transforms
- `src/run_pipeline.py` - end-to-end entrypoint
- `data/` - landing, raw, refined, curated, and quality outputs
- `tests/` - pytest coverage for transforms

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./.venv/bin/python src/run_pipeline.py
```

## Run tests

```bash
./.venv/bin/pytest -q
```

## Docker

```bash
docker build -t crypto-data-lakehouse .
docker run --rm crypto-data-lakehouse
```

## Current run results

The current pipeline run produced:

- `data/raw` with partitioned Parquet output
- `data/refined` with cleaned Parquet output
- `data/curated` with derived metrics output
- `data/quality/quality_report.json`

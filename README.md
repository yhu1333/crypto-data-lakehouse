# Crypto Data Lakehouse

A PySpark data engineering pipeline that ingests Binance 1-minute OHLCV (kline) data for
multiple symbols, cleans and integrates it into a unified "golden copy," and validates it
with an automated data-quality report. Runs entirely locally (`local[*]` Spark) and writes
to a local Parquet lake — no cloud services required.

## Architecture

```
landing (CSV/zip)  →  raw (typed Parquet)  →  refined (cleaned)  →  curated (unified + metrics)
                                                                              ↓
                                                                     quality report (JSON)
```

- **landing**: raw Binance kline zips/CSVs downloaded from `data.binance.vision`, unmodified.
- **raw**: CSVs read with an explicit schema (`src/models/schema.py`, no `inferSchema`), typed,
  tagged with `symbol`, and written as Parquet.
- **refined**: deduplicated on `(symbol, open_time)`, invalid rows dropped (non-positive
  prices, `high < low`), and missing 1-minute buckets flagged per symbol
  (`gap_minutes_before` / `has_gap_before`).
- **curated**: all symbols unified into one dataset with Spark window-function metrics —
  20/50-period moving averages, VWAP, period returns, and 20-period rolling volatility.
- **quality**: validity (nulls/invalid prices/duplicates on the core OHLCV fields),
  completeness (actual vs. expected row count per symbol), continuity (gap minutes summed
  from refined), and raw→refined row-count reconciliation. The pipeline raises and exits
  non-zero if any hard check fails.

Every layer is partitioned by `symbol` + `year_month` and written with Spark's dynamic
partition overwrite mode, so re-running the pipeline only rewrites the partitions that
actually changed.

## Project structure

- `src/ingest/` - Binance kline downloader
- `src/models/schema.py` - explicit source schema
- `src/transforms/` - raw, refined, curated, and quality transforms
- `src/run_pipeline.py` - end-to-end orchestration (ingest → raw → refined → curated → quality)
- `data/` - landing, raw, refined, curated, and quality outputs (gitignored)
- `tests/` - pytest + Spark-fixture coverage for every transform

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./.venv/bin/python src/run_pipeline.py
```

## Run tests

```bash
ruff check .
./.venv/bin/pytest -q
```

## Docker

```bash
docker build -t crypto-data-lakehouse .
docker run --rm crypto-data-lakehouse
```

Verified end-to-end: build succeeds (~1.6GB image), Spark initializes on the container's
JVM, and a full pipeline run against all 3 symbols produces the same `status: pass` quality
report as the local run. The base image's Debian release only ships OpenJDK 21/25 (no 17),
so the Dockerfile installs `openjdk-21-jre-headless` — Spark 4.x supports Java 21, and a
headless JRE is enough since nothing gets compiled in the container.

## Current run results

Full-volume run: 3 symbols (BTCUSDT, ETHUSDT, BNBUSDT), 1-minute interval, full year 2025.

| Layer | Size on disk | Partitions |
|---|---|---|
| landing | 321M | 36 monthly CSV/zip sets |
| raw | 115M | 36 (`symbol` x `year_month`) |
| refined | 114M | 36 |
| curated | 175M | 36 |

- **Row count**: 1,576,800 curated rows (525,600 per symbol — exactly one row per minute
  for the full year, zero gaps).
- **Quality report** (`data/quality/quality_report.json`): `status: pass` — 0 nulls in key
  fields, 0 invalid prices, 0 duplicates, completeness ratio 1.0 for all 3 symbols, 0 rows
  dropped between raw and refined.

## Design decisions

- **Partitioning granularity**: initially partitioned by `symbol` + calendar day, which for
  1-minute data created ~1,000+ tiny partitions and ballooned `data/raw` to 7-8GB with heavy
  small-file overhead. Repartitioned to `symbol` + `year_month` (36 partitions total),
  which cut total lake size to under 1GB and made writes dramatically faster — a concrete
  example of matching partition grain to query/row-density patterns rather than defaulting
  to the finest available column.
- **Timestamp precision**: Binance's `data.binance.vision` kline dumps switched from
  millisecond to microsecond epoch timestamps starting with 2025 data, with no flag in the
  file to distinguish them. `transforms/raw.py` normalizes both `open_time` and `close_time`
  to milliseconds by magnitude (values above `10**14` are treated as microseconds) so every
  downstream stage can assume one unit.
- **Explicit schema over `inferSchema`**: avoids Spark silently mis-typing columns and makes
  schema drift a visible, testable failure instead of a runtime surprise.
- **Fail-loud quality gate**: `run_pipeline.py` raises if the quality report status is
  `fail`, so a broken run can't silently produce a "curated" dataset downstream consumers
  would trust.

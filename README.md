![CI](https://github.com/yhu1333/crypto-data-lakehouse/actions/workflows/ci.yml/badge.svg)
# Crypto Data Lakehouse

![CI](https://github.com/yhu1333/crypto-data-lakehouse/actions/workflows/ci.yml/badge.svg)

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
JVM, and a containerized pipeline run reproduces the same `status: pass` quality report as
the local run (verified at the 3-symbol/1-year scale; the full 6-symbol/4-year run above was
executed locally). The base image's Debian release only ships OpenJDK 21/25 (no 17), so the
Dockerfile installs `openjdk-21-jre-headless` — Spark 4.x supports Java 21, and a headless
JRE is enough since nothing gets compiled in the container.

## Current run results

Full-volume run: 6 symbols (BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, ADAUSDT), 1-minute
interval, 2022-01 through 2025-12 (4 years).

| Layer | Size on disk | Partitions |
|---|---|---|
| landing | 2.2G | 288 monthly CSV/zip sets |
| raw | 826M | 288 (`symbol` x `year_month`) |
| refined | 823M | 288 |
| curated | 1.2G | 288 |

- **Row count**: 12,622,559 curated rows across all 6 symbols (~2.1M per symbol over 4
  years — one row per minute, essentially zero gaps).
- **Quality report** (`data/quality/quality_report.json`): `status: pass` — 0 nulls in key
  fields, 0 invalid prices, 0 duplicates, completeness ratio ~0.99996 for every symbol
  (the shortfall is a handful of exchange-downtime minutes, not a pipeline defect), 0 rows
  dropped between raw and refined.

## Design decisions

- **Partitioning granularity**: initially partitioned by `symbol` + calendar day, which for
  1-minute data created ~1,000+ tiny partitions and ballooned `data/raw` to 7-8GB (with only
  3 symbols x 1 year) from small-file overhead. Repartitioned to `symbol` + `year_month`,
  which cut `data/raw` for that same run to 115MB and made writes dramatically faster — a
  concrete example of matching partition grain to query/row-density patterns rather than
  defaulting to the finest available column.
- **Explicit schema over `inferSchema`**: avoids Spark silently mis-typing columns and makes
  schema drift a visible, testable failure instead of a runtime surprise.
- **Fail-loud quality gate**: `run_pipeline.py` raises if the quality report status is
  `fail`, so a broken run can't silently produce a "curated" dataset downstream consumers
  would trust.

### Debugging a silent upstream schema change

Binance's `data.binance.vision` kline dumps switched `open_time`/`close_time` from
millisecond to microsecond epoch precision starting with 2025 data — with no flag or schema
change in the file to signal the switch. Every downstream consumer had been assuming
millisecond epochs, so a raw microsecond value read as milliseconds lands roughly 1,000x
further in the future than it should: it was corrupting downstream time calculations
(partitioning by `year_month`, gap detection, return/volatility windows) for every 2025 row
before the fix, without throwing a single error. `transforms/raw.py` now normalizes both
columns to milliseconds by magnitude (`MICROSECOND_THRESHOLD = 10**14` — real epoch-ms
values sit well below it, epoch-us values well above), so every downstream stage can safely
assume one unit.

---

CI runs `ruff check .` and `pytest -q` on every push and pull request (see badge above).

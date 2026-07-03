# Crypto Market Data Lakehouse — Project Spec

> A spec for Claude Code to implement. Put this file in the project root, open in VS Code, and tell Claude Code:
> *"Read CRYPTO_DATA_PIPELINE_SPEC.md and implement it step by step, starting with Day 1. Confirm each day's acceptance check before moving on."*

---

## 0. What this project is (and is not)

**Is:** A production-style **data engineering** pipeline that ingests large-scale crypto OHLCV (candlestick) data from multiple symbols, cleans and integrates it into a single well-modeled "golden copy," validates data quality, and serves analysis-ready curated datasets — all with distributed processing (Spark), columnar storage (Parquet), and CI/CD.

**Is NOT:** A trading strategy, a price predictor, or an ML project. The deliverable is *reliable, well-governed data assets* — the core of data engineering. Do not drift into modeling or backtesting.

**Portfolio purpose:** One general-purpose DE project that demonstrates Spark, distributed processing, layered data modeling, data quality/reconciliation, Parquet, and CI/CD. Usable for any data engineering role.

---

## 1. Data source

**Binance public historical K-line (candlestick) data** — free, no API key, bulk-downloadable.

- Public data dump: `https://data.binance.vision/` (monthly/daily klines as CSV zips)
- Also usable: the Binance REST klines endpoint for smaller top-ups
- **Scope to target ~10–25M rows total, ~1–3 GB on disk:**
  - ~5–8 symbols (e.g., BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, ADAUSDT)
  - 1-minute interval
  - ~2–4 years of history per symbol
- OHLCV schema per row: `open_time, open, high, low, close, volume, close_time, quote_asset_volume, num_trades, taker_buy_base, taker_buy_quote`

**Hardware note (M-series MacBook Air, fanless):** Run Spark in `local[*]` mode with a capped driver memory (see §5). Process in partitions and never `collect()` the full dataset. Develop logic on ONE month of ONE symbol first; only run the full volume once the pipeline is correct. Target: any single stage completes in seconds-to-minutes, not a continuously running job.

---

## 2. Tech stack

| Layer | Choice | Notes |
|---|---|---|
| Processing | Apache Spark (PySpark) | `local[*]`, capped memory |
| Storage format | Parquet (partitioned) | columnar; partition by symbol + date |
| Layering | raw → refined → curated | medallion-style |
| Data quality | custom PySpark checks + reconciliation | completeness, validity, continuity |
| Orchestration (light) | a simple Python runner (`run_pipeline.py`) | optional: Airflow later, not required |
| Packaging | Docker | containerize the job |
| CI/CD | GitHub Actions | lint + unit tests on push |
| Testing | pytest + chispa (Spark df asserts) | test transforms on small fixtures |

Keep it lean. No cloud services required — everything runs locally and writes to a local `data/` lake directory.

---

## 3. Target directory structure

```
crypto-data-lakehouse/
├── README.md
├── CRYPTO_DATA_PIPELINE_SPEC.md      # this file
├── requirements.txt
├── Dockerfile
├── .github/
│   └── workflows/
│       └── ci.yml                    # GitHub Actions: lint + pytest
├── config/
│   └── pipeline_config.yaml          # symbols, date range, paths, spark settings
├── src/
│   ├── __init__.py
│   ├── ingest/
│   │   ├── __init__.py
│   │   └── download.py               # pull Binance kline zips → data/landing/
│   ├── spark_session.py              # builds capped local Spark session
│   ├── transforms/
│   │   ├── __init__.py
│   │   ├── raw.py                    # landing CSV → raw Parquet (typed, partitioned)
│   │   ├── refined.py                # clean: dedupe, nulls, bad prices, time gaps
│   │   ├── curated.py                # unify symbols + derived metrics (MA, VWAP, returns)
│   │   └── quality.py                # data quality + reconciliation checks
│   ├── models/
│   │   └── schema.py                 # explicit schemas (no inferSchema in prod)
│   └── run_pipeline.py               # orchestrates raw→refined→curated→quality
├── data/                             # local lake (gitignored)
│   ├── landing/                      # downloaded raw CSVs
│   ├── raw/                          # typed Parquet, partitioned by symbol/date
│   ├── refined/                      # cleaned Parquet
│   └── curated/                      # analysis-ready Parquet
├── tests/
│   ├── conftest.py                   # shared local Spark fixture
│   ├── fixtures/                     # tiny sample CSV/Parquet for tests
│   ├── test_refined.py
│   ├── test_curated.py
│   └── test_quality.py
└── .gitignore                        # ignore data/, __pycache__, .venv
```

---

## 4. Day-by-day plan (~6–8 days, 3–4 hrs/day)

### Day 1 — Scaffold + ingestion + Spark session
- Create the structure above; set up `.venv`, `requirements.txt` (pyspark, pyyaml, pytest, chispa, requests).
- `spark_session.py`: build a `local[*]` session with capped memory (§5).
- `ingest/download.py`: download a few months of kline zips for 1–2 symbols from `data.binance.vision` into `data/landing/`. Make symbol/date range config-driven.
- `config/pipeline_config.yaml`: symbols, date range, interval, paths, spark memory.
- ✅ Done when: running the ingest script lands CSV files locally and a Spark session starts without OOM.

### Day 2 — Raw layer (typed, partitioned Parquet)
- `models/schema.py`: explicit schema for kline data (no `inferSchema`).
- `transforms/raw.py`: read landing CSVs with the explicit schema, cast types (timestamps, decimals), add a `symbol` column, write to `data/raw/` as **Parquet partitioned by `symbol` and `date`**.
- ✅ Done when: `data/raw/` holds partitioned Parquet; a quick `spark.read.parquet(...).count()` matches input rows.

### Day 3 — Refined layer (cleaning)
- `transforms/refined.py`, handling the messy realities of market data:
  - drop exact duplicate `(symbol, open_time)` rows
  - handle nulls / zero or negative prices / `high < low` anomalies
  - detect and flag **missing time buckets** (gaps in the 1-min series per symbol)
  - enforce monotonic, de-duplicated timestamps per symbol
- Write cleaned output to `data/refined/`.
- ✅ Done when: refined output passes a manual spot-check and row counts/anomaly counts are logged.

### Day 4 — Curated layer (unify + derive)
- `transforms/curated.py`:
  - unify all symbols into one consistently-schem-ed dataset (the "golden copy")
  - compute derived metrics with **Spark window functions**: moving averages (e.g., 20/50-period), VWAP, period returns, rolling volatility
  - partition curated output sensibly for downstream reads
- ✅ Done when: `data/curated/` contains the unified, enriched dataset and window-function metrics are correct on a spot-check.

### Day 5 — Data quality + reconciliation
- `transforms/quality.py`:
  - **completeness**: expected vs actual row counts per symbol/day
  - **validity**: price/volume within sane bounds, no nulls in key fields
  - **continuity**: quantify time-series gaps
  - **reconciliation**: e.g., raw→refined row-count reconciliation, and cross-check aggregates (daily totals) against an independent recompute
  - emit a structured quality report (JSON/CSV) per run
- ✅ Done when: `run_pipeline.py` produces a quality report and fails loudly if a hard check breaks.

### Day 6 — Tests + CI/CD
- `tests/`: unit-test each transform on tiny fixtures using pytest + chispa (assert cleaned output, derived-metric values, quality logic).
- `.github/workflows/ci.yml`: GitHub Actions running `ruff`/`flake8` lint + `pytest` on every push.
- ✅ Done when: tests pass locally and the Actions workflow is green on GitHub.

### Day 7 — Docker + README + full run
- `Dockerfile`: containerize the pipeline (JDK + Python + Spark).
- Run the **full volume** (all symbols, full date range) once; capture row counts, runtime, and the quality report.
- `README.md`: architecture diagram (landing→raw→refined→curated), how-to-run, data-quality results, design decisions (why Spark, why partitioning, schema choices).
- ✅ Done when: `docker run` executes the pipeline end-to-end and the README shows real numbers.

### Day 8 — Buffer
Performance tuning (partition sizing, caching), more tests, cleanup.

---

## 5. Keeping the MacBook Air happy (important)

- Build the Spark session with capped memory, e.g.:
  ```python
  SparkSession.builder
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "16")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  ```
- Develop on **one month / one symbol** until logic is correct. Run full volume only at the end (Day 7).
- Always read/write **Parquet with partition pruning**; never `.collect()` or `.toPandas()` the full dataset.
- Write intermediate layers to disk so you don't recompute upstream stages on every run.
- If memory gets tight, lower the date range or symbol count — the *techniques* are what matter, not raw size.

---

## 6. Build principles

- **Data engineering, not modeling.** The value is clean, reliable, well-modeled data. Do not add price prediction or trading logic.
- **Explicit schemas everywhere.** No `inferSchema` in the pipeline — schema-on-read discipline is a DE signal.
- **Idempotent, layered, partitioned.** Re-running a stage should reproduce the same output; each layer is a clean checkpoint.
- **Data quality is a first-class output**, not an afterthought. Reconciliation is what separates real DE from a script.
- **Depth over breadth.** Get raw→refined→curated→quality working correctly for 1–2 symbols before scaling to all of them.

---

## 7. Resume bullets (fill bracketed values after building)

**Crypto Market Data Lakehouse** — *PySpark, Parquet, Docker, GitHub Actions CI/CD*
- Built a distributed ETL pipeline in PySpark ingesting ~1.6M rows of multi-symbol OHLCV market data, integrating disparate feeds into a unified, well-modeled "golden copy" stored as partitioned Parquet.
- Designed a layered (raw → refined → curated) lakehouse with explicit schemas, deduplication, gap detection, and Spark window-function metrics (moving averages, VWAP, rolling volatility).
- Implemented data-quality and reconciliation controls (completeness, validity, continuity, raw-to-refined row reconciliation) emitting a structured per-run quality report.
- Containerized the pipeline with Docker and added a GitHub Actions CI/CD workflow (lint + pytest on Spark transforms).

---

## 8. Interview talking points this sets up

- **Distributed processing:** why Spark over pandas at this scale; partitioning, shuffle tuning, window functions.
- **Data modeling:** medallion layering, explicit schemas, schema evolution, natural vs surrogate keys, partitioning strategy.
- **Data quality:** completeness/validity/continuity checks, reconciliation, root-cause of breaks — the vocabulary senior DE interviews probe.
- **Engineering discipline:** idempotency, testing Spark transforms, CI/CD, containerization.
- **Formats:** why columnar Parquet, partition pruning, predicate pushdown.

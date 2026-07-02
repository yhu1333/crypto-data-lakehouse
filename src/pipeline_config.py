from pathlib import Path
from typing import Any

import yaml


def load_pipeline_config(config_path: str | Path | None = None) -> dict[str, Any]:
    config_file = Path(config_path or "config/pipeline_config.yaml")
    with config_file.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _expand_monthly_sources(base_source: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(base_source, dict):
        return []

    start_year = int(base_source.get("start_year", base_source.get("year", 2023)))
    start_month = int(base_source.get("start_month", base_source.get("month", 1)))
    end_year = int(base_source.get("end_year", start_year))
    end_month = int(base_source.get("end_month", start_month))

    sources: list[dict[str, Any]] = []
    current_year, current_month = start_year, start_month
    while (current_year, current_month) <= (end_year, end_month):
        sources.append(
            {
                "symbol": base_source.get("symbol", "BTCUSDT"),
                "interval": base_source.get("interval", "1m"),
                "year": current_year,
                "month": current_month,
            }
        )
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    return sources


def normalize_sources(config: dict[str, Any]) -> list[dict[str, Any]]:
    explicit_sources = config.get("sources")
    if isinstance(explicit_sources, list) and explicit_sources:
        expanded: list[dict[str, Any]] = []
        for source in explicit_sources:
            if not isinstance(source, dict):
                continue
            if "start_year" in source or "start_month" in source or "end_year" in source or "end_month" in source:
                expanded.extend(_expand_monthly_sources(source))
            else:
                expanded.append(source)
        return expanded

    single_source = config.get("source")
    if isinstance(single_source, dict):
        return _expand_monthly_sources(single_source)

    return [{"symbol": "BTCUSDT", "interval": "1m", "year": 2023, "month": 1}]

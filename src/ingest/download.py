import zipfile
from pathlib import Path

import requests

BASE_URL = "https://data.binance.vision/data/spot"


def build_download_url(symbol: str, interval: str, year: int, month: int, kind: str = "monthly") -> str:
    symbol_upper = symbol.upper()

    if kind == "monthly":
        return (
            f"{BASE_URL}/monthly/klines/{symbol_upper}/{interval}/"
            f"{symbol_upper}-{interval}-{year:04d}-{month:02d}.zip"
        )

    return (
        f"{BASE_URL}/daily/klines/{symbol_upper}/{interval}/"
        f"{symbol_upper}-{interval}-{year:04d}-{month:02d}-01.zip"
    )


def download_kline_zip(
    symbol: str, interval: str, year: int, month: int, output_dir: Path, kind: str = "monthly"
) -> Path | None:
    output_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{symbol.upper()}-{interval}-{year:04d}-{month:02d}.zip"
    url = build_download_url(symbol, interval, year, month, kind=kind)

    target_path = output_dir / file_name
    if target_path.exists():
        print(f"Already exists: {target_path}")
        return target_path

    print(f"Downloading {url}")
    response = requests.get(url, timeout=60)
    if response.status_code == 404:
        print(f"Skip missing file: {url}")
        return None
    response.raise_for_status()

    target_path.write_bytes(response.content)
    print(f"Saved to {target_path}")

    return target_path


def extract_zip(zip_path: Path, extract_dir: Path) -> None:
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
        print(f"Extracted {zip_path} -> {extract_dir}")


if __name__ == "__main__":
    symbol = "BTCUSDT"
    interval = "1m"
    year = 2023
    month = 1

    landing_dir = Path("data/landing")
    zip_path = download_kline_zip(symbol, interval, year, month, landing_dir, kind="monthly")
    extract_zip(zip_path, landing_dir / "btcusdt_2023_01")
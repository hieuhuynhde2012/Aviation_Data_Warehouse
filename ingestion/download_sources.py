from __future__ import annotations

import os
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

from ingestion.config import INPUT_DIR


def _download(url: str, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.stat().st_size > 0:
        return target
    urlretrieve(url, target)
    return target


def download_bts(year: int = None, month: int = None) -> Path:
    year = int(year or os.getenv("BTS_YEAR", "2026"))
    month = int(month or os.getenv("BTS_MONTH", "4"))
    template = os.getenv(
        "BTS_URL_TEMPLATE",
        "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip",
    )
    url = template.format(year=year, month=month)
    zip_path = INPUT_DIR / "bts" / f"bts_on_time_{year}_{month:02d}.zip"
    csv_path = INPUT_DIR / "bts" / f"bts_on_time_{year}_{month:02d}.csv"
    _download(url, zip_path)
    if not csv_path.exists():
        with zipfile.ZipFile(zip_path) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                raise RuntimeError(f"No CSV found in {zip_path}")
            with archive.open(csv_names[0]) as src, csv_path.open("wb") as dst:
                dst.write(src.read())
    return csv_path


def download_ourairports() -> list[Path]:
    sources = {
        "airports.csv": os.getenv(
            "OURAIRPORTS_AIRPORTS_URL",
            "https://davidmegginson.github.io/ourairports-data/airports.csv",
        ),
        "countries.csv": os.getenv(
            "OURAIRPORTS_COUNTRIES_URL",
            "https://davidmegginson.github.io/ourairports-data/countries.csv",
        ),
        "regions.csv": os.getenv(
            "OURAIRPORTS_REGIONS_URL",
            "https://davidmegginson.github.io/ourairports-data/regions.csv",
        ),
    }
    return [_download(url, INPUT_DIR / "ourairports" / name) for name, url in sources.items()]


def main() -> None:
    paths = [download_bts(), *download_ourairports()]
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()

import asyncio
import csv
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Final, List, Optional, Sequence, Set
from xml.etree import ElementTree as ET

import httpx

# --- Configuration ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

S3_API_ENDPOINT: Final[str] = "https://s3-ap-northeast-1.amazonaws.com"
S3_BUCKET: Final[str] = "data.binance.vision"
S3_XML_NS: Final[str] = "http://s3.amazonaws.com/doc/2006-03-01/"
MAX_CONCURRENT_SYMBOLS: Final[int] = 10
REQUEST_TIMEOUT: Final[int] = 30


# --- Data Structures ---


class Exchange(str, Enum):
    SPOT = "spot"
    PERP = "futures/um"
    FUT = "futures/cm"

    def __str__(self) -> str:
        return self.name.lower()


class Interval(str, Enum):
    DAILY = "daily"
    MONTHLY = "monthly"

    def __str__(self) -> str:
        return self.name.lower()


@dataclass(frozen=True, order=True)
class DataFile:
    date: date
    interval: Interval
    data_type: str
    key: str

    @property
    def url(self) -> str:
        return f"https://{S3_BUCKET}/{self.key}"


# --- S3 API Interaction ---


def get_data_path_prefix(
    exchange: Exchange,
    interval: Interval,
    data_type: str,
    symbol: Optional[str] = None,
) -> str:
    """Constructs the S3 prefix for a given data category."""
    parts = ["data", exchange.value, interval.value, data_type]
    if symbol:
        parts.append(symbol)
    return "/".join(parts) + "/"


async def fetch_s3_listing_page(
    client: httpx.AsyncClient,
    prefix: str,
    delimiter: str,
    marker: Optional[str] = None,
) -> tuple[list[str], bool]:
    """Fetches a single page of items from the S3-like listing."""
    params = {"prefix": prefix, "delimiter": delimiter}
    if marker:
        params["marker"] = marker

    try:
        response = await client.get(
            f"/{S3_BUCKET}", params=params, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
        ns = {"s3": S3_XML_NS}

        keys = [
            elem.text
            for elem in root.findall(".//s3:Key", ns)
            if elem.text and not elem.text.endswith(".CHECKSUM")
        ]
        prefixes = [elem.text for elem in root.findall(".//s3:Prefix", ns) if elem.text]

        # S3 returns either Contents (files) or CommonPrefixes (directories)
        items = keys or prefixes

        is_truncated = root.findtext("s3:IsTruncated", "false", ns) == "true"
        return items, is_truncated
    except (httpx.RequestError, ET.ParseError) as e:
        logging.error(f"Failed to fetch {prefix} with marker {marker}: {e}")
        return [], False


async def fetch_all_s3_keys(
    client: httpx.AsyncClient,
    prefix: str,
    delimiter: str = "",
    start_after: Optional[str] = None,
) -> List[str]:
    """Fetches all keys from an S3 listing, handling pagination."""
    all_keys: List[str] = []
    marker = start_after
    while True:
        keys, is_truncated = await fetch_s3_listing_page(
            client, prefix, delimiter, marker
        )
        if not keys:
            break
        all_keys.extend(keys)
        if not is_truncated:
            break
        marker = keys[-1]
    return all_keys


# --- Core Logic ---


async def fetch_symbols(client: httpx.AsyncClient, exchange: Exchange) -> List[str]:
    """Fetches all trading symbols for a given exchange."""
    logging.info(f"Fetching all symbols for exchange: {exchange}...")
    prefix = get_data_path_prefix(exchange, Interval.DAILY, "aggTrades")
    # The result prefixes look like 'data/spot/daily/aggTrades/ETHBTC/'
    # We just want the 'ETHBTC' part.
    prefixes = await fetch_all_s3_keys(client, prefix, delimiter="/")
    symbols = [p.split("/")[-2] for p in prefixes]
    logging.info(f"Found {len(symbols)} symbols for {exchange}.")
    return symbols


def parse_key_to_datafile(key: str, exchange: Exchange, interval: Interval) -> Optional[DataFile]:
    """Parses an S3 key string into a DataFile object."""
    parts = key.split('/')
    num_exchange_parts = exchange.value.count('/') + 1
    data_type_index = 1 + num_exchange_parts + 1  # "data" + exchange_parts + "interval"
    if len(parts) <= data_type_index:
        return None
    data_type = parts[data_type_index]

    date_pattern = (
        r"-(\d{4}-\d{2}-\d{2})\.zip$"
        if interval == Interval.DAILY
        else r"-(\d{4}-\d{2})\.zip$"
    )
    match = re.search(date_pattern, key)
    if not match:
        return None
    date_str = match.group(1)
    try:
        file_date = (
            datetime.strptime(date_str, "%Y-%m-%d").date()
            if interval == Interval.DAILY
            else datetime.strptime(f"{date_str}-01", "%Y-%m-%d").date()
        )
        return DataFile(date=file_date, interval=interval, data_type=data_type, key=key)
    except ValueError:
        return None


async def fetch_data_files(
    client: httpx.AsyncClient,
    exchange: Exchange,
    symbol: str,
    interval: Interval,
    data_type: str,
    start_after: Optional[str] = None,
) -> List[DataFile]:
    """Fetches all new data file entries for a symbol."""
    prefix = get_data_path_prefix(exchange, interval, data_type, symbol)
    keys = await fetch_all_s3_keys(client, prefix, start_after=start_after)
    files = [parse_key_to_datafile(key, exchange, interval) for key in keys]
    return [f for f in files if f]


# --- Local Index (CSV) Management ---


def get_index_path(base_dir: Path, exchange: Exchange, symbol: str) -> Path:
    """Gets the file path for a symbol's index CSV."""
    return base_dir / str(exchange) / f"{symbol}.csv"


def read_index(path: Path) -> List[DataFile]:
    """Reads the index CSV for a symbol, returning a list of DataFile objects."""
    if not path.is_file():
        return []

    files: List[DataFile] = []
    try:
        with path.open("r", newline="") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                dt = date.fromisoformat(row[0])
                interval = Interval(row[1])
                data_type = row[2]
                key = row[3]
                files.append(DataFile(date=dt, interval=interval, data_type=data_type, key=key))
    except (IOError, IndexError, ValueError, StopIteration) as e:
        logging.warning(f"Could not read or parse index file {path}: {e}")
        return []
    return files


def write_index(path: Path, files: Sequence[DataFile]):
    """Writes a sorted, unique list of DataFile objects to a CSV index."""
    if not files:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    # Sort and remove duplicates
    unique_sorted_files = sorted(list(set(files)))
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "interval", "data_type", "key"])
        for file in unique_sorted_files:
            writer.writerow([file.date.isoformat(), file.interval.value, file.data_type, file.key])
    logging.info(f"Wrote {len(unique_sorted_files)} entries to {path}")


# --- Main Application Logic ---


async def update_symbol_index(
    client: httpx.AsyncClient,
    base_dir: Path,
    exchange: Exchange,
    symbol: str,
    semaphore: asyncio.Semaphore,
):
    """Updates the index for a single symbol."""
    async with semaphore:
        logging.info(f"Updating index for {exchange}/{symbol}...")
        path = get_index_path(base_dir, exchange, symbol)
        existing_files = read_index(path)

        # Find the last key for each interval and data type to resume fetching
        existing_set: Set[DataFile] = set(existing_files)

        tasks = []
        
        # aggTrades for monthly and daily
        last_monthly_agg_key = max(
            (f.key for f in existing_set if f.interval == Interval.MONTHLY and f.data_type == "aggTrades"),
            default=None,
        )
        tasks.append(fetch_data_files(
            client, exchange, symbol, Interval.MONTHLY, "aggTrades", start_after=last_monthly_agg_key
        ))
        last_daily_agg_key = max(
            (f.key for f in existing_set if f.interval == Interval.DAILY and f.data_type == "aggTrades"),
            default=None,
        )
        tasks.append(fetch_data_files(
            client, exchange, symbol, Interval.DAILY, "aggTrades", start_after=last_daily_agg_key
        ))

        # bookDepth for daily only
        last_daily_bookdepth_key = max(
            (f.key for f in existing_set if f.interval == Interval.DAILY and f.data_type == "bookDepth"),
            default=None,
        )
        tasks.append(fetch_data_files(
            client, exchange, symbol, Interval.DAILY, "bookDepth", start_after=last_daily_bookdepth_key
        ))

        # metrics for daily only
        last_daily_metrics_key = max(
            (f.key for f in existing_set if f.interval == Interval.DAILY and f.data_type == "metrics"),
            default=None,
        )
        tasks.append(fetch_data_files(
            client, exchange, symbol, Interval.DAILY, "metrics", start_after=last_daily_metrics_key
        ))

        new_files_results = await asyncio.gather(*tasks)
        new_files = [file for result in new_files_results for file in result]

        all_files = list(existing_set) + new_files

        if len(all_files) > len(existing_set):
            write_index(path, all_files)
        else:
            logging.info(f"No new data for {exchange}/{symbol}.")


async def update_exchanges(base_dir: Path, exchanges_to_sync: Sequence[Exchange]):
    """Updates the index for all symbols in the specified exchanges."""
    async with httpx.AsyncClient(base_url=S3_API_ENDPOINT, http2=True) as client:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_SYMBOLS)
        for exchange in exchanges_to_sync:
            symbols = await fetch_symbols(client, exchange)
            if not symbols:
                logging.warning(f"No symbols found for {exchange}, skipping.")
                continue

            tasks = [
                update_symbol_index(client, base_dir, exchange, symbol, semaphore)
                for symbol in symbols
            ]
            await asyncio.gather(*tasks)


def main():
    """Main entry point for the index synchronization script."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and update Binance historical data index.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "data_dir",
        type=Path,
        help="The root directory to store the index CSV files.",
    )
    parser.add_argument(
        "--exchanges",
        nargs="+",
        type=str,
        choices=[e.name.lower() for e in Exchange],
        default=["spot", "perp", "fut"],
        help="A list of exchanges to sync.",
    )
    args = parser.parse_args()

    selected_exchanges = [Exchange[e.upper()] for e in args.exchanges]

    logging.info(f"Starting index sync for: {[str(e) for e in selected_exchanges]}")
    logging.info(f"Data will be stored in: {args.data_dir.resolve()}")

    asyncio.run(update_exchanges(args.data_dir, selected_exchanges))

    logging.info("Index synchronization complete.")


if __name__ == "__main__":
    main()

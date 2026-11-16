import os
import logging
from pathlib import Path
import pandas as pd
from src.config import DEFAULT_CSV_PATH, PROJECT_ROOT, CSV_FILENAME, CHUNK_SIZE

log = logging.getLogger("etl.extract")


def resolve_csv_path(custom_path: str = None) -> Path:
    """
    Locates the source CSV file across multiple fallback locations:
    1. Custom explicit path passed as argument
    2. TAXI_CSV environment variable
    3. data/raw/yellow_tripdata_2015-01.csv
    4. Project root / yellow_tripdata_2015-01.csv
    """
    candidates = []
    if custom_path:
        candidates.append(Path(custom_path))
    if os.getenv("TAXI_CSV"):
        candidates.append(Path(os.getenv("TAXI_CSV")))
    candidates.append(DEFAULT_CSV_PATH)
    candidates.append(PROJECT_ROOT / CSV_FILENAME)

    for candidate in candidates:
        if candidate.is_dir():
            candidate = candidate / CSV_FILENAME
        if candidate.is_file():
            log.info("Resolved CSV source path: %s", candidate)
            return candidate

    raise FileNotFoundError(
        f"Source CSV not found in any standard location: {[str(c) for c in candidates]}. "
        f"Place {CSV_FILENAME} into data/raw/ or set TAXI_CSV."
    )


def extract_chunks(csv_path: Path, chunk_size: int = CHUNK_SIZE):
    """
    Yields successive DataFrame chunks from the source CSV.
    Keeps memory footprint constant regardless of total dataset size.
    """
    log.info("Streaming chunks from %s (chunksize=%s)", csv_path.name, f"{chunk_size:,}")
    return pd.read_csv(csv_path, chunksize=chunk_size)

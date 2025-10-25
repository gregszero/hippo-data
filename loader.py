import polars as pl
from pathlib import Path
from typing import List
from logger import setup_logger

log = setup_logger()

def load_pharmacies(dirs: List[Path]) -> pl.DataFrame:
    dfs = []
    for dir_path in dirs:
        if not dir_path.exists() or not dir_path.is_dir():
            log.warning(f"Directory not found: {dir_path}")
            continue

        for file in dir_path.iterdir():
            if file.suffix.lower() != ".csv":
                continue

            try:
                df = pl.read_csv(file, try_parse_dates=False)

                if not {"npi", "chain"}.issubset(df.columns):
                    log.warning(f"Skipping {file.name}: missing 'npi' or 'chain'")
                    continue

                df = df.with_columns(
                    pl.col("npi").cast(pl.Utf8)
                ).select(["npi", "chain"]).unique(subset="npi")

                dfs.append(df)
                log.info(f"Loaded {len(df)} pharmacies from {file.name}")

            except Exception as e:
                log.error(f"Failed to read CSV {file}: {e}")

    if not dfs:
        raise ValueError("No valid pharmacy data found.")

    result = pl.concat(dfs)
    log.info(f"Total pharmacies loaded: {len(result)}")
    return result

def load_events(dirs: List[Path], schema: dict, event_type: str) -> pl.DataFrame:
    dfs = []
    for dir_path in dirs:
        if not dir_path.exists() or not dir_path.is_dir():
            log.warning(f"{event_type.capitalize()} directory not found: {dir_path}")
            continue
        for file in dir_path.glob("*.json"):
            try:
                df = pl.read_json(file, schema=schema).unique(subset="id")
                dfs.append(df)
                log.info(f"Loaded {len(df)} {event_type} from {file.name}")
            except Exception as e:
                log.error(f"Failed to read {event_type} file {file}: {e}")
    if not dfs:
        log.warning(f"No {event_type} data loaded.")
        return pl.DataFrame(schema=schema)
    return pl.concat(dfs)

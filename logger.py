import logging
from pathlib import Path

def setup_logger(log_dir: Path = Path(".")) -> logging.Logger:
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "analytics.log"

    logger = logging.getLogger("pharmacy")
    logger.setLevel(logging.INFO)

    logger.handlers.clear()

    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

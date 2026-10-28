"""Logging to the console and to outputs/logs/pipeline.log."""
import logging

from src.pipeline.db import PROJECT_ROOT

LOG_FILE = PROJECT_ROOT / "outputs" / "logs" / "pipeline.log"
LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name):
    """Return a logger that writes to both the console and the log file."""
    logger = logging.getLogger(name)
    if logger.handlers:  # already configured
        return logger

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(LOG_FORMAT)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    return logger

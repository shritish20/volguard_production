import logging
import sys
from pythonjsonlogger import jsonlogger
from pathlib import Path

def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    # JSON Formatter
    formatter = jsonlogger.JsonFormatter('%(asctime)s %(name)s %(levelname)s %(message)s')

    # Console
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File
    file_handler = logging.FileHandler("logs/volguard.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

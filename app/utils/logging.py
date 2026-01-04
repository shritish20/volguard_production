import logging
import sys
from pathlib import Path
from pythonjsonlogger import jsonlogger
from datetime import datetime

def setup_logging(log_level: str = "INFO"):
    """
    Setup structured logging with rotation and console output.
    """
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.handlers.clear()

    # JSON Formatter with extra fields
    class CustomJsonFormatter(jsonlogger.JsonFormatter):
        def add_fields(self, log_record, record, message_dict):
            super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
            log_record['timestamp'] = datetime.utcnow().isoformat()
            log_record['level'] = record.levelname
            log_record['logger'] = record.name
            log_record['function'] = record.funcName
            log_record['line'] = record.lineno

    formatter = CustomJsonFormatter(
        '%(timestamp)s %(level)s %(name)s %(funcName)s %(message)s'
    )

    # Console Handler (Human-readable for development)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console.setFormatter(console_formatter)
    logger.addHandler(console)

    # File Handler (JSON for parsing)
    file_handler = logging.FileHandler(
        log_dir / f"volguard_{datetime.now().strftime('%Y%m%d')}.log"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Error File Handler (Separate file for errors)
    error_handler = logging.FileHandler(
        log_dir / f"volguard_errors_{datetime.now().strftime('%Y%m%d')}.log"
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    # Suppress noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logger

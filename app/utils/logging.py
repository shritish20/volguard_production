"""
Structured logging setup.
"""
import logging
import sys
from pythonjsonlogger import jsonlogger
from pathlib import Path

def setup_logging():
    """Setup structured logging"""
    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logger = logging.getLogger("volguard")
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # JSON formatter
    json_formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s %(module)s %(funcName)s"
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(json_formatter)
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler("logs/volguard.log")
    file_handler.setFormatter(json_formatter)
    logger.addHandler(file_handler)
    
    # Supervisor log
    supervisor_handler = logging.FileHandler("logs/supervisor.log")
    supervisor_handler.setFormatter(json_formatter)
    supervisor_logger = logging.getLogger("volguard.supervisor")
    supervisor_logger.addHandler(supervisor_handler)
    
    return logger

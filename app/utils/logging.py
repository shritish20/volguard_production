import logging
import sys
from pathlib import Path
from pythonjsonlogger import jsonlogger
from datetime import datetime
import time
import gzip
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

class EnhancedJsonFormatter(jsonlogger.JsonFormatter):
    """Enhanced JSON formatter with additional fields"""
    
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        
        # Standard fields
        log_record['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        log_record['level'] = record.levelname
        log_record['logger'] = record.name
        log_record['function'] = record.funcName
        log_record['line'] = record.lineno
        log_record['module'] = record.module
        log_record['thread'] = record.threadName
        log_record['thread_id'] = record.thread
        
        # Application-specific fields
        log_record['application'] = 'VolGuard'
        log_record['environment'] = os.getenv('ENVIRONMENT', 'unknown')
        
        # Process info
        log_record['process_id'] = os.getpid()
        
        # Add duration for performance logging
        if hasattr(record, 'duration_ms'):
            log_record['duration_ms'] = record.duration_ms
            
        # Add any extra fields from record
        if hasattr(record, 'extra_fields'):
            log_record.update(record.extra_fields)

class GZipRotatingFileHandler(TimedRotatingFileHandler):
    """Handler that rotates and compresses log files"""
    
    def __init__(self, filename, when='midnight', interval=1, backupCount=7, 
                 encoding=None, delay=False, utc=False, atTime=None):
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc, atTime)
        
    def doRollover(self):
        """Override to compress old log files"""
        super().doRollover()
        
        # Compress the rolled file
        for i in range(self.backupCount - 1, 0, -1):
            sfn = self.rotation_filename(f"{self.baseFilename}.{i}")
            sfn_gz = f"{sfn}.gz"
            
            if os.path.exists(sfn):
                if os.path.exists(sfn_gz):
                    os.remove(sfn_gz)
                    
                # Compress with gzip
                with open(sfn, 'rb') as f_in:
                    with gzip.open(sfn_gz, 'wb') as f_out:
                        f_out.writelines(f_in)
                        
                os.remove(sfn)
                
        # Handle the current backupCount file
        sfn = self.rotation_filename(f"{self.baseFilename}.{self.backupCount}")
        if os.path.exists(sfn):
            os.remove(sfn)

class PerformanceFilter(logging.Filter):
    """Filter to add performance metrics to logs"""
    
    def filter(self, record):
        # Add performance context if available
        if not hasattr(record, 'duration_ms'):
            record.duration_ms = 0
        return True

def setup_logging(log_level: str = "INFO", log_dir: str = "logs"):
    """
    ACTUAL enhanced logging setup with all claimed features:
    - Structured JSON logging
    - Log rotation (time-based and size-based)
    - GZIP compression of old logs
    - Separate error logs
    - Performance tracking
    - Console and file outputs
    """
    # Create log directory
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # ============================================
    # 1. JSON FILE HANDLER (Main log with rotation)
    # ============================================
    
    # Enhanced JSON formatter
    json_formatter = EnhancedJsonFormatter(
        '%(timestamp)s %(level)s %(logger)s %(function)s:%(lineno)d %(message)s'
    )
    
    # Time-based rotating file handler (daily rotation)
    time_rotating_handler = GZipRotatingFileHandler(
        filename=log_path / f"volguard_{datetime.now().strftime('%Y%m%d')}.log",
        when='midnight',  # Rotate daily at midnight
        interval=1,
        backupCount=30,  # Keep 30 days of logs
        encoding='utf-8',
        delay=False
    )
    time_rotating_handler.setLevel(logging.DEBUG)  # Capture everything
    time_rotating_handler.setFormatter(json_formatter)
    time_rotating_handler.addFilter(PerformanceFilter())
    logger.addHandler(time_rotating_handler)
    
    # ============================================
    # 2. ERROR-ONLY FILE HANDLER (Separate error log)
    # ============================================
    
    error_handler = RotatingFileHandler(
        filename=log_path / f"volguard_errors_{datetime.now().strftime('%Y%m%d')}.log",
        maxBytes=10 * 1024 * 1024,  # 10 MB per file
        backupCount=5,  # Keep 5 error log files
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)  # Only errors and critical
    error_handler.setFormatter(json_formatter)
    logger.addHandler(error_handler)
    
    # ============================================
    # 3. PERFORMANCE LOG HANDLER (Separate performance log)
    # ============================================
    
    perf_handler = RotatingFileHandler(
        filename=log_path / f"volguard_perf_{datetime.now().strftime('%Y%m%d')}.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB per file
        backupCount=3,
        encoding='utf-8'
    )
    perf_handler.setLevel(logging.INFO)
    perf_formatter = EnhancedJsonFormatter(
        '%(timestamp)s %(level)s %(logger)s %(function)s %(duration_ms).2fms %(message)s'
    )
    perf_handler.setFormatter(perf_formatter)
    
    # Create a separate logger for performance
    perf_logger = logging.getLogger('volguard.performance')
    perf_logger.setLevel(logging.INFO)
    perf_logger.addHandler(perf_handler)
    perf_logger.propagate = False  # Don't propagate to root
    
    # ============================================
    # 4. CONSOLE HANDLER (Human-readable for development)
    # ============================================
    
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    
    # Color formatter for console
    class ColorFormatter(logging.Formatter):
        """Color formatter for console output"""
        
        grey = "\x1b[38;20m"
        green = "\x1b[32;20m"
        yellow = "\x1b[33;20m"
        red = "\x1b[31;20m"
        bold_red = "\x1b[31;1m"
        blue = "\x1b[34;20m"
        reset = "\x1b[0m"
        
        FORMATS = {
            logging.DEBUG: f"{grey}%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d - %(message)s{reset}",
            logging.INFO: f"{green}%(asctime)s [%(levelname)s] %(name)s:%(funcName)s - %(message)s{reset}",
            logging.WARNING: f"{yellow}%(asctime)s [%(levelname)s] %(name)s:%(funcName)s - %(message)s{reset}",
            logging.ERROR: f"{red}%(asctime)s [%(levelname)s] %(name)s:%(funcName)s - %(message)s{reset}",
            logging.CRITICAL: f"{bold_red}%(asctime)s [%(levelname)s] %(name)s:%(funcName)s - %(message)s{reset}"
        }
        
        def format(self, record):
            log_fmt = self.FORMATS.get(record.levelno, self.FORMATS[logging.DEBUG])
            formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
            return formatter.format(record)
    
    console.setFormatter(ColorFormatter())
    logger.addHandler(console)
    
    # ============================================
    # 5. AUDIT TRAIL HANDLER (For compliance)
    # ============================================
    
    audit_handler = RotatingFileHandler(
        filename=log_path / f"volguard_audit_{datetime.now().strftime('%Y%m%d')}.log",
        maxBytes=20 * 1024 * 1024,  # 20 MB per file
        backupCount=90,  # Keep 90 days for compliance
        encoding='utf-8'
    )
    audit_handler.setLevel(logging.INFO)
    
    # Special audit formatter with more fields
    audit_formatter = EnhancedJsonFormatter(
        '%(timestamp)s %(level)s %(logger)s %(function)s %(message)s %(extra_fields)s'
    )
    audit_handler.setFormatter(audit_formatter)
    
    # Create separate audit logger
    audit_logger = logging.getLogger('volguard.audit')
    audit_logger.setLevel(logging.INFO)
    audit_logger.addHandler(audit_handler)
    audit_logger.propagate = False
    
    # ============================================
    # 6. SUPPRESS NOISY LIBRARIES
    # ============================================
    
    # Reduce noise from external libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    
    # ============================================
    # 7. LOG INITIALIZATION MESSAGE
    # ============================================
    
    logger.info(f"Logging initialized at level {log_level}")
    logger.info(f"Log directory: {log_path.absolute()}")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'unknown')}")
    
    return logger

# ============================================
# PERFORMANCE LOGGING DECORATORS
# ============================================

def log_performance(logger_name='volguard.performance'):
    """Decorator to log function performance"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            duration_ms = (time.time() - start_time) * 1000
            
            logger = logging.getLogger(logger_name)
            logger.info(
                f"Function {func.__name__} completed",
                extra={
                    'duration_ms': duration_ms,
                    'function': func.__name__,
                    'module': func.__module__
                }
            )
            
            return result
        
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            result = await func(*args, **kwargs)
            duration_ms = (time.time() - start_time) * 1000
            
            logger = logging.getLogger(logger_name)
            logger.info(
                f"Async function {func.__name__} completed",
                extra={
                    'duration_ms': duration_ms,
                    'function': func.__name__,
                    'module': func.__module__
                }
            )
            
            return result
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator

def log_with_context(**extra_fields):
    """Decorator to add context to logs"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Get logger
            logger = logging.getLogger(f"volguard.{func.__module__}")
            
            # Log entry
            logger.debug(
                f"Entering {func.__name__}",
                extra={'extra_fields': {**extra_fields, 'action': 'enter'}}
            )
            
            try:
                result = func(*args, **kwargs)
                
                # Log exit
                logger.debug(
                    f"Exiting {func.__name__}",
                    extra={'extra_fields': {**extra_fields, 'action': 'exit', 'success': True}}
                )
                
                return result
                
            except Exception as e:
                # Log error
                logger.error(
                    f"Error in {func.__name__}: {e}",
                    extra={'extra_fields': {**extra_fields, 'action': 'error', 'success': False, 'error': str(e)}}
                )
                raise
        
        async def async_wrapper(*args, **kwargs):
            logger = logging.getLogger(f"volguard.{func.__module__}")
            
            logger.debug(
                f"Entering async {func.__name__}",
                extra={'extra_fields': {**extra_fields, 'action': 'enter'}}
            )
            
            try:
                result = await func(*args, **kwargs)
                
                logger.debug(
                    f"Exiting async {func.__name__}",
                    extra={'extra_fields': {**extra_fields, 'action': 'exit', 'success': True}}
                )
                
                return result
                
            except Exception as e:
                logger.error(
                    f"Error in async {func.__name__}: {e}",
                    extra={'extra_fields': {**extra_fields, 'action': 'error', 'success': False, 'error': str(e)}}
                )
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator

# Import asyncio for async wrapper
import asyncio

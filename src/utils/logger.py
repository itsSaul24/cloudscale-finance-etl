"""
Logging configuration for CloudScale Finance ETL
Sets up structured logging with appropriate formatting
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler

from src.utils.config import Config


def setup_logger(name=None):
    """
    Set up logger with console and file output
    
    Args:
        name: Logger name (defaults to root logger)
        
    Returns:
        Configured logger instance
    """
    # Get logger
    logger = logging.getLogger(name)
    
    # Set level from config
    level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler (only if not in test mode)
    if 'pytest' not in sys.modules:
        log_dir = Path(__file__).parent.parent.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger


# Create a default logger for the package
logger = setup_logger('cloudscale_etl')


# Example usage functions
def log_start(task_name):
    """Log the start of a task"""
    logger.info(f"Starting {task_name}")
    

def log_success(task_name, message=""):
    """Log successful completion of a task"""
    msg = f"Successfully completed {task_name}"
    if message:
        msg += f": {message}"
    logger.info(msg)


def log_error(task_name, error):
    """Log an error with context"""
    logger.error(f"Error in {task_name}: {str(error)}", exc_info=True)


def log_metric(metric_name, value):
    """Log a metric value"""
    logger.info(f"METRIC - {metric_name}: {value}")


if __name__ == "__main__":
    # Test logging
    test_logger = setup_logger('test')
    test_logger.debug("This is a debug message")
    test_logger.info("This is an info message")
    test_logger.warning("This is a warning message")
    test_logger.error("This is an error message")
    
    # Test helper functions
    log_start("test task")
    log_success("test task", "processed 100 records")
    log_metric("records_processed", 100)
    
    try:
        raise ValueError("Test error")
    except Exception as e:
        log_error("test task", e)
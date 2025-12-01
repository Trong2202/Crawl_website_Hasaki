"""
Simple logging setup with UTF-8 support for Vietnamese characters
"""
import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logger(name="hasaki_crawler"):
    """Setup simple console logger with UTF-8 encoding (Windows compatible)"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Format: Simplified without timestamp for cleaner output
    formatter = logging.Formatter('%(message)s')
    
    # Fix Windows encoding issue for Vietnamese characters
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding='utf-8')
    
    # Console handler only
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


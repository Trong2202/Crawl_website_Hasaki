"""
Simple logging setup with UTF-8 support for Vietnamese characters
"""
import logging
import sys
import io
from pathlib import Path
from datetime import datetime


def setup_logger(name="hasaki_crawler"):
    """Setup simple console logger with UTF-8 encoding (Windows compatible)"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Format
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Fix Windows encoding issue for Vietnamese characters
    # Wrap stdout in UTF-8 TextIOWrapper
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, 
            encoding='utf-8', 
            errors='replace'
        )
    
    # Console handler only
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


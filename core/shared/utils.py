"""
SHARED UTILITIES
Location: core/shared/utils.py
Role: Common helper functions like logging, time manipulation, etc.
"""

import logging
import sys

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Factory untuk membuat logger yang seragam.
    Output: [JAM] [LEVEL] [MODULE] Pesan
    """
    logger = logging.getLogger(name)
    
    # Mencegah duplicate logs jika get_logger dipanggil berkali-kali
    if logger.hasHandlers():
        return logger
        
    logger.setLevel(level)
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    
    # Create formatter
    # Format: 2023-10-27 10:00:00 [INFO] [core.risk.manager] Pesan anda
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

__all__ = ['get_logger']

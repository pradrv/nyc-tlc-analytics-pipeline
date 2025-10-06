"""Utility functions for NYC Taxi Pipeline"""

import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from dateutil.relativedelta import relativedelta
from loguru import logger


def generate_month_range(start_date: str, end_date: str) -> List[Tuple[int, int]]:
    """
    Generate list of (year, month) tuples for date range

    Args:
        start_date: Start date in 'YYYY-MM' format
        end_date: End date in 'YYYY-MM' format

    Returns:
        List of (year, month) tuples

    Example:
        >>> generate_month_range('2024-01', '2024-03')
        [(2024, 1), (2024, 2), (2024, 3)]
    """
    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")

    months = []
    current = start

    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    return months


def calculate_file_checksum(file_path: Path, algorithm: str = "sha256") -> str:
    """
    Calculate checksum of file

    Args:
        file_path: Path to file
        algorithm: Hash algorithm ('sha256', 'md5', etc.)

    Returns:
        Hex digest of file hash
    """
    hash_func = hashlib.new(algorithm)

    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hash_func.update(chunk)

    return hash_func.hexdigest()


def format_bytes(bytes_val: int) -> str:
    """
    Format bytes as human-readable string

    Args:
        bytes_val: Number of bytes

    Returns:
        Formatted string (e.g., '1.5 GB')
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_val < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds as human-readable string

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., '2m 30s')
    """
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)

    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"

    hours = minutes // 60
    remaining_minutes = minutes % 60
    return f"{hours}h {remaining_minutes}m"


def setup_logger(log_file: Path = None, level: str = "INFO"):
    """
    Configure loguru logger

    Args:
        log_file: Optional log file path
        level: Log level (DEBUG, INFO, WARNING, ERROR)
    """
    # Remove default handler
    logger.remove()

    # Console handler with colors
    logger.add(
        sink=lambda msg: print(msg, end=""),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>\n",
        level=level,
        colorize=True,
    )

    # File handler if specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            sink=str(log_file),
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
            level=level,
            rotation="100 MB",
            retention="30 days",
        )

    return logger


def parse_date_arg(date_str: str) -> Tuple[int, int]:
    """
    Parse date string in 'YYYY-MM' format

    Args:
        date_str: Date string

    Returns:
        (year, month) tuple
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m")
        return dt.year, dt.month
    except ValueError as e:
        raise ValueError(f"Invalid date format '{date_str}'. Expected 'YYYY-MM'") from e

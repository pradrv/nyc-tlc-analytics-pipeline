"""Configuration management for NYC Taxi Pipeline"""

import os
from pathlib import Path
from typing import Any, Dict, List

import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent


class Config:
    """Pipeline configuration"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = PROJECT_ROOT / "config" / "pipeline_config.yaml"

        with open(config_path, "r") as f:
            self._config = yaml.safe_load(f)

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by dot-notation key"""
        keys = key.split(".")
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default

    @property
    def database_path(self) -> Path:
        """Get database file path"""
        db_path = os.getenv("DUCKDB_PATH", self.get("database.path"))
        return PROJECT_ROOT / db_path

    @property
    def raw_data_dir(self) -> Path:
        """Get raw data directory"""
        return PROJECT_ROOT / self.get("directories.raw_data", "data/raw")

    @property
    def log_dir(self) -> Path:
        """Get log directory"""
        return PROJECT_ROOT / self.get("directories.logs", "data/logs")

    @property
    def sql_dir(self) -> Path:
        """Get SQL directory"""
        return PROJECT_ROOT / self.get("directories.sql", "sql")

    @property
    def data_sources(self) -> Dict[str, Any]:
        """Get data source configuration"""
        return self.get("data_sources", {})

    @property
    def services(self) -> List[str]:
        """Get list of service types"""
        return list(self.get("data_sources.services", {}).keys())

    @property
    def quality_checks(self) -> Dict[str, Any]:
        """Get quality check thresholds"""
        return self.get("quality_checks", {})

    @property
    def ingestion_config(self) -> Dict[str, Any]:
        """Get ingestion configuration"""
        return self.get("ingestion", {})

    def get_service_config(self, service_type: str) -> Dict[str, Any]:
        """Get configuration for specific service type"""
        return self.get(f"data_sources.services.{service_type}", {})

    def get_file_url(self, service_type: str, year: int, month: int) -> str:
        """Generate download URL for specific service/year/month"""
        service_config = self.get_service_config(service_type)
        base_url = self.get("data_sources.base_url")
        filename_pattern = service_config.get("filename_pattern", "")

        filename = filename_pattern.format(year=year, month=month)
        return f"{base_url}/{filename}"

    def get_file_path(self, service_type: str, year: int, month: int) -> Path:
        """Get local file path for downloaded data"""
        service_config = self.get_service_config(service_type)
        filename_pattern = service_config.get("filename_pattern", "")
        filename = filename_pattern.format(year=year, month=month)

        return self.raw_data_dir / filename


# Global config instance
config = Config()

"""Async downloader for NYC TLC trip data"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import httpx
from loguru import logger

from src.config import config
from src.utils import calculate_file_checksum, format_bytes, format_duration


class TripDataDownloader:
    """Asynchronous downloader for NYC TLC trip data files"""

    def __init__(
        self,
        max_concurrent: int = None,
        retry_attempts: int = None,
        timeout: int = None,
    ):
        """
        Initialize downloader

        Args:
            max_concurrent: Maximum concurrent downloads
            retry_attempts: Number of retry attempts
            timeout: Download timeout in seconds
        """
        self.max_concurrent = max_concurrent or config.ingestion_config.get(
            "max_concurrent_downloads", 10
        )
        self.retry_attempts = retry_attempts or config.ingestion_config.get(
            "retry_attempts", 3
        )
        self.timeout = timeout or config.ingestion_config.get("download_timeout", 300)

        # Ensure raw data directory exists
        config.raw_data_dir.mkdir(parents=True, exist_ok=True)

    async def download_file(
        self, url: str, dest_path: Path, skip_if_exists: bool = True
    ) -> Dict:
        """
        Download single file with retry logic

        Args:
            url: URL to download from
            dest_path: Destination file path
            skip_if_exists: Skip download if file already exists

        Returns:
            Dict with download metadata
        """
        # Check if file already exists
        if skip_if_exists and dest_path.exists():
            file_size = dest_path.stat().st_size
            checksum = calculate_file_checksum(dest_path)
            logger.info(
                f"  Skipping {dest_path.name} (already exists, {format_bytes(file_size)})"
            )
            return {
                "url": url,
                "file_path": dest_path,
                "file_size": file_size,
                "checksum": checksum,
                "status": "skipped",
                "download_time": 0,
                "error": None,
            }

        # Attempt download with retries
        for attempt in range(1, self.retry_attempts + 1):
            try:
                start_time = datetime.now()

                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    logger.info(
                        f" Downloading {dest_path.name} (attempt {attempt}/{self.retry_attempts})"
                    )

                    response = await client.get(url, follow_redirects=True)
                    response.raise_for_status()

                    # Write to file
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(dest_path, "wb") as f:
                        f.write(response.content)

                    # Calculate metrics
                    download_time = (datetime.now() - start_time).total_seconds()
                    file_size = dest_path.stat().st_size
                    checksum = calculate_file_checksum(dest_path)

                    logger.success(
                        f" Downloaded {dest_path.name} - "
                        f"{format_bytes(file_size)} in {format_duration(download_time)}"
                    )

                    return {
                        "url": url,
                        "file_path": dest_path,
                        "file_size": file_size,
                        "checksum": checksum,
                        "status": "success",
                        "download_time": download_time,
                        "error": None,
                    }

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning(f" File not found: {url}")
                    return {
                        "url": url,
                        "file_path": dest_path,
                        "file_size": 0,
                        "checksum": None,
                        "status": "not_found",
                        "download_time": 0,
                        "error": "HTTP 404: File not found",
                    }
                else:
                    logger.error(f" HTTP error {e.response.status_code}: {url}")
                    if attempt < self.retry_attempts:
                        await asyncio.sleep(2**attempt)  # Exponential backoff
                    else:
                        return {
                            "url": url,
                            "file_path": dest_path,
                            "file_size": 0,
                            "checksum": None,
                            "status": "failed",
                            "download_time": 0,
                            "error": f"HTTP {e.response.status_code}: {str(e)}",
                        }

            except (httpx.TimeoutException, httpx.RequestError) as e:
                logger.error(f" Network error: {str(e)}")
                if attempt < self.retry_attempts:
                    logger.info(f"⏳ Retrying in {2**attempt} seconds...")
                    await asyncio.sleep(2**attempt)
                else:
                    return {
                        "url": url,
                        "file_path": dest_path,
                        "file_size": 0,
                        "checksum": None,
                        "status": "failed",
                        "download_time": 0,
                        "error": f"Network error: {str(e)}",
                    }

            except Exception as e:
                logger.error(f" Unexpected error: {str(e)}")
                return {
                    "url": url,
                    "file_path": dest_path,
                    "file_size": 0,
                    "checksum": None,
                    "status": "failed",
                    "download_time": 0,
                    "error": f"Unexpected error: {str(e)}",
                }

    async def download_month(
        self, service_type: str, year: int, month: int, skip_if_exists: bool = True
    ) -> Dict:
        """
        Download data for specific service/year/month

        Args:
            service_type: Service type (yellow, green, hvfhv)
            year: Year
            month: Month (1-12)
            skip_if_exists: Skip if file already exists

        Returns:
            Download metadata dict
        """
        url = config.get_file_url(service_type, year, month)
        dest_path = config.get_file_path(service_type, year, month)

        result = await self.download_file(url, dest_path, skip_if_exists)
        result["service_type"] = service_type
        result["year"] = year
        result["month"] = month

        return result

    async def download_taxi_zones(self, skip_if_exists: bool = True) -> Dict:
        """
        Download taxi zone lookup CSV

        Args:
            skip_if_exists: Skip if file already exists

        Returns:
            Download metadata dict
        """
        url = config.data_sources.get("taxi_zones", {}).get("url")
        if not url:
            raise ValueError("Taxi zones URL not configured")

        dest_path = config.raw_data_dir / "taxi_zone_lookup.csv"

        result = await self.download_file(url, dest_path, skip_if_exists)
        result["service_type"] = "taxi_zones"

        return result

    async def download_date_range(
        self,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        services: List[str] = None,
        skip_if_exists: bool = True,
    ) -> List[Dict]:
        """
        Download data for date range across multiple services

        Args:
            start_year: Start year
            start_month: Start month
            end_year: End year
            end_month: End month
            services: List of service types (default: all)
            skip_if_exists: Skip if file already exists

        Returns:
            List of download metadata dicts
        """
        if services is None:
            services = config.services

        # Generate list of downloads
        tasks = []

        # Add taxi zones
        tasks.append(self.download_taxi_zones(skip_if_exists))

        # Generate month range
        from src.utils import generate_month_range

        months = generate_month_range(
            f"{start_year}-{start_month:02d}", f"{end_year}-{end_month:02d}"
        )

        # Create download tasks
        for service in services:
            for year, month in months:
                tasks.append(self.download_month(service, year, month, skip_if_exists))

        logger.info(
            f" Queued {len(tasks)} downloads ({len(services)} services × {len(months)} months + zones)"
        )

        # Execute with concurrency limit
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def bounded_download(task):
            async with semaphore:
                return await task

        # Run all downloads
        start_time = datetime.now()
        results = await asyncio.gather(*[bounded_download(task) for task in tasks])
        total_time = (datetime.now() - start_time).total_seconds()

        # Calculate summary statistics
        successful = sum(1 for r in results if r["status"] == "success")
        skipped = sum(1 for r in results if r["status"] == "skipped")
        failed = sum(1 for r in results if r["status"] == "failed")
        not_found = sum(1 for r in results if r["status"] == "not_found")
        total_bytes = sum(r["file_size"] for r in results)

        logger.info("=" * 70)
        logger.info(" Download Summary:")
        logger.info(f"   Total: {len(results)} files")
        logger.info(f"    Downloaded: {successful}")
        logger.info(f"     Skipped: {skipped}")
        logger.info(f"    Failed: {failed}")
        logger.info(f"    Not found: {not_found}")
        logger.info(f"    Total size: {format_bytes(total_bytes)}")
        logger.info(f"   ⏱  Total time: {format_duration(total_time)}")
        logger.info("=" * 70)

        return results


async def download_sample_months(skip_if_exists: bool = True) -> List[Dict]:
    """
    Download sample months for testing (from config)

    Args:
        skip_if_exists: Skip if files already exist

    Returns:
        List of download metadata dicts
    """
    downloader = TripDataDownloader()
    sample_months = config.get(
        "date_ranges.sample_months", ["2024-01", "2024-06", "2024-12"]
    )

    logger.info(f" Downloading sample months: {sample_months}")

    tasks = [downloader.download_taxi_zones(skip_if_exists)]

    for month_str in sample_months:
        year, month = month_str.split("-")
        year = int(year)
        month = int(month)

        for service in config.services:
            tasks.append(
                downloader.download_month(service, year, month, skip_if_exists)
            )

    results = await asyncio.gather(*tasks)
    return results

"""Prefect flows for orchestrating the data pipeline"""

from typing import Dict, List

from loguru import logger
from prefect import flow, task

from src.config import config
from src.database.loader import DataLoader
from src.database.schema import SchemaManager
from src.ingestion.downloader import TripDataDownloader
from src.transformations.aggregations import AggregationBuilder
from src.transformations.quality_checks import DataQualityChecker
from src.transformations.standardize import DataTransformer


@task(name="initialize-database", retries=2)
def initialize_database_task():
    """Initialize database schema"""
    logger.info(" Initializing database schema...")
    SchemaManager.initialize_database()
    logger.success(" Database schema initialized")
    return True


@task(name="download-data", retries=3)
async def download_data_task(service_type: str, year_month: str) -> Dict:
    """Download data for a specific service and month"""
    logger.info(f" Downloading {service_type} data for {year_month}...")

    downloader = TripDataDownloader()

    # Parse year and month
    year, month = year_month.split("-")
    year = int(year)
    month = int(month)

    # Download single file
    results = await downloader.download_date_range(
        year, month, year, month, [service_type], skip_if_exists=True
    )

    result = (
        results[0] if results else {"status": "failed", "error_message": "No results"}
    )

    if result["status"] == "success":
        logger.success(
            f" Downloaded {service_type} {year_month}: {result.get('file_size_mb', 0)} MB"
        )
    else:
        logger.error(
            f" Failed to download {service_type} {year_month}: {result.get('error_message')}"
        )

    return result


@task(name="load-data", retries=2)
def load_data_task(service_type: str = "all") -> int:
    """Load downloaded data into raw tables"""
    logger.info(f" Loading {service_type} data to database...")

    # Load all downloaded files and get total row count
    DataLoader.load_all_downloaded_files()

    # Get total row count from database
    from src.database.connection import DatabaseConnection

    conn = DatabaseConnection.get_connection()

    total_rows = 0
    for table in ["raw_yellow", "raw_green", "raw_hvfhv"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        total_rows += count

    logger.success(f" Loaded data successfully, total rows in database: {total_rows:,}")
    return total_rows


@task(name="quality-check", retries=1)
def quality_check_task(table_name: str) -> List[Dict]:
    """Run quality checks on a table"""
    logger.info(f" Running quality checks on {table_name}...")

    checker = DataQualityChecker()
    results = checker.run_all_checks(table_name)

    logger.success(f" Quality checks complete for {table_name}")
    return results


@task(name="transform-to-fact", retries=2)
def transform_to_fact_task() -> Dict:
    """Transform raw data to fact_trips"""
    logger.info(" Transforming raw data to fact_trips...")

    result = DataTransformer.transform_all()

    logger.success(f" Transformed {result['total']:,} trips to fact_trips")
    return result


@task(name="build-aggregates", retries=2)
def build_aggregates_task() -> Dict:
    """Build aggregate tables"""
    logger.info(" Building aggregate tables...")

    result = AggregationBuilder.build_all()

    logger.success(f" Built {result['total']:,} aggregate rows")
    return result


@flow(name="ingestion-flow", log_prints=True)
async def ingestion_flow(service_types: List[str], year_months: List[str]) -> Dict:
    """
    Flow for downloading and loading data

    Args:
        service_types: List of service types (yellow, green, hvfhv)
        year_months: List of year-month strings (YYYY-MM)

    Returns:
        Summary of ingestion results
    """
    logger.info(" Starting ingestion flow...")

    # Download all files
    downloader = TripDataDownloader()

    # Parse date range
    if year_months:
        start_year, start_month = map(int, year_months[0].split("-"))
        end_year, end_month = map(int, year_months[-1].split("-"))
    else:
        start_year, start_month = 2024, 1
        end_year, end_month = 2024, 12

    download_results = await downloader.download_date_range(
        start_year, start_month, end_year, end_month, service_types, skip_if_exists=True
    )

    # Load all downloaded files
    total_rows = load_data_task.submit("all").result()

    summary = {
        "downloads": len(download_results),
        "successful_downloads": sum(
            1 for r in download_results if r["status"] in ["success", "skipped"]
        ),
        "failed_downloads": sum(1 for r in download_results if r["status"] == "failed"),
        "total_rows_loaded": total_rows,
    }

    logger.success(
        f" Ingestion flow complete: {summary['total_rows_loaded']:,} rows loaded"
    )
    return summary


@flow(name="quality-check-flow", log_prints=True)
def quality_check_flow() -> Dict:
    """
    Flow for running quality checks on all raw tables

    Returns:
        Summary of quality check results
    """
    logger.info(" Starting quality check flow...")

    tables = ["raw_yellow", "raw_green", "raw_hvfhv"]

    all_results = []
    for table in tables:
        results = quality_check_task(table)
        all_results.extend(results)

    total_rows = sum(r["total_rows"] for r in all_results)
    passed_rows = sum(r["passed_rows"] for r in all_results)

    summary = {
        "tables_checked": len(tables),
        "total_checks": len(all_results),
        "total_rows": total_rows,
        "passed_rows": passed_rows,
        "data_quality_pct": round(passed_rows / total_rows * 100, 2)
        if total_rows > 0
        else 0,
    }

    logger.success(
        f" Quality check flow complete: {summary['data_quality_pct']}% data quality"
    )
    return summary


@flow(name="transformation-flow", log_prints=True)
def transformation_flow() -> Dict:
    """
    Flow for transforming raw data to fact and aggregate tables

    Returns:
        Summary of transformation results
    """
    logger.info(" Starting transformation flow...")

    # Transform to fact table
    fact_result = transform_to_fact_task()

    # Build aggregates
    agg_result = build_aggregates_task()

    summary = {
        "fact_trips": fact_result["total"],
        "yellow_trips": fact_result["yellow"],
        "green_trips": fact_result["green"],
        "hvfhv_trips": fact_result["hvfhv"],
        "aggregate_rows": agg_result["total"],
    }

    logger.success(
        f" Transformation flow complete: {summary['fact_trips']:,} fact trips, {summary['aggregate_rows']:,} aggregate rows"
    )
    return summary


@flow(name="full-pipeline", log_prints=True)
async def full_pipeline_flow(
    service_types: List[str] = None,
    year_months: List[str] = None,
    skip_download: bool = False,
) -> Dict:
    """
    Complete end-to-end pipeline flow

    Args:
        service_types: List of service types (default: from config)
        year_months: List of year-month strings (default: from config)
        skip_download: Skip download step if data already exists

    Returns:
        Complete pipeline summary
    """
    logger.info(" Starting full E2E pipeline...")

    # Use config defaults if not provided
    if service_types is None:
        service_types = list(config.data_sources.keys())

    if year_months is None:
        from src.utils import generate_month_range

        year_months = generate_month_range(
            config.date_range["start_date"], config.date_range["end_date"]
        )

    # Step 1: Initialize database
    initialize_database_task()

    # Step 2: Ingestion (download + load)
    if not skip_download:
        ingestion_summary = await ingestion_flow(service_types, year_months)
    else:
        logger.info("  Skipping download, loading existing files...")
        total_rows = load_data_task("all")
        ingestion_summary = {"total_rows_loaded": total_rows}

    # Step 3: Quality checks
    quality_summary = quality_check_flow()

    # Step 4: Transformations
    transformation_summary = transformation_flow()

    # Final summary
    summary = {
        "pipeline_status": "completed",
        "ingestion": ingestion_summary,
        "quality": quality_summary,
        "transformation": transformation_summary,
    }

    logger.success("=" * 70)
    logger.success(" FULL E2E PIPELINE COMPLETED SUCCESSFULLY!")
    logger.success("=" * 70)
    logger.success(
        f"Total rows loaded: {ingestion_summary.get('total_rows_loaded', 0):,}"
    )
    logger.success(f"Data quality: {quality_summary['data_quality_pct']}%")
    logger.success(f"Fact trips: {transformation_summary['fact_trips']:,}")
    logger.success(f"Aggregate rows: {transformation_summary['aggregate_rows']:,}")
    logger.success("=" * 70)

    return summary

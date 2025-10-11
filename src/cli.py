"""Command-line interface for NYC Taxi Pipeline"""

import asyncio
from pathlib import Path

import click
from loguru import logger

from src.config import config
from src.database.connection import DatabaseConnection
from src.database.loader import DataLoader
from src.database.schema import SchemaManager
from src.ingestion.downloader import TripDataDownloader, download_sample_months
from src.orchestration.flows import full_pipeline_flow
from src.transformations.aggregations import AggregationBuilder
from src.transformations.quality_checks import DataQualityChecker
from src.transformations.standardize import DataTransformer
from src.utils import generate_month_range, setup_logger


@click.group()
def cli():
    """NYC Taxi & HVFHV Data Pipeline"""
    # Setup logging
    log_file = config.log_dir / f"pipeline_{Path(__file__).stem}.log"
    setup_logger(log_file, level="INFO")


@cli.command()
def init_db():
    """Initialize database schema"""
    logger.info("  Initializing database...")

    try:
        SchemaManager.initialize_database()
        SchemaManager.verify_schema()
        SchemaManager.get_schema_summary()
        logger.success(" Database initialized successfully")
    except Exception as e:
        logger.error(f" Database initialization failed: {e}")
        raise


@cli.command()
@click.option("--start-date", default="2021-01", help="Start date (YYYY-MM)")
@click.option("--end-date", default="2025-01", help="End date (YYYY-MM)")
@click.option(
    "--services", default="yellow,green,hvfhv", help="Comma-separated service types"
)
@click.option(
    "--skip-existing/--no-skip-existing", default=True, help="Skip existing files"
)
def download(start_date, end_date, services, skip_existing):
    """Download trip data files"""
    logger.info(f" Downloading data: {start_date} to {end_date}")

    service_list = services.split(",")

    # Parse dates
    start_year, start_month = map(int, start_date.split("-"))
    end_year, end_month = map(int, end_date.split("-"))

    # Download
    downloader = TripDataDownloader()

    async def run_download():
        return await downloader.download_date_range(
            start_year, start_month, end_year, end_month, service_list, skip_existing
        )

    results = asyncio.run(run_download())

    # Log results
    successful = sum(1 for r in results if r["status"] in ["success", "skipped"])
    logger.info(f" Download complete: {successful}/{len(results)} files")


@cli.command()
def download_sample():
    """Download sample months for testing"""
    logger.info(" Downloading sample months...")

    results = asyncio.run(download_sample_months())

    successful = sum(1 for r in results if r["status"] in ["success", "skipped"])
    logger.info(f" Sample download complete: {successful}/{len(results)} files")


@cli.command()
@click.option(
    "--services", default="yellow,green,hvfhv", help="Comma-separated service types"
)
def load(services):
    """Load downloaded files to database"""
    logger.info(" Loading data to database...")

    service_list = services.split(",")
    results = DataLoader.load_all_downloaded_files(service_list)

    successful = sum(1 for r in results if r["status"] == "success")
    logger.info(f" Load complete: {successful}/{len(results)} files")


@cli.command()
def load_zones():
    """Load taxi zone lookup data"""
    logger.info("📍 Loading taxi zones...")
    SchemaManager.load_taxi_zones()


@cli.command()
def db_stats():
    """Show database statistics"""
    SchemaManager.get_schema_summary()


@cli.command()
@click.option("--sample/--full", default=True, help="Run sample or full pipeline")
def run_pipeline(sample):
    """Run full ETL pipeline (ingestion only - use run-e2e for full pipeline)"""
    logger.info(" Starting NYC Taxi Pipeline (Ingestion)...")

    try:
        # Step 1: Initialize database
        logger.info("Step 1: Initialize database")
        SchemaManager.initialize_database()

        # Step 2: Download data
        logger.info("Step 2: Download data")
        if sample:
            asyncio.run(download_sample_months())
        else:
            downloader = TripDataDownloader()
            asyncio.run(
                downloader.download_date_range(
                    2021, 1, 2025, 1, config.services, skip_if_exists=True
                )
            )

        # Step 3: Load taxi zones
        logger.info("Step 3: Load taxi zones")
        SchemaManager.load_taxi_zones()

        # Step 4: Load trip data
        logger.info("Step 4: Load trip data to raw tables")
        DataLoader.load_all_downloaded_files()

        # Step 5: Show summary
        logger.info("Step 5: Database summary")
        SchemaManager.get_schema_summary()

        logger.success(" Ingestion pipeline completed successfully!")
        logger.info(
            " Run 'uv run python -m src.cli run-e2e' for full E2E pipeline with transformations"
        )

    except Exception as e:
        logger.error(f" Pipeline failed: {e}")
        raise


@cli.command()
@click.option(
    "--tables",
    default="raw_yellow,raw_green,raw_hvfhv",
    help="Comma-separated table names",
)
def quality_check(tables):
    """Run data quality checks on raw tables"""
    logger.info(" Running data quality checks...")

    table_list = tables.split(",")
    checker = DataQualityChecker()

    for table in table_list:
        checker.run_all_checks(table.strip())

    logger.success(" Quality checks completed!")


@cli.command()
def transform():
    """Transform raw data to fact_trips table"""
    logger.info(" Starting data transformation...")

    try:
        result = DataTransformer.transform_all()

        logger.success("=" * 70)
        logger.success(" Transformation completed!")
        logger.success(f"Yellow trips: {result['yellow']:,}")
        logger.success(f"Green trips: {result['green']:,}")
        logger.success(f"HVFHV trips: {result['hvfhv']:,}")
        logger.success(f"Total trips: {result['total']:,}")
        logger.success("=" * 70)

    except Exception as e:
        logger.error(f" Transformation failed: {e}")
        raise


@cli.command()
def build_aggregates():
    """Build aggregate tables for analytics"""
    logger.info(" Building aggregate tables...")

    try:
        result = AggregationBuilder.build_all()

        logger.success("=" * 70)
        logger.success(" Aggregates built!")
        logger.success(f"Pricing by zone/hour: {result['pricing']:,} rows")
        logger.success(f"HVFHV take rates: {result['take_rates']:,} rows")
        logger.success(f"Market share: {result['market_share']:,} rows")
        logger.success(f"Daily summary: {result['daily_summary']:,} rows")
        logger.success(f"Total: {result['total']:,} rows")
        logger.success("=" * 70)

    except Exception as e:
        logger.error(f" Aggregate build failed: {e}")
        raise


@cli.command()
@click.option("--sample/--full", default=True, help="Run sample or full pipeline")
@click.option(
    "--skip-download", is_flag=True, help="Skip download if data already exists"
)
def run_e2e(sample, skip_download):
    """Run complete E2E pipeline with Prefect orchestration"""
    logger.info(" Starting FULL E2E Pipeline with Prefect...")

    try:
        # Determine service types and date range
        service_types = ["yellow", "green", "hvfhv"]

        if sample:
            # Sample: 1 month only (to minimize disk space usage)
            year_months = ["2024-06"]
        else:
            # Full: all months from config
            year_months = generate_month_range(
                config.date_range["start_date"], config.date_range["end_date"]
            )

        logger.info(f"Service types: {service_types}")
        logger.info(f"Date range: {len(year_months)} months")

        # Run the full pipeline flow (async)
        summary = asyncio.run(
            full_pipeline_flow(
                service_types=service_types,
                year_months=year_months,
                skip_download=skip_download,
            )
        )

        logger.success("=" * 70)
        logger.success(" FULL E2E PIPELINE COMPLETED!")
        logger.success("=" * 70)
        logger.success(f"Status: {summary['pipeline_status']}")
        logger.success(
            f"Rows loaded: {summary['ingestion'].get('total_rows_loaded', 0):,}"
        )
        logger.success(f"Data quality: {summary['quality']['data_quality_pct']}%")
        logger.success(f"Fact trips: {summary['transformation']['fact_trips']:,}")
        logger.success(
            f"Aggregate rows: {summary['transformation']['aggregate_rows']:,}"
        )
        logger.success("=" * 70)
        logger.info(" Query analytics: ls sql/analytics/*.sql")
        logger.info(" Database stats: python -m src.cli db-stats")

    except Exception as e:
        logger.error(f" E2E Pipeline failed: {e}")
        raise


@cli.command()
@click.argument("query_file", type=click.Path(exists=True))
def run_analytics(query_file):
    """Run an analytics SQL query"""
    logger.info(f" Running analytics query: {query_file}")

    try:
        with open(query_file, "r") as f:
            sql = f.read()

        conn = DatabaseConnection.get_connection()
        result = conn.execute(sql).fetchdf()

        logger.info(f"\n{result.to_string()}\n")
        logger.success(f" Query returned {len(result)} rows")

    except Exception as e:
        logger.error(f" Query failed: {e}")
        raise


if __name__ == "__main__":
    cli()

# Contributing to NYC Taxi & HVFHV Data Pipeline

Thank you for your interest in contributing to this project!

## Development Setup

### Prerequisites
- Python 3.11+
- `uv` package manager
- Git

### Initial Setup

```bash
# Clone the repository
git clone <repository-url>
cd data_pipeline

# Install dependencies
./scripts/setup.sh

# Initialize database
uv run python -m src.cli init-db

# Run sample pipeline (1 month)
uv run python -m src.cli run-e2e --sample
```

## Project Structure

```
data_pipeline/
├── src/                    # Source code
│   ├── ingestion/         # Data download & validation
│   ├── database/          # DuckDB connection & schema
│   ├── transformations/   # Data quality & transformations
│   ├── orchestration/     # Prefect flows
│   └── cli.py            # Command-line interface
├── sql/                   # SQL definitions
│   ├── ddl/              # Table schemas
│   └── analytics/        # Analytics queries
├── config/               # Configuration files
├── scripts/              # Shell scripts
└── tests/               # Unit tests (future)
```

## Making Changes

### Adding New Analytics Queries

1. Create SQL file in `sql/analytics/`
2. Follow naming convention: `##_descriptive_name.sql`
3. Add comments explaining purpose and business use case
4. Test the query:
   ```bash
   uv run python -m src.cli run-analytics sql/analytics/your_query.sql
   ```

### Modifying Transformations

1. Edit files in `src/transformations/`
2. Ensure idempotency (can run multiple times safely)
3. Update data quality checks if needed
4. Test with sample data first

### Adding New Data Sources

1. Update `config/pipeline_config.yaml`
2. Add DDL in `sql/ddl/01_raw_tables.sql`
3. Create loader in `src/database/loader.py`
4. Add transformation in `src/transformations/standardize.py`

## Testing

### Run Sample Pipeline
```bash
uv run python -m src.cli run-e2e --sample
```

### Test Analytics Queries
```bash
./test_all_analytics.sh
```

### Check Database Stats
```bash
uv run python -m src.cli db-stats
```

## Code Style

- Follow PEP 8 for Python code
- Use type hints where applicable
- Add docstrings for functions and classes
- Keep SQL formatted and readable
- Use meaningful variable names

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Test thoroughly
5. Commit with clear messages
6. Push to your fork
7. Open a pull request with description

## Questions or Issues?

Open an issue on GitHub with:
- Clear description of the problem/question
- Steps to reproduce (if bug)
- Expected vs actual behavior
- Environment details (OS, Python version)

## License

By contributing, you agree that your contributions will be licensed under the MIT License.


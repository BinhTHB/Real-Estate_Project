# AI Coding Assistant Instructions for Practical Data Engineering Project

## Project Overview
This is a real estate data engineering pipeline that scrapes property listings from Vietnamese real estate websites (nhadat247.com.vn), processes data using Delta Lake, and enables exploration through Jupyter notebooks. The project demonstrates modern data engineering practices with Dagster orchestration, MinIO storage, and containerized deployment.

## Architecture & Data Flow
- **Scraping Layer**: SeleniumBase-based threaded web scraping with configurable user profiles and headless mode
- **Processing Layer**: Pandas DataFrames with Delta Lake ACID transactions and UPSERT operations
- **Storage Layer**: MinIO S3-compatible object storage with Delta tables in bronze layer
- **Orchestration**: Dagster pipelines with dynamic outputs for parallel processing
- **Exploration**: Jupyter notebooks for data analysis and Apache Superset for visualization

**Key Data Flow**: Search Criteria → URL Generation → Threaded Selenium Scraping → DataFrame Processing → Delta Lake Merge → Jupyter Exploration

## Critical Developer Workflows

### Local Development Setup
```bash
# Install dependencies (from src/pipelines/real-estate/)
pip install -e ".[dev]"

# Start MinIO storage
minio server /tmp/minio/

# Start Dagster development server
dagster dev
# Alternative: make up
```

### Testing
```bash
# Run all tests
tox

# Run specific test file
pytest realestate_tests/pipeline_tests.py
```

### Pipeline Execution
- Access Dagster UI at http://localhost:3000
- Launch `scrape_realestate` job with search criteria from `config_pipelines/scrape_realestate.yaml`
- Monitor pipeline runs and view execution graphs

## Project-Specific Patterns & Conventions

### Dagster Pipeline Structure
- Use `@op` for individual operations, `@graph` for composable sub-pipelines
- Dynamic outputs (`DynamicOut`) for parallel processing of multiple search criteria
- Resource injection via `required_resource_keys` (e.g., `{"pyspark", "s3"}`)
- Configuration merging from `config_environments/` and `config_pipelines/`

### Selenium Scraping Patterns
```python
# Threaded scraping with configurable limits
df = threaded_selenium_scrapping(
    nthreads=1,           # Single thread to avoid detection
    urls=urls,
    limit_each_page=5,    # Rate limiting
    user_profile_dir=None, # Use real Chrome profile for anti-detection
    headless=False        # Visible browser for debugging
)
```

### Delta Lake Operations
- Use `merge_property_delta()` for UPSERT operations with schema evolution
- Store in bronze layer: `s3a://real-estate/lake/bronze/property`
- Enable auto-merge: `spark.databricks.delta.schema.autoMerge.enabled: true`

### Configuration Management
- **Environment configs**: `config_environments/local_base.yaml` (MinIO, Spark settings)
- **Pipeline configs**: `config_pipelines/scrape_realestate.yaml` (search criteria, credentials)
- **Resource configs**: boto3 credentials, S3 endpoints, PySpark configurations

### Data Types & Validation
- `SearchCoordinate`: Dict with city, propertyType, rentOrBuy, radius
- `PropertyDataFrame`: Pandas DataFrame with property listings
- Custom Dagster types with JSON validation

### File Organization
```
realestate/
├── pipelines.py          # Main job definitions
├── resources.py          # Database/S3 resource configurations
├── common/
│   ├── selenium_scrapping.py    # Web scraping logic
│   ├── solids_spark_delta.py    # Delta Lake operations
│   ├── types_realestate.py      # Custom data types
│   └── helper_functions.py      # Utility functions
├── config_environments/  # Environment-specific settings
├── config_pipelines/     # Pipeline execution parameters
└── notebooks/           # Data exploration notebooks
```

## Integration Points & Dependencies

### External Services
- **MinIO**: S3-compatible storage (localhost:9000, credentials: minioadmin/minioadmin)
- **Dagster UI**: Development interface (localhost:3000)
- **Jupyter**: Data exploration (launched via dagstermill)
- **Apache Superset**: Dashboard visualization (separate deployment)

### Key Dependencies
- `dagster-deltalake-pandas`: Delta Lake integration
- `seleniumbase`: Anti-detection web scraping
- `pandas`: Data manipulation
- `pyarrow`: Parquet/Delta format support
- `boto3`: AWS S3 API (MinIO compatible)

### Cross-Component Communication
- Pipeline outputs passed as DataFrames between ops
- Resource injection for database connections and S3 clients
- YAML configuration files merged at runtime
- Dynamic mapping keys for parallel search criteria processing

## Common Development Tasks

### Adding New Scraping Targets
1. Update `generate_urls_from_criteria()` in `pipelines.py`
2. Add URL extraction logic in `selenium_scrapping.py`
3. Update `SearchCoordinate` type if new parameters needed
4. Test with single URL before enabling parallel processing

### Modifying Data Processing
1. Update Delta merge logic in `solids_spark_delta.py`
2. Ensure schema evolution handles new columns
3. Update downstream Jupyter notebooks
4. Test UPSERT operations with sample data

### Configuration Changes
1. Environment settings: `config_environments/local_base.yaml`
2. Pipeline parameters: `config_pipelines/scrape_realestate.yaml`
3. Restart Dagster dev server after config changes

### Debugging Scraping Issues
- Set `HEADLESS = False` in pipeline execution
- Use real Chrome profile: `USER_PROFILE_DIR = r"E:\Profile 2"`
- Check browser console for JavaScript errors
- Verify CSS selectors in `extract_property_urls_single_page()`

### Selenium Troubleshooting
- **DNS Resolution Error**: Use `requests_scraping.py` instead of Selenium to bypass Chrome DNS issues
- **API Compatibility**: Code provides multiple fallbacks: SeleniumBase → undetected-chromedriver → regular selenium → requests
- **Cloudflare Blocking**: Use requests with realistic headers for better success rate
- **Chrome Version Issues**: Chrome 142+ has DNS issues, use `requests_scraping.py` as final fallback
- **Driver Creation Failed**: Run `python debug_selenium_api.py`, fallback to `requests_scraping.py`

## Quality Assurance
- Run `tox` for automated testing
- Validate DataFrame schemas before Delta Lake writes
- Test with small datasets before full pipeline runs
- Monitor MinIO bucket for data persistence</content>
<parameter name="filePath">.github/copilot-instructions.md

before running any file, activate the virtual environment in `src/pipelines/real-estate/.venv`

all project code is under `src/pipelines/real-estate/`
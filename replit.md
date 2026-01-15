# Cryptocurrency & Equity Data Pipeline

## Overview
A Python data pipeline system for recurring API pulls of cryptocurrency and equity data with PostgreSQL storage. The system is designed to be modular and extensible for adding multiple data sources.

## Project Structure
```
.
├── main.py                  # CLI entry point
├── query_data.py           # Query helper functions with CSV export
├── artemis_pull_config.csv # Configuration for Artemis API pulls (matrix format)
├── db/
│   ├── __init__.py
│   └── setup.py            # Database connection and schema setup
└── sources/
    ├── __init__.py         # Source registry
    ├── base.py             # BaseSource abstract class
    └── artemis.py          # Artemis API source implementation
```

## Database Schema
- **pulls**: Logs each API pull (pull_id, source_name, pulled_at, status, records_count)
- **metrics**: Time series data (id, pulled_at, source, asset, metric_name, value)
- Index on (source, asset, metric_name, pulled_at) for fast queries

## Usage

### Setup Database
```bash
python main.py setup
```

### Pull Data
```bash
python main.py pull artemis
```

### Query Data (Interactive)
```bash
python main.py query
```

### List Available Sources
```bash
python main.py sources
```

## Adding New Sources
1. Create a new file in `sources/` (e.g., `sources/newsource.py`)
2. Extend `BaseSource` class and implement:
   - `source_name` property
   - `pull()` method
3. Register in `sources/__init__.py` by adding to the `SOURCES` dict

## Environment Variables
- `DATABASE_URL`: PostgreSQL connection string (auto-configured)
- `ARTEMIS_API_KEY`: API key for Artemis data (required for artemis pulls)

## Artemis Configuration
The `artemis_pull_config.csv` uses a matrix format:
- Rows: Assets (with name, asset ID, category)
- Column `Pull`: Set to 1 to include asset
- Metric columns: Set to 1 to pull that metric for the asset

Friendly metric names are mapped to API IDs automatically (e.g., "Fees" → "FEES").
Symbols are batched in groups of 250 per API request for efficiency.

## Garbage Value Filtering
The following API responses are filtered out automatically:
- "Metric not found."
- "Metric not available for asset."
- "Market data not available for asset."
- "Latest data not available for this asset."

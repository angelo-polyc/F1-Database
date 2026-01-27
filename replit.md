# Cryptocurrency & Equity Data Pipeline

## Overview
This project is a Python-based data pipeline designed for the recurring collection and storage of cryptocurrency and equity data from various APIs into a PostgreSQL database. Its primary purpose is to provide a robust, modular, and extensible system for financial data aggregation and analysis. The system supports multiple data sources, handles historical backfills, and ensures data integrity through sophisticated deduplication and normalization. The ultimate goal is to enable unified cross-source queries for comprehensive market insights.

## User Preferences
- **Do NOT restart workflows automatically** - Only restart when explicitly instructed

## System Architecture
The system is built around a modular architecture, allowing easy integration of new data sources. It features a CLI for database setup and data pulling, a scheduler for automated recurring pulls, and a REST API (FastAPI) for LLM access.

**Core Components:**
- **Data Sources:** Abstract `BaseSource` class with concrete implementations for Artemis, DefiLlama, Velo.xyz, CoinGecko, and Alpha Vantage. Each source handles its specific API interactions, data parsing, and metric extraction.
- **Database:** PostgreSQL used for data storage.
  - **Schema:** `pulls` table logs API pull metadata, and `metrics` table stores time-series data with a unique index for deduplication.
  - **Deduplication:** Utilizes `ON CONFLICT DO NOTHING` for backfills and `ON CONFLICT DO UPDATE` for live pulls to prevent duplicates and handle updates.
- **Entity Master System:** Provides canonical IDs across different data sources through `entities` and `entity_source_ids` tables, enabling unified queries despite varying source ID formats.
- **Metric Normalization:** A `v_normalized_metrics` view maps source-specific metric names to canonical names (e.g., Velo `CLOSE_PRICE` to `PRICE`).
- **Scheduler (`scheduler.py`):** Automates data pulls at specific intervals (hourly for DefiLlama, Velo, CoinGecko, AlphaVantage; daily for Artemis). Includes robust gap-free backfill logic to ensure continuous data streams.
- **Performance Optimizations:**
  - Parallel API requests using `ThreadPoolExecutor`.
  - HTTP connection pooling with `requests.Session` and `HTTPAdapter`.
  - Global token bucket rate limiting for all API calls.
  - Dynamic column batching for Velo API to handle large data volumes efficiently.

**Key Features:**
- **Historical Backfills:** Dedicated scripts for DefiLlama, Artemis, and Velo to ingest historical data.
- **Configuration:** CSV files for each source define assets and parameters for API pulls.
- **CLI:** Provides commands for database setup, manual data pulls, data querying, and listing available sources.

## External Dependencies
- **Artemis API:** Requires `ARTEMIS_API_KEY`.
- **DefiLlama API:** Requires `DEFILLAMA_API_KEY` (for Pro API access).
- **Velo.xyz API:** Requires `VELO_API_KEY`.
- **CoinGecko API:** Used for cryptocurrency market data.
- **Alpha Vantage API:** Used for equity data.
- **PostgreSQL:** The primary database for storing all collected data.
- **FastAPI:** Used to expose a REST API for LLM access.

## REST API (api.py)
The REST API is designed for LLM access (Claude, ChatGPT) to enable AI-driven financial data analysis.

**Base URL:** `http://localhost:8000`

**Endpoints:**
| Endpoint | Description |
|----------|-------------|
| `GET /` | Health check and endpoint list |
| `GET /data-dictionary` | Complete schema documentation for LLMs |
| `GET /sources` | List data sources with record counts |
| `GET /entities` | List entities with filtering (type, sector, source, search) |
| `GET /entities/{canonical_id}` | Get entity details with source mappings |
| `GET /metrics` | List available metrics with counts |
| `GET /latest` | Get latest values for a metric across assets |
| `GET /time-series` | Get historical time series for asset/metric |
| `GET /cross-source` | Compare data across sources for an entity |
| `GET /stats` | Database statistics and recent pull history |

**Example Queries:**
```bash
# Get data dictionary for LLM context
curl http://localhost:8000/data-dictionary

# Get latest Bitcoin prices from all sources
curl "http://localhost:8000/cross-source?canonical_id=bitcoin&metric=PRICE"

# Get 30-day price history for Solana
curl "http://localhost:8000/time-series?asset=solana&metric=PRICE&source=coingecko&days=30"

# Search for layer-1 entities
curl "http://localhost:8000/entities?sector=layer-1&limit=20"
```

## Claude Integration (MCP Server)
The `mcp_server.py` provides Model Context Protocol support for Claude Desktop.

**Available Tools:**
- `get_data_dictionary` - Schema + metrics (call first)
- `search_entities` - Find assets by type/sector/name
- `get_time_series` - Historical data
- `get_latest_values` - Rankings/comparisons
- `compare_cross_source` - Multi-source comparison

**Setup:** See `CLAUDE_INTEGRATION.md` for configuration instructions.
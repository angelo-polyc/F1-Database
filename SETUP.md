# Crypto & Equity Data Pipeline - Setup Guide

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL database
- API keys for data sources

### Environment Variables

Create a `.env` file or set these environment variables:

```bash
# Database (required)
DATABASE_URL=postgresql://user:password@host:5432/dbname

# API Keys (required for respective sources)
ARTEMIS_API_KEY=your_key
DEFILLAMA_API_KEY=your_key
VELO_API_KEY=your_key
COINGECKO_API_KEY=your_key
ALPHAVANTAGE_API_KEY=your_key

# Optional - for API authentication
DATA_API_KEY=your_api_key
ADMIN_API_KEY=your_admin_key
```

### Installation

```bash
# Clone the repository
git clone https://github.com/your-username/your-repo.git
cd your-repo

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Database Setup

```bash
# Initialize the database (creates tables, views, entities)
python main.py setup
```

### Running the Application

#### Option 1: Full server with scheduler (recommended for production)
```bash
python server.py
```
This starts:
- REST API on port 5000
- Background scheduler for automated data pulls
- Smart startup (detects gaps and backfills automatically)

#### Option 2: Manual operations
```bash
# Run a manual pull for a specific source
python main.py pull --source artemis

# Run backfills manually
python backfill_artemis.py --days 365
python backfill_defillama.py --days 365
python backfill_coingecko.py --days 365
python backfill_alphavantage.py --days 365
python backfill_velo.py --days 365

# Query data
python query_data.py
```

### API Endpoints

Once running, access the API at `http://localhost:5000`:

| Endpoint | Description |
|----------|-------------|
| GET `/` | Health check |
| GET `/data-dictionary` | Schema documentation |
| GET `/sources` | List data sources |
| GET `/entities` | List entities |
| GET `/metrics` | Available metrics |
| GET `/latest` | Latest metric values |
| GET `/time-series` | Historical data |
| GET `/cross-source` | Compare across sources |

Admin endpoints require `X-Admin-Key` header:
| Endpoint | Description |
|----------|-------------|
| GET `/admin/tables` | List database tables |
| POST `/admin/query` | Execute SQL queries |
| GET `/admin/source-status` | Check source status |
| POST `/admin/backfill/{source}` | Trigger backfill |

### Docker Deployment (optional)

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000
CMD ["python", "server.py"]
```

### Scheduler Flags

```bash
# Fresh start (clears all data, runs full backfills)
python server.py --fresh

# Skip smart startup (just run scheduler without initial checks)
python server.py --no-startup
```

### Project Structure

```
├── server.py           # Main entry point (API + scheduler)
├── api.py              # REST API endpoints
├── scheduler.py        # Smart scheduler with gap detection
├── main.py             # CLI for manual operations
├── db/
│   └── setup.py        # Database initialization
├── sources/            # Data source implementations
├── backfill_*.py       # Historical backfill scripts
├── *_config.csv        # Asset configuration per source
└── migrations/         # Database migrations
```

### Troubleshooting

**Database connection issues:**
- Verify DATABASE_URL is correct
- Check PostgreSQL is running
- Ensure user has CREATE TABLE permissions

**API rate limits:**
- Each source has built-in rate limiting
- Reduce MAX_WORKERS in backfill scripts if hitting limits
- Check API key quotas

**Missing data:**
- Run `python main.py pull --source <source>` manually
- Check logs for API errors
- Verify API keys are valid

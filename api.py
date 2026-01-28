"""
REST API for Cryptocurrency & Equity Data Pipeline
Designed for LLM access (Claude, ChatGPT) to enable AI-driven financial data analysis.
"""

from fastapi import FastAPI, Query, HTTPException, Request, Depends, Security
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from typing import Optional, List
from datetime import datetime, date
from pydantic import BaseModel
from db.setup import get_connection
import psycopg2
import os

API_KEY = os.environ.get("DATA_API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

VALID_SOURCES = ["artemis", "defillama", "velo", "coingecko", "alphavantage"]

METRIC_TO_PREFERRED_SOURCE = {
    "TVL": "defillama",
    "CHAIN_TVL": "defillama",
    "FEES_24H": "defillama",
    "REVENUE_24H": "defillama",
    "DEX_VOLUME_24H": "defillama",
    "CHAIN_DEX_VOLUME_24H": "defillama",
    "DERIVATIVES_VOLUME_24H": "defillama",
    "STABLECOIN_SUPPLY": "defillama",
    "BRIDGE_VOLUME_24H": "defillama",
    "BRIDGE_VOLUME_7D": "defillama",
    "BRIDGE_VOLUME_30D": "defillama",
    "FUNDING_RATE_AVG": "velo",
    "DOLLAR_OI_CLOSE": "velo",
    "LIQ_DOLLAR_VOL": "velo",
    "CLOSE_PRICE": "velo",
    "DOLLAR_VOLUME": "velo",
    "FEES": "artemis",
    "REVENUE": "artemis",
    "DAU": "artemis",
    "TXNS": "artemis",
    "NEW_USERS": "artemis",
    "AVG_TXN_FEE": "artemis",
    "OPEN": "alphavantage",
    "HIGH": "alphavantage",
    "LOW": "alphavantage",
    "CLOSE": "alphavantage",
    "ADJUSTED_CLOSE": "alphavantage",
    "DIVIDEND_AMOUNT": "alphavantage",
    "SPLIT_COEFFICIENT": "alphavantage",
    "PRICE": "coingecko",
    "MARKET_CAP": "coingecko",
    "VOLUME_24H": "coingecko",
    "PRICE_CHANGE_24H_PCT": "coingecko",
    "ATH": "coingecko",
    "ATL": "coingecko",
}


def get_preferred_source_for_metric(metric: str) -> Optional[str]:
    """Get the preferred source for a metric, or None if ambiguous."""
    return METRIC_TO_PREFERRED_SOURCE.get(metric.upper())


def normalize_source(source: Optional[str]) -> Optional[str]:
    """Normalize source to lowercase."""
    if source is None:
        return None
    return source.lower().strip()


def normalize_metric(metric: str) -> str:
    """Normalize metric to uppercase."""
    return metric.upper().strip()


def get_source_mappings(canonical_id: str, conn) -> dict:
    """Get all source ID mappings for a canonical entity."""
    cur = conn.cursor()
    cur.execute("""
        SELECT esi.source, esi.source_id 
        FROM entities e
        JOIN entity_source_ids esi ON e.entity_id = esi.entity_id
        WHERE e.canonical_id = %s
    """, (canonical_id.lower(),))
    mappings = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return mappings


async def verify_api_key(api_key: str = Security(api_key_header)):
    """Verify API key if one is configured."""
    if not API_KEY:
        return True
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return True

app = FastAPI(
    title="Crypto & Equity Data API",
    description="""
    REST API for accessing cryptocurrency and equity market data from multiple sources.
    
    **Data Sources:**
    - **Artemis**: Crypto fundamentals (fees, revenue, DAU, transactions)
    - **DefiLlama**: DeFi metrics (TVL, volume, protocol data)
    - **Velo**: Derivatives data (funding rates, open interest, liquidations)
    - **CoinGecko**: Market data (price, market cap, volume)
    - **AlphaVantage**: Equity data (stock prices, OHLCV)
    
    **Designed for LLM Access**: Includes a /data-dictionary endpoint for schema understanding.
    """,
    version="1.0.0",
)


@app.exception_handler(psycopg2.Error)
async def database_exception_handler(request: Request, exc: psycopg2.Error):
    """Handle database errors gracefully."""
    return JSONResponse(
        status_code=503,
        content={"error": "Database temporarily unavailable", "detail": "The database is busy or unavailable. Please try again later."}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors."""
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )


@app.get("/")
def root():
    """API health check and overview."""
    return {
        "status": "ok",
        "api": "Crypto & Equity Data Pipeline",
        "version": "1.0.0",
        "endpoints": [
            "/data-dictionary",
            "/sources",
            "/entities",
            "/metrics",
            "/time-series",
            "/latest",
            "/cross-source",
        ]
    }


@app.get("/data-dictionary")
def data_dictionary(_: bool = Depends(verify_api_key)):
    """
    Complete data dictionary for LLM understanding.
    Returns schema, available metrics, sources, and query patterns.
    """
    return {
        "description": "Cryptocurrency and equity market data pipeline with 5 data sources",
        "sources": {
            "artemis": {
                "description": "Crypto fundamentals from Artemis API",
                "frequency": "daily",
                "id_format": "lowercase short IDs (sol, eth, btc)",
                "metrics": [
                    "PRICE", "MC", "FEES", "REVENUE", "DAU", "TXNS", 
                    "24H_VOLUME", "AVG_TXN_FEE", "CIRCULATING_SUPPLY_NATIVE",
                    "FDMC_FEES_RATIO", "FDMC_REVENUE_RATIO", "NEW_USERS",
                    "SPOT_VOLUME", "TOTAL_SUPPLY_NATIVE"
                ],
                "example_assets": ["sol", "eth", "btc", "aave", "uniswap"]
            },
            "defillama": {
                "description": "DeFi metrics from DefiLlama API",
                "frequency": "hourly",
                "id_format": "CoinGecko IDs or slugs (solana, ethereum, aave)",
                "metrics": [
                    "TVL", "CHAIN_TVL", "FEES_24H", "REVENUE_24H",
                    "CHAIN_FEES_24H", "CHAIN_REVENUE_24H", "CHAIN_DEX_VOLUME_24H",
                    "DEX_VOLUME_24H", "DERIVATIVES_VOLUME_24H", "STABLECOIN_SUPPLY",
                    "BRIDGE_VOLUME_24H", "BRIDGE_VOLUME_7D", "BRIDGE_VOLUME_30D"
                ],
                "example_assets": ["solana", "ethereum", "aave", "uniswap", "lido"]
            },
            "velo": {
                "description": "Derivatives data from Velo.xyz API",
                "frequency": "hourly",
                "id_format": "SYMBOL_EXCHANGE (BTC_binance-futures, ETH_bybit)",
                "metrics": [
                    "CLOSE_PRICE", "DOLLAR_VOLUME", "DOLLAR_OI_CLOSE",
                    "FUNDING_RATE_AVG", "LIQ_DOLLAR_VOL"
                ],
                "exchanges": ["binance-futures", "bybit", "okx-swap", "hyperliquid"],
                "example_assets": ["BTC_binance-futures", "ETH_bybit", "SOL_hyperliquid"]
            },
            "coingecko": {
                "description": "Market data from CoinGecko API",
                "frequency": "hourly",
                "id_format": "CoinGecko IDs (bitcoin, ethereum, solana)",
                "metrics": [
                    "PRICE", "MARKET_CAP", "VOLUME_24H", "PRICE_CHANGE_24H_PCT",
                    "MARKET_CAP_CHANGE_24H_PCT", "ATH", "ATH_CHANGE_PCT", "ATL"
                ],
                "example_assets": ["bitcoin", "ethereum", "solana", "cardano"]
            },
            "alphavantage": {
                "description": "Equity data from Alpha Vantage API",
                "frequency": "hourly",
                "id_format": "Stock tickers (COIN, MSTR, SPY)",
                "metrics": [
                    "OPEN", "HIGH", "LOW", "CLOSE", "ADJUSTED_CLOSE",
                    "VOLUME", "DIVIDEND_AMOUNT", "SPLIT_COEFFICIENT"
                ],
                "example_assets": ["COIN", "MSTR", "SPY", "QQQ", "MARA", "RIOT"]
            }
        },
        "entity_system": {
            "description": "Cross-source entity linking via canonical IDs",
            "tables": {
                "entities": "Master entity table with canonical_id, name, symbol, type, sector",
                "entity_source_ids": "Maps source-specific IDs to canonical entities"
            },
            "example": {
                "canonical_id": "bitcoin",
                "mappings": {
                    "artemis": "btc",
                    "defillama": "bitcoin",
                    "velo": "BTC",
                    "coingecko": "bitcoin"
                }
            }
        },
        "query_patterns": {
            "get_latest_price": "GET /latest?metric=PRICE&source=coingecko",
            "get_time_series": "GET /time-series?asset=bitcoin&metric=PRICE&source=coingecko&days=30",
            "cross_source_compare": "GET /cross-source?canonical_id=bitcoin&metric=PRICE",
            "list_entities": "GET /entities?type=token&sector=layer-1",
            "list_metrics": "GET /metrics?source=artemis"
        }
    }


@app.get("/sources")
def list_sources(_: bool = Depends(verify_api_key)):
    """List all available data sources with record counts."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT source, COUNT(*) as records, 
               COUNT(DISTINCT asset) as assets,
               MIN(pulled_at) as earliest,
               MAX(pulled_at) as latest
        FROM metrics 
        GROUP BY source 
        ORDER BY records DESC
    """)
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {
        "sources": [
            {
                "name": r[0],
                "records": r[1],
                "assets": r[2],
                "earliest_date": r[3].isoformat() if r[3] else None,
                "latest_date": r[4].isoformat() if r[4] else None
            }
            for r in rows
        ]
    }


@app.get("/entities")
def list_entities(
    _: bool = Depends(verify_api_key),
    type: Optional[str] = Query(None, description="Filter by entity type (token, chain, protocol, equity, etf)"),
    sector: Optional[str] = Query(None, description="Filter by sector (layer-1, defi, btc_mining, etc.)"),
    source: Optional[str] = Query(None, description="Filter by source (artemis, defillama, velo, coingecko, alphavantage)"),
    search: Optional[str] = Query(None, description="Search by name or symbol"),
    limit: int = Query(100, description="Max results to return")
):
    """List entities with optional filtering."""
    conn = get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT DISTINCT e.canonical_id, e.name, e.symbol, e.entity_type, e.sector
        FROM entities e
    """
    conditions = []
    params = []
    
    if source:
        query += " JOIN entity_source_ids esi ON e.entity_id = esi.entity_id"
        conditions.append("esi.source = %s")
        params.append(source)
    
    if type:
        conditions.append("e.entity_type = %s")
        params.append(type)
    
    if sector:
        conditions.append("e.sector = %s")
        params.append(sector)
    
    if search:
        conditions.append("(e.name ILIKE %s OR e.symbol ILIKE %s OR e.canonical_id ILIKE %s)")
        search_param = f"%{search}%"
        params.extend([search_param, search_param, search_param])
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY e.canonical_id LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {
        "count": len(rows),
        "entities": [
            {
                "canonical_id": r[0],
                "name": r[1],
                "symbol": r[2],
                "type": r[3],
                "sector": r[4]
            }
            for r in rows
        ]
    }


@app.get("/entities/{canonical_id}")
def get_entity(canonical_id: str, _: bool = Depends(verify_api_key)):
    """Get entity details with all source mappings and available metrics per source."""
    canonical_id = canonical_id.lower()
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT e.entity_id, e.canonical_id, e.name, e.symbol, e.entity_type, e.sector
        FROM entities e
        WHERE e.canonical_id = %s
    """, (canonical_id,))
    row = cur.fetchone()
    
    if not row:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Entity '{canonical_id}' not found")
    
    entity_id = row[0]
    
    cur.execute("""
        SELECT source, source_id FROM entity_source_ids WHERE entity_id = %s
    """, (entity_id,))
    mappings = {r[0]: r[1] for r in cur.fetchall()}
    
    available_metrics = {}
    for source, source_id in mappings.items():
        cur.execute("""
            SELECT DISTINCT metric_name FROM metrics 
            WHERE source = %s AND asset = %s
            ORDER BY metric_name
        """, (source, source_id))
        metrics = [r[0] for r in cur.fetchall()]
        if metrics:
            available_metrics[source] = metrics
    
    cur.close()
    conn.close()
    
    return {
        "canonical_id": row[1],
        "name": row[2],
        "symbol": row[3],
        "type": row[4],
        "sector": row[5],
        "source_mappings": mappings,
        "available_metrics": available_metrics
    }


@app.get("/metrics")
def list_metrics(
    _: bool = Depends(verify_api_key),
    source: Optional[str] = Query(None, description="Filter by source - case insensitive"),
    asset: Optional[str] = Query(None, description="Filter by asset")
):
    """List available metrics with counts."""
    source = normalize_source(source)
    
    conn = get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT metric_name, COUNT(*) as records, COUNT(DISTINCT asset) as assets
        FROM metrics
    """
    conditions = []
    params = []
    
    if source:
        conditions.append("source = %s")
        params.append(source)
    
    if asset:
        conditions.append("asset = %s")
        params.append(asset)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " GROUP BY metric_name ORDER BY records DESC"
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {
        "count": len(rows),
        "metrics": [
            {"name": r[0], "records": r[1], "assets": r[2]}
            for r in rows
        ]
    }


@app.get("/latest")
def get_latest(
    _: bool = Depends(verify_api_key),
    metric: str = Query(..., description="Metric name (PRICE, TVL, FEES, etc.) - case insensitive"),
    source: Optional[str] = Query(None, description="Filter by source - case insensitive, auto-selected based on metric if omitted"),
    assets: Optional[str] = Query(None, description="Comma-separated list of asset IDs or canonical IDs (bitcoin, ethereum)"),
    limit: int = Query(100, description="Max results")
):
    """Get latest values for a metric across all assets. Accepts canonical IDs and auto-resolves to source-specific IDs."""
    metric = normalize_metric(metric)
    source = normalize_source(source)
    
    if not source:
        preferred = get_preferred_source_for_metric(metric)
        if preferred:
            source = preferred
    
    conn = get_connection()
    cur = conn.cursor()
    
    resolved_assets = []
    if assets:
        asset_list = [a.strip().lower() for a in assets.split(",")]
        for asset in asset_list:
            mappings = get_source_mappings(asset, conn)
            if mappings and source and source in mappings:
                resolved_assets.append(mappings[source])
            elif mappings and not source:
                for preferred_source in ["coingecko", "artemis", "defillama", "velo", "alphavantage"]:
                    if preferred_source in mappings:
                        resolved_assets.append(mappings[preferred_source])
                        break
            else:
                resolved_assets.append(asset)
    
    query = """
        SELECT DISTINCT ON (asset) 
            asset, metric_name, value, pulled_at, source
        FROM metrics
        WHERE metric_name = %s
    """
    params = [metric]
    
    if source:
        query += " AND source = %s"
        params.append(source)
    
    if resolved_assets:
        query += " AND asset = ANY(%s)"
        params.append(resolved_assets)
    
    query += " ORDER BY asset, pulled_at DESC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {
        "metric": metric,
        "count": len(rows),
        "data": [
            {
                "asset": r[0],
                "value": float(r[2]) if r[2] else None,
                "timestamp": r[3].isoformat() if r[3] else None,
                "source": r[4]
            }
            for r in rows
        ]
    }


@app.get("/time-series")
def get_time_series(
    _: bool = Depends(verify_api_key),
    asset: str = Query(..., description="Asset ID or canonical ID (bitcoin, ethereum, solana) - auto-resolves to source-specific ID"),
    metric: str = Query(..., description="Metric name - case insensitive"),
    source: Optional[str] = Query(None, description="Filter by source - case insensitive, auto-selected if omitted"),
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    days: Optional[int] = Query(None, description="Number of days back from today"),
    limit: int = Query(1000, description="Max results")
):
    """Get time series data for an asset/metric combination. Accepts canonical IDs (bitcoin, ethereum) and auto-resolves to source-specific IDs."""
    metric = normalize_metric(metric)
    source = normalize_source(source)
    
    conn = get_connection()
    cur = conn.cursor()
    
    original_asset = asset
    source_mappings = get_source_mappings(asset, conn)
    
    if not source:
        preferred = get_preferred_source_for_metric(metric)
        if preferred:
            source = preferred
    
    if source_mappings:
        if source and source in source_mappings:
            asset = source_mappings[source]
        elif source:
            pass
        elif not source:
            for preferred_source in ["coingecko", "artemis", "defillama", "velo", "alphavantage"]:
                if preferred_source in source_mappings:
                    asset = source_mappings[preferred_source]
                    source = preferred_source
                    break
    
    query = """
        SELECT pulled_at, value, source
        FROM metrics
        WHERE asset = %s AND metric_name = %s
    """
    params = [asset, metric]
    
    if source:
        query += " AND source = %s"
        params.append(source)
    
    if start_date:
        query += " AND pulled_at >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND pulled_at <= %s"
        params.append(end_date)
    
    if days and not start_date:
        query += " AND pulled_at >= NOW() - make_interval(days => %s)"
        params.append(days)
    
    query += " ORDER BY pulled_at DESC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    response = {
        "asset": asset,
        "metric": metric,
        "count": len(rows),
        "data": [
            {
                "timestamp": r[0].isoformat() if r[0] else None,
                "value": float(r[1]) if r[1] else None,
                "source": r[2]
            }
            for r in rows
        ]
    }
    
    if original_asset != asset or source_mappings:
        response["_meta"] = {
            "requested_asset": original_asset,
            "resolved_asset": asset,
            "resolved_source": source
        }
    
    return response


@app.get("/cross-source")
def cross_source_comparison(
    _: bool = Depends(verify_api_key),
    canonical_id: str = Query(..., description="Canonical entity ID (bitcoin, ethereum, solana)"),
    metric: Optional[str] = Query(None, description="Filter by metric name"),
    days: int = Query(7, description="Number of days to look back")
):
    """
    Compare data across sources for a single entity.
    Uses entity mappings to find the same asset across different sources.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT esi.source, esi.source_id
        FROM entities e
        JOIN entity_source_ids esi ON e.entity_id = esi.entity_id
        WHERE e.canonical_id = %s
    """, (canonical_id,))
    mappings = cur.fetchall()
    
    if not mappings:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Entity '{canonical_id}' not found")
    
    results = {}
    for source, source_id in mappings:
        query = """
            SELECT metric_name, pulled_at, value
            FROM metrics
            WHERE source = %s AND asset = %s
              AND pulled_at >= NOW() - make_interval(days => %s)
        """
        params = [source, source_id, days]
        
        if metric:
            query += " AND metric_name = %s"
            params.append(metric)
        
        query += " ORDER BY metric_name, pulled_at DESC"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        source_data = {}
        for r in rows:
            metric_name = r[0]
            if metric_name not in source_data:
                source_data[metric_name] = []
            source_data[metric_name].append({
                "timestamp": r[1].isoformat() if r[1] else None,
                "value": float(r[2]) if r[2] else None
            })
        
        results[source] = {
            "source_id": source_id,
            "metrics": source_data
        }
    
    cur.close()
    conn.close()
    
    return {
        "canonical_id": canonical_id,
        "sources": results
    }


@app.get("/stats")
def get_stats(_: bool = Depends(verify_api_key)):
    """Get overall database statistics."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM metrics")
    total_records = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM entities")
    total_entities = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT asset) FROM metrics")
    total_assets = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT metric_name) FROM metrics")
    total_metrics = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(pulled_at), MAX(pulled_at) FROM metrics")
    date_range = cur.fetchone()
    
    cur.execute("""
        SELECT source_name, MAX(pulled_at) as last_pull, status
        FROM pulls
        GROUP BY source_name, status
        ORDER BY last_pull DESC
    """)
    recent_pulls = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {
        "total_records": total_records,
        "total_entities": total_entities,
        "total_assets": total_assets,
        "total_metrics": total_metrics,
        "date_range": {
            "earliest": date_range[0].isoformat() if date_range[0] else None,
            "latest": date_range[1].isoformat() if date_range[1] else None
        },
        "recent_pulls": [
            {
                "source": r[0],
                "last_pull": r[1].isoformat() if r[1] else None,
                "status": r[2]
            }
            for r in recent_pulls[:10]
        ]
    }


# =============================================================================
# ADMIN ENDPOINTS - Database Administration
# =============================================================================

ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY")
admin_key_header = APIKeyHeader(name="X-Admin-Key", auto_error=False)


async def verify_admin_key(admin_key: str = Security(admin_key_header)):
    """Verify admin API key for privileged operations."""
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=503, detail="Admin API not configured. Set ADMIN_API_KEY secret.")
    if admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing admin key")
    return True


@app.get("/admin/tables")
def admin_list_tables(_: bool = Depends(verify_admin_key)):
    """List all database tables with row counts."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    
    result = []
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        result.append({"table": table, "rows": count})
    
    cur.close()
    conn.close()
    
    return {"tables": result}


@app.get("/admin/schema/{table_name}")
def admin_table_schema(table_name: str, _: bool = Depends(verify_admin_key)):
    """Get schema details for a specific table."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    columns = cur.fetchall()
    
    if not columns:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    
    cur.close()
    conn.close()
    
    return {
        "table": table_name,
        "columns": [
            {
                "name": c[0],
                "type": c[1],
                "nullable": c[2] == "YES",
                "default": c[3]
            }
            for c in columns
        ]
    }


class SQLQuery(BaseModel):
    sql: str
    params: Optional[List] = None


@app.post("/admin/query")
def admin_execute_query(query: SQLQuery, _: bool = Depends(verify_admin_key)):
    """
    Execute a SQL query on the database.
    
    For SELECT queries: Returns results as JSON
    For INSERT/UPDATE/DELETE: Returns affected row count
    
    Use with caution - this has full database access.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    sql = query.sql.strip()
    params = query.params or []
    
    try:
        cur.execute(sql, params)
        
        is_select = sql.upper().startswith("SELECT") or sql.upper().startswith("WITH")
        
        if is_select:
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
            
            data = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    elif isinstance(val, (bytes, memoryview)):
                        val = str(val)
                    row_dict[col] = val
                data.append(row_dict)
            
            result = {
                "success": True,
                "type": "select",
                "columns": columns,
                "row_count": len(data),
                "data": data
            }
        else:
            conn.commit()
            result = {
                "success": True,
                "type": "mutation",
                "rows_affected": cur.rowcount,
                "message": f"Query executed successfully. {cur.rowcount} rows affected."
            }
        
        cur.close()
        conn.close()
        return result
        
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail=f"SQL Error: {str(e)}")


@app.get("/admin/source-status")
def admin_source_status(_: bool = Depends(verify_admin_key)):
    """Get detailed status for all data sources."""
    conn = get_connection()
    cur = conn.cursor()
    
    sources = ['artemis', 'defillama', 'velo', 'coingecko', 'alphavantage']
    result = []
    
    for source in sources:
        cur.execute("""
            SELECT 
                COUNT(*) as records,
                COUNT(DISTINCT asset) as assets,
                COUNT(DISTINCT metric_name) as metrics,
                MIN(pulled_at) as earliest,
                MAX(pulled_at) as latest
            FROM metrics WHERE source = %s
        """, (source,))
        row = cur.fetchone()
        
        hours_ago = None
        if row[4]:
            from datetime import timezone
            latest = row[4]
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)
            hours_ago = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
        
        result.append({
            "source": source,
            "records": row[0],
            "assets": row[1],
            "metrics": row[2],
            "earliest": row[3].isoformat() if row[3] else None,
            "latest": row[4].isoformat() if row[4] else None,
            "hours_since_update": round(hours_ago, 1) if hours_ago else None
        })
    
    cur.close()
    conn.close()
    
    return {"sources": result}


@app.post("/admin/truncate/{table_name}")
def admin_truncate_table(table_name: str, confirm: bool = Query(False), _: bool = Depends(verify_admin_key)):
    """
    Truncate a table (delete all rows).
    Requires confirm=true query parameter.
    """
    allowed_tables = ["metrics", "pulls"]
    
    if table_name not in allowed_tables:
        raise HTTPException(status_code=403, detail=f"Cannot truncate '{table_name}'. Allowed: {allowed_tables}")
    
    if not confirm:
        raise HTTPException(status_code=400, detail="Add ?confirm=true to confirm truncation")
    
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count_before = cur.fetchone()[0]
    
    cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
    conn.commit()
    
    cur.close()
    conn.close()
    
    return {
        "success": True,
        "table": table_name,
        "rows_deleted": count_before,
        "message": f"Table '{table_name}' truncated. {count_before} rows removed."
    }


# =============================================================================
# ADMIN - Backfill & Gap Management
# =============================================================================

import subprocess
import threading

backfill_status = {}


@app.post("/admin/backfill/{source}")
def admin_trigger_backfill(
    source: str,
    days: int = Query(None, description="Number of days to backfill"),
    start_date: str = Query(None, description="Start date YYYY-MM-DD"),
    end_date: str = Query(None, description="End date YYYY-MM-DD"),
    background: bool = Query(True, description="Run in background"),
    _: bool = Depends(verify_admin_key)
):
    """Trigger a backfill for a specific data source."""
    valid_sources = ['artemis', 'defillama', 'velo', 'coingecko', 'alphavantage']
    if source not in valid_sources:
        raise HTTPException(status_code=400, detail=f"Invalid source. Valid: {valid_sources}")
    
    script = f"backfill_{source}.py"
    cmd = ["python", "-u", script]  # -u for unbuffered output
    
    if days:
        cmd.extend(["--days", str(days)])
    elif start_date and end_date:
        cmd.extend(["--start", start_date, "--end", end_date])
    
    if background:
        def run_backfill():
            backfill_status[source] = {"status": "running", "started": datetime.now().isoformat()}
            try:
                # capture_output=False so we can see progress in deployment logs
                result = subprocess.run(cmd, capture_output=False, timeout=50400)
                backfill_status[source] = {
                    "status": "completed" if result.returncode == 0 else "failed",
                    "exit_code": result.returncode,
                    "finished": datetime.now().isoformat()
                }
            except Exception as e:
                backfill_status[source] = {"status": "error", "error": str(e)}
        
        thread = threading.Thread(target=run_backfill, daemon=True)
        thread.start()
        
        return {
            "success": True,
            "message": f"Backfill started for {source} in background",
            "command": " ".join(cmd),
            "check_status": f"/admin/backfill-status/{source}"
        }
    else:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            return {
                "success": result.returncode == 0,
                "exit_code": result.returncode,
                "stdout": result.stdout[-5000:] if result.stdout else None,
                "stderr": result.stderr[-2000:] if result.stderr else None
            }
        except subprocess.TimeoutExpired:
            raise HTTPException(status_code=504, detail="Backfill timed out (1 hour limit for sync mode)")


@app.get("/admin/backfill-status/{source}")
def admin_backfill_status(source: str, _: bool = Depends(verify_admin_key)):
    """Check status of a running backfill."""
    if source in backfill_status:
        return backfill_status[source]
    return {"status": "not_running", "message": f"No backfill running or completed for {source}"}


@app.get("/admin/gaps/{source}")
def admin_detect_gaps(
    source: str,
    days: int = Query(30, description="Days to check for gaps"),
    _: bool = Depends(verify_admin_key)
):
    """Detect gaps in data for a source."""
    valid_sources = ['artemis', 'defillama', 'velo', 'coingecko', 'alphavantage']
    if source not in valid_sources:
        raise HTTPException(status_code=400, detail=f"Invalid source. Valid: {valid_sources}")
    
    granularity_map = {
        'artemis': 'daily', 'defillama': 'daily', 'alphavantage': 'daily',
        'velo': 'hourly', 'coingecko': 'hourly'
    }
    granularity = granularity_map[source]
    
    conn = get_connection()
    cur = conn.cursor()
    
    if granularity == 'daily':
        cur.execute("""
            WITH date_range AS (
                SELECT generate_series(
                    CURRENT_DATE - INTERVAL '%s days',
                    CURRENT_DATE - INTERVAL '1 day',
                    INTERVAL '1 day'
                )::date AS expected_date
            ),
            actual_dates AS (
                SELECT DISTINCT DATE(pulled_at) as actual_date
                FROM metrics WHERE source = %s
                AND pulled_at >= CURRENT_DATE - INTERVAL '%s days'
            )
            SELECT expected_date FROM date_range
            LEFT JOIN actual_dates ON date_range.expected_date = actual_dates.actual_date
            WHERE actual_dates.actual_date IS NULL
            ORDER BY expected_date
        """, (days, source, days))
    else:
        cur.execute("""
            WITH hour_range AS (
                SELECT generate_series(
                    DATE_TRUNC('hour', NOW() - INTERVAL '%s days'),
                    DATE_TRUNC('hour', NOW() - INTERVAL '1 hour'),
                    INTERVAL '1 hour'
                ) AS expected_hour
            ),
            actual_hours AS (
                SELECT DISTINCT DATE_TRUNC('hour', pulled_at) as actual_hour
                FROM metrics WHERE source = %s
                AND pulled_at >= NOW() - INTERVAL '%s days'
            )
            SELECT expected_hour FROM hour_range
            LEFT JOIN actual_hours ON hour_range.expected_hour = actual_hours.actual_hour
            WHERE actual_hours.actual_hour IS NULL
            ORDER BY expected_hour
        """, (days, source, days))
    
    missing = [row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]) for row in cur.fetchall()]
    cur.close()
    conn.close()
    
    return {
        "source": source,
        "granularity": granularity,
        "days_checked": days,
        "gaps_found": len(missing),
        "missing_periods": missing[:100],
        "truncated": len(missing) > 100
    }


@app.post("/admin/fill-gaps/{source}")
def admin_fill_gaps(
    source: str,
    days: int = Query(30, description="Days to check and fill"),
    _: bool = Depends(verify_admin_key)
):
    """Detect and fill gaps for a source by running backfill."""
    gaps = admin_detect_gaps(source, days, _)
    
    if gaps["gaps_found"] == 0:
        return {"success": True, "message": f"No gaps found for {source} in last {days} days"}
    
    return admin_trigger_backfill(source, days=days + 3, background=True, _=_)


# =============================================================================
# ADMIN - Entity Management
# =============================================================================

class EntityCreate(BaseModel):
    canonical_id: str
    name: str
    symbol: Optional[str] = None
    entity_type: str = "token"
    asset_class: str = "crypto"
    sector: Optional[str] = None
    parent_chain: Optional[str] = None
    coingecko_id: Optional[str] = None


class EntitySourceMapping(BaseModel):
    source: str
    source_id: str


@app.get("/admin/entities")
def admin_list_entities(
    limit: int = Query(100),
    offset: int = Query(0),
    search: Optional[str] = Query(None),
    _: bool = Depends(verify_admin_key)
):
    """List all entities with full details."""
    conn = get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT entity_id, canonical_id, name, symbol, entity_type, asset_class, sector, parent_chain, coingecko_id, is_active
        FROM entities
    """
    params = []
    
    if search:
        query += " WHERE canonical_id ILIKE %s OR name ILIKE %s OR symbol ILIKE %s"
        params = [f"%{search}%", f"%{search}%", f"%{search}%"]
    
    query += " ORDER BY canonical_id LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    cur.execute("SELECT COUNT(*) FROM entities")
    total = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "entities": [
            {
                "entity_id": r[0], "canonical_id": r[1], "name": r[2], "symbol": r[3],
                "entity_type": r[4], "asset_class": r[5], "sector": r[6],
                "parent_chain": r[7], "coingecko_id": r[8], "is_active": r[9]
            }
            for r in rows
        ]
    }


@app.post("/admin/entities")
def admin_create_entity(entity: EntityCreate, _: bool = Depends(verify_admin_key)):
    """Create a new entity."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO entities (canonical_id, name, symbol, entity_type, asset_class, sector, parent_chain, coingecko_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING entity_id
        """, (entity.canonical_id, entity.name, entity.symbol, entity.entity_type, 
              entity.asset_class, entity.sector, entity.parent_chain, entity.coingecko_id))
        entity_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return {"success": True, "entity_id": entity_id, "canonical_id": entity.canonical_id}
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/admin/entities/{canonical_id}")
def admin_update_entity(canonical_id: str, entity: EntityCreate, _: bool = Depends(verify_admin_key)):
    """Update an existing entity."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        UPDATE entities SET name=%s, symbol=%s, entity_type=%s, asset_class=%s, 
        sector=%s, parent_chain=%s, coingecko_id=%s
        WHERE canonical_id = %s
        RETURNING entity_id
    """, (entity.name, entity.symbol, entity.entity_type, entity.asset_class,
          entity.sector, entity.parent_chain, entity.coingecko_id, canonical_id))
    
    result = cur.fetchone()
    if not result:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Entity '{canonical_id}' not found")
    
    conn.commit()
    cur.close()
    conn.close()
    return {"success": True, "entity_id": result[0], "canonical_id": canonical_id}


@app.delete("/admin/entities/{canonical_id}")
def admin_delete_entity(canonical_id: str, confirm: bool = Query(False), _: bool = Depends(verify_admin_key)):
    """Delete an entity and its source mappings."""
    if not confirm:
        raise HTTPException(status_code=400, detail="Add ?confirm=true to confirm deletion")
    
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT entity_id FROM entities WHERE canonical_id = %s", (canonical_id,))
    result = cur.fetchone()
    if not result:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Entity '{canonical_id}' not found")
    
    entity_id = result[0]
    cur.execute("DELETE FROM entity_source_ids WHERE entity_id = %s", (entity_id,))
    mappings_deleted = cur.rowcount
    cur.execute("DELETE FROM entities WHERE entity_id = %s", (entity_id,))
    conn.commit()
    cur.close()
    conn.close()
    
    return {"success": True, "canonical_id": canonical_id, "mappings_deleted": mappings_deleted}


@app.get("/admin/entities/{canonical_id}/mappings")
def admin_get_entity_mappings(canonical_id: str, _: bool = Depends(verify_admin_key)):
    """Get all source ID mappings for an entity."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT entity_id FROM entities WHERE canonical_id = %s", (canonical_id,))
    result = cur.fetchone()
    if not result:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Entity '{canonical_id}' not found")
    
    entity_id = result[0]
    cur.execute("SELECT source, source_id FROM entity_source_ids WHERE entity_id = %s", (entity_id,))
    mappings = {r[0]: r[1] for r in cur.fetchall()}
    
    cur.close()
    conn.close()
    return {"canonical_id": canonical_id, "entity_id": entity_id, "mappings": mappings}


@app.post("/admin/entities/{canonical_id}/mappings")
def admin_add_entity_mapping(canonical_id: str, mapping: EntitySourceMapping, _: bool = Depends(verify_admin_key)):
    """Add or update a source ID mapping for an entity."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT entity_id FROM entities WHERE canonical_id = %s", (canonical_id,))
    result = cur.fetchone()
    if not result:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Entity '{canonical_id}' not found")
    
    entity_id = result[0]
    cur.execute("""
        INSERT INTO entity_source_ids (entity_id, source, source_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (entity_id, source) DO UPDATE SET source_id = EXCLUDED.source_id
    """, (entity_id, mapping.source, mapping.source_id))
    conn.commit()
    cur.close()
    conn.close()
    
    return {"success": True, "canonical_id": canonical_id, "source": mapping.source, "source_id": mapping.source_id}


@app.delete("/admin/entities/{canonical_id}/mappings/{source}")
def admin_delete_entity_mapping(canonical_id: str, source: str, _: bool = Depends(verify_admin_key)):
    """Delete a source ID mapping for an entity."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT entity_id FROM entities WHERE canonical_id = %s", (canonical_id,))
    result = cur.fetchone()
    if not result:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail=f"Entity '{canonical_id}' not found")
    
    entity_id = result[0]
    cur.execute("DELETE FROM entity_source_ids WHERE entity_id = %s AND source = %s", (entity_id, source))
    deleted = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    
    return {"success": deleted, "canonical_id": canonical_id, "source": source}


# =============================================================================
# ADMIN - Pull History & Maintenance
# =============================================================================

@app.get("/admin/pulls")
def admin_list_pulls(
    source: Optional[str] = Query(None),
    limit: int = Query(50),
    _: bool = Depends(verify_admin_key)
):
    """Get recent pull history."""
    conn = get_connection()
    cur = conn.cursor()
    
    query = "SELECT id, source_name, pulled_at, records_count, status FROM pulls"
    params = []
    
    if source:
        query += " WHERE source_name = %s"
        params.append(source)
    
    query += " ORDER BY pulled_at DESC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return {
        "pulls": [
            {"id": r[0], "source": r[1], "pulled_at": r[2].isoformat() if r[2] else None, 
             "records": r[3], "status": r[4]}
            for r in rows
        ]
    }


@app.post("/admin/maintenance/vacuum")
def admin_vacuum(table: Optional[str] = Query(None), _: bool = Depends(verify_admin_key)):
    """Run VACUUM ANALYZE on tables to optimize performance."""
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        if table:
            cur.execute(f"VACUUM ANALYZE {table}")
            msg = f"VACUUM ANALYZE completed for {table}"
        else:
            cur.execute("VACUUM ANALYZE")
            msg = "VACUUM ANALYZE completed for all tables"
        cur.close()
        conn.close()
        return {"success": True, "message": msg}
    except Exception as e:
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/admin/db-size")
def admin_db_size(_: bool = Depends(verify_admin_key)):
    """Get database and table sizes."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
    db_size = cur.fetchone()[0]
    
    cur.execute("""
        SELECT table_name, 
               pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size,
               pg_total_relation_size(quote_ident(table_name)) as bytes
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY bytes DESC
    """)
    tables = [{"table": r[0], "size": r[1]} for r in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    return {"database_size": db_size, "tables": tables}


@app.get("/admin/metrics-summary")
def admin_metrics_summary(_: bool = Depends(verify_admin_key)):
    """Get summary of all metrics across sources."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT source, metric_name, COUNT(*) as records, COUNT(DISTINCT asset) as assets
        FROM metrics
        GROUP BY source, metric_name
        ORDER BY source, records DESC
    """)
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    summary = {}
    for r in rows:
        source = r[0]
        if source not in summary:
            summary[source] = []
        summary[source].append({"metric": r[1], "records": r[2], "assets": r[3]})
    
    return {"metrics_by_source": summary}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)

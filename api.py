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


def normalize_source(source: Optional[str]) -> Optional[str]:
    """Normalize source to lowercase."""
    if source is None:
        return None
    return source.lower().strip()


def normalize_metric(metric: str) -> str:
    """Normalize metric to uppercase."""
    return metric.upper().strip()


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
    """Get entity details with all source mappings."""
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
    
    cur.close()
    conn.close()
    
    return {
        "canonical_id": row[1],
        "name": row[2],
        "symbol": row[3],
        "type": row[4],
        "sector": row[5],
        "source_mappings": mappings
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
    source: Optional[str] = Query(None, description="Filter by source - case insensitive"),
    assets: Optional[str] = Query(None, description="Comma-separated list of assets"),
    limit: int = Query(100, description="Max results")
):
    """Get latest values for a metric across all assets."""
    metric = normalize_metric(metric)
    source = normalize_source(source)
    
    conn = get_connection()
    cur = conn.cursor()
    
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
    
    if assets:
        asset_list = [a.strip() for a in assets.split(",")]
        query += " AND asset = ANY(%s)"
        params.append(asset_list)
    
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
    asset: str = Query(..., description="Asset ID or canonical ID (bitcoin, ethereum, solana)"),
    metric: str = Query(..., description="Metric name - case insensitive"),
    source: Optional[str] = Query(None, description="Filter by source - case insensitive"),
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    days: Optional[int] = Query(None, description="Number of days back from today"),
    limit: int = Query(1000, description="Max results")
):
    """Get time series data for an asset/metric combination."""
    metric = normalize_metric(metric)
    source = normalize_source(source)
    
    conn = get_connection()
    cur = conn.cursor()
    
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
    
    return {
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

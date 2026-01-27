"""
MCP Server for Crypto & Equity Data Pipeline
Allows Claude Desktop to query financial data via tools.
"""

import json
import sys
import requests
from typing import Any

API_BASE = "http://localhost:8000"


def call_api(endpoint: str, params: dict = None) -> dict:
    """Call the REST API and return JSON response."""
    try:
        resp = requests.get(f"{API_BASE}{endpoint}", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


def get_data_dictionary() -> dict:
    """Get complete schema and available metrics for understanding the data."""
    return call_api("/data-dictionary")


def list_sources() -> dict:
    """List all data sources with record counts."""
    return call_api("/sources")


def search_entities(
    entity_type: str = None,
    sector: str = None,
    source: str = None,
    search: str = None,
    limit: int = 20
) -> dict:
    """Search for entities (assets) with optional filters."""
    params = {"limit": limit}
    if entity_type:
        params["type"] = entity_type
    if sector:
        params["sector"] = sector
    if source:
        params["source"] = source
    if search:
        params["search"] = search
    return call_api("/entities", params)


def get_entity_details(canonical_id: str) -> dict:
    """Get full details for a specific entity including all source mappings."""
    return call_api(f"/entities/{canonical_id}")


def list_metrics(source: str = None) -> dict:
    """List available metrics, optionally filtered by source."""
    params = {}
    if source:
        params["source"] = source
    return call_api("/metrics", params)


def get_latest_values(metric: str, source: str = None, limit: int = 50) -> dict:
    """Get the latest value for a metric across all assets."""
    params = {"metric": metric, "limit": limit}
    if source:
        params["source"] = source
    return call_api("/latest", params)


def get_time_series(
    asset: str,
    metric: str,
    source: str,
    days: int = 30
) -> dict:
    """Get historical time series data for an asset/metric combination."""
    params = {
        "asset": asset,
        "metric": metric,
        "source": source,
        "days": days
    }
    return call_api("/time-series", params)


def compare_cross_source(canonical_id: str, metric: str = None) -> dict:
    """Compare data for an entity across different sources."""
    params = {"canonical_id": canonical_id}
    if metric:
        params["metric"] = metric
    return call_api("/cross-source", params)


def get_stats() -> dict:
    """Get database statistics and recent pull history."""
    return call_api("/stats")


TOOLS = {
    "get_data_dictionary": {
        "description": "Get complete schema documentation including all sources, metrics, and example queries. Call this first to understand available data.",
        "parameters": {},
        "handler": get_data_dictionary
    },
    "list_sources": {
        "description": "List all data sources (artemis, defillama, velo, coingecko, alphavantage) with record counts.",
        "parameters": {},
        "handler": list_sources
    },
    "search_entities": {
        "description": "Search for entities (crypto assets, stocks). Filter by type (token/chain/protocol/equity), sector (layer-1/defi/etc), or source.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_type": {"type": "string", "description": "Filter by type: token, chain, protocol, equity"},
                "sector": {"type": "string", "description": "Filter by sector: layer-1, defi, meme, etc"},
                "source": {"type": "string", "description": "Filter by source: artemis, defillama, velo, coingecko, alphavantage"},
                "search": {"type": "string", "description": "Search term for name/symbol"},
                "limit": {"type": "integer", "description": "Max results (default 20)"}
            }
        },
        "handler": search_entities
    },
    "get_entity_details": {
        "description": "Get full details for a specific entity including IDs across all sources.",
        "parameters": {
            "type": "object",
            "properties": {
                "canonical_id": {"type": "string", "description": "The canonical entity ID (e.g., bitcoin, ethereum, solana)"}
            },
            "required": ["canonical_id"]
        },
        "handler": get_entity_details
    },
    "list_metrics": {
        "description": "List all available metrics (PRICE, TVL, FEES, etc) with counts per source.",
        "parameters": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "description": "Optional: filter by source"}
            }
        },
        "handler": list_metrics
    },
    "get_latest_values": {
        "description": "Get the most recent value for a metric across all assets. Good for rankings and comparisons.",
        "parameters": {
            "type": "object",
            "properties": {
                "metric": {"type": "string", "description": "Metric name (e.g., PRICE, TVL, MC, FEES)"},
                "source": {"type": "string", "description": "Optional: filter by source"},
                "limit": {"type": "integer", "description": "Max results (default 50)"}
            },
            "required": ["metric"]
        },
        "handler": get_latest_values
    },
    "get_time_series": {
        "description": "Get historical time series data. Use for price history, TVL trends, etc.",
        "parameters": {
            "type": "object",
            "properties": {
                "asset": {"type": "string", "description": "Asset ID (format varies by source, check entity details)"},
                "metric": {"type": "string", "description": "Metric name (e.g., PRICE, TVL)"},
                "source": {"type": "string", "description": "Source: artemis, defillama, velo, coingecko, alphavantage"},
                "days": {"type": "integer", "description": "Days of history (default 30)"}
            },
            "required": ["asset", "metric", "source"]
        },
        "handler": get_time_series
    },
    "compare_cross_source": {
        "description": "Compare data for an entity across different sources. Great for seeing price/metrics from multiple providers.",
        "parameters": {
            "type": "object",
            "properties": {
                "canonical_id": {"type": "string", "description": "Canonical entity ID (e.g., bitcoin, ethereum)"},
                "metric": {"type": "string", "description": "Optional: filter to specific metric"}
            },
            "required": ["canonical_id"]
        },
        "handler": compare_cross_source
    },
    "get_stats": {
        "description": "Get database statistics including total records, last pull times, and system health.",
        "parameters": {},
        "handler": get_stats
    }
}


def handle_request(request: dict) -> dict:
    """Handle an MCP request."""
    method = request.get("method", "")
    req_id = request.get("id")
    
    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "crypto-equity-data",
                    "version": "1.0.0"
                }
            }
        }
    
    elif method == "tools/list":
        tools_list = []
        for name, info in TOOLS.items():
            tool_def = {
                "name": name,
                "description": info["description"],
                "inputSchema": info.get("parameters", {"type": "object", "properties": {}})
            }
            tools_list.append(tool_def)
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": tools_list}
        }
    
    elif method == "tools/call":
        params = request.get("params", {})
        tool_name = params.get("name")
        tool_args = params.get("arguments", {})
        
        if tool_name not in TOOLS:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}
            }
        
        handler = TOOLS[tool_name]["handler"]
        try:
            result = handler(**tool_args)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, indent=2)}]
                }
            }
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32000, "message": str(e)}
            }
    
    elif method == "notifications/initialized":
        return None
    
    else:
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        }


def main():
    """Run the MCP server via stdio."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            request = json.loads(line)
            response = handle_request(request)
            if response:
                print(json.dumps(response), flush=True)
        except json.JSONDecodeError:
            error_response = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": "Parse error"}
            }
            print(json.dumps(error_response), flush=True)


if __name__ == "__main__":
    main()

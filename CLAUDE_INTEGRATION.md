# Claude Integration Guide

This guide explains how to connect Claude to the Crypto & Equity Data API.

## Option 1: Claude Desktop with MCP (Recommended)

The MCP server lets Claude Desktop directly query your financial data using tools.

### Setup

1. **Deploy your Replit project** to get a public URL (e.g., `https://your-project.replit.app`)

2. **Update the API_BASE in mcp_server.py** to your deployed URL:
   ```python
   API_BASE = "https://your-project.replit.app"
   ```

3. **Add to Claude Desktop config** (`~/Library/Application Support/Claude/claude_desktop_config.json` on Mac):
   ```json
   {
     "mcpServers": {
       "crypto-data": {
         "command": "python",
         "args": ["/path/to/your/mcp_server.py"]
       }
     }
   }
   ```

4. **Restart Claude Desktop** - you'll see the tools available.

### Available Tools

| Tool | Description |
|------|-------------|
| `get_data_dictionary` | Schema + all metrics (call first!) |
| `list_sources` | Data sources with record counts |
| `search_entities` | Find assets by type/sector/name |
| `get_entity_details` | Full entity info + source IDs |
| `list_metrics` | Available metrics per source |
| `get_latest_values` | Latest metric values (rankings) |
| `get_time_series` | Historical data for charts |
| `compare_cross_source` | Compare entity across sources |
| `get_stats` | Database health + stats |

### Example Prompts for Claude

- "What's the current TVL of the top 10 DeFi protocols?"
- "Show me Bitcoin's price history for the last 30 days"
- "Compare Ethereum data across all sources"
- "Which layer-1 chains have the highest fees?"
- "Get me Solana's metrics from Artemis"

---

## Option 2: Claude API with Tools

If using Claude via the Anthropic API, define tools that call the REST endpoints:

```python
import anthropic

client = anthropic.Anthropic()

tools = [
    {
        "name": "get_time_series",
        "description": "Get historical data for an asset",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset": {"type": "string"},
                "metric": {"type": "string"},
                "source": {"type": "string"},
                "days": {"type": "integer"}
            },
            "required": ["asset", "metric", "source"]
        }
    }
]

response = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1024,
    tools=tools,
    messages=[{"role": "user", "content": "Get Bitcoin price history"}]
)
```

---

## Option 3: Context Injection

For simple use cases, fetch the data dictionary and include it in the prompt:

```python
import requests

data_dict = requests.get("https://your-api/data-dictionary").json()

prompt = f"""
You have access to this financial data API:
{data_dict}

User question: What's the TVL trend for Aave?
"""
```

---

## REST API Endpoints

Base URL: `http://localhost:8000` (dev) or your deployed URL

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check |
| `/data-dictionary` | GET | Full schema for LLMs |
| `/sources` | GET | List sources + counts |
| `/entities` | GET | Search entities |
| `/entities/{id}` | GET | Entity details |
| `/metrics` | GET | Available metrics |
| `/latest?metric=X` | GET | Latest values |
| `/time-series` | GET | Historical data |
| `/cross-source` | GET | Multi-source comparison |
| `/stats` | GET | Database stats |

---

## Data Sources

| Source | Data Type | Frequency | Example Assets |
|--------|-----------|-----------|----------------|
| Artemis | Fundamentals (fees, revenue, DAU) | Daily | sol, eth, btc |
| DefiLlama | DeFi metrics (TVL, volume) | Hourly | solana, aave, uniswap |
| Velo | Derivatives (funding, OI) | Hourly | BTC_binance-futures |
| CoinGecko | Market data (price, mcap) | Hourly | bitcoin, ethereum |
| AlphaVantage | Equities (OHLCV) | Hourly | AAPL, MSFT, COIN |

# Alpha Vantage Daily Pull - Setup Guide

## Rate Limit Considerations

Your ticker list has **38 symbols**. Here's what each tier allows:

| Tier | Requests/Day | Requests/Min | Can Pull 38 Tickers? | Est. Time |
|------|--------------|--------------|---------------------|-----------|
| Free | 25 | 5 | ❌ No | N/A |
| 30/min ($49.99/mo) | Unlimited | 30 | ✅ Yes | ~1.5 min |
| 75/min ($99.99/mo) | Unlimited | 75 | ✅ Yes | ~35 sec |
| 150/min ($149.99/mo) | Unlimited | 150 | ✅ Yes | ~20 sec |

**Recommendation**: The $49.99/mo plan (30 req/min) is sufficient for 38 tickers.

## Installation

```bash
# Create directory
mkdir -p ~/alphavantage_pull/data
cd ~/alphavantage_pull

# Copy script
cp alphavantage_daily_pull.py ~/alphavantage_pull/

# Install dependencies
pip install requests psycopg2-binary  # psycopg2 only if using PostgreSQL
```

## Configuration

Edit `alphavantage_daily_pull.py`:

1. **API Key** (line ~24):
   ```python
   API_KEY = "YOUR_PREMIUM_API_KEY"
   ```

2. **Rate Limit** (line ~28) - adjust based on your tier:
   ```python
   REQUESTS_PER_MINUTE = 30  # For $49.99 tier
   ```

3. **PostgreSQL** (optional, lines ~210-215) - uncomment and configure:
   ```python
   save_to_postgres(
       results,
       host="localhost",
       database="your_database",
       user="your_user", 
       password="your_password",
       table="alphavantage_daily"
   )
   ```

## PostgreSQL Table Schema

```sql
CREATE TABLE alphavantage_daily (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12,4),
    high NUMERIC(12,4),
    low NUMERIC(12,4),
    close NUMERIC(12,4),
    volume BIGINT,
    dollar_volume NUMERIC(18,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);

CREATE INDEX idx_alphavantage_daily_symbol ON alphavantage_daily(symbol);
CREATE INDEX idx_alphavantage_daily_date ON alphavantage_daily(date);
```

## Scheduling with Cron

Alpha Vantage updates daily data shortly after market close (4:00 PM ET).

```bash
# Edit crontab
crontab -e

# Add this line (runs 4:05 PM ET = 21:05 UTC during EST)
05 21 * * 1-5 cd ~/alphavantage_pull && python alphavantage_daily_pull.py >> cron.log 2>&1

# For EDT (summer), use 20:05 UTC instead
# Or use a timezone-aware scheduler like systemd timers
```

### Systemd Timer (alternative to cron, timezone-aware)

Create `/etc/systemd/system/alphavantage-pull.service`:
```ini
[Unit]
Description=Alpha Vantage Daily Pull

[Service]
Type=oneshot
WorkingDirectory=/home/youruser/alphavantage_pull
ExecStart=/usr/bin/python3 alphavantage_daily_pull.py
User=youruser
```

Create `/etc/systemd/system/alphavantage-pull.timer`:
```ini
[Unit]
Description=Run Alpha Vantage pull daily at 4:05 PM ET

[Timer]
OnCalendar=*-*-* 16:05:00 America/New_York
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:
```bash
sudo systemctl enable alphavantage-pull.timer
sudo systemctl start alphavantage-pull.timer
```

## Output Files

The script creates:
- `data/alphavantage_daily_YYYYMMDD.csv` - Daily snapshot
- `data/alphavantage_daily_YYYYMMDD.json` - Daily snapshot (JSON)
- `data/alphavantage_daily_master.csv` - Appended historical data
- `alphavantage_pull.log` - Execution log

## Test Run

```bash
cd ~/alphavantage_pull
python alphavantage_daily_pull.py
```

## Ticker List (38 total)

**Crypto/Fintech:**
COIN, HOOD, CRCL, GLXY, SOFI, BLSH, FIGR, GEMI, ETOR, XYZ, PYPL, EXOD, WU, IBKR, BTGO

**ETFs:**
QQQ, SPY

**Bitcoin Treasury/Mining:**
MSTR, FWDI, HSDT, IPST, HYPD, PURR, THAR, GNLN, CIFR, RIOT, WULF, CORZ, MARA, HUT, CLSK, BTDR, IREN

**Other:**
SBET, BMNR, DFDV, HODL, ETHZ

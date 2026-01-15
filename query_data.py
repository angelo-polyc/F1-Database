import csv
from db.setup import get_connection

def get_latest_values(metric_name: str, source: str = None) -> list:
    conn = get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT DISTINCT ON (asset) 
            asset, metric_name, value, pulled_at, source
        FROM metrics
        WHERE metric_name = %s
    """
    params = [metric_name]
    
    if source:
        query += " AND source = %s"
        params.append(source)
    
    query += " ORDER BY asset, pulled_at DESC"
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return [
        {"asset": r[0], "metric_name": r[1], "value": r[2], "pulled_at": r[3], "source": r[4]}
        for r in rows
    ]

def get_time_series(asset: str, metric_name: str, source: str = None, limit: int = 1000) -> list:
    conn = get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT pulled_at, value, source
        FROM metrics
        WHERE asset = %s AND metric_name = %s
    """
    params = [asset, metric_name]
    
    if source:
        query += " AND source = %s"
        params.append(source)
    
    query += " ORDER BY pulled_at DESC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return [
        {"pulled_at": r[0], "value": r[1], "source": r[2]}
        for r in rows
    ]

def export_to_csv(data: list, filename: str):
    if not data:
        print("No data to export")
        return
    
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Exported {len(data)} rows to {filename}")

def list_available_metrics() -> list:
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT DISTINCT metric_name FROM metrics ORDER BY metric_name")
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return [r[0] for r in rows]

def list_available_assets(metric_name: str = None) -> list:
    conn = get_connection()
    cur = conn.cursor()
    
    if metric_name:
        cur.execute("SELECT DISTINCT asset FROM metrics WHERE metric_name = %s ORDER BY asset", (metric_name,))
    else:
        cur.execute("SELECT DISTINCT asset FROM metrics ORDER BY asset")
    
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return [r[0] for r in rows]

def get_pull_history(source_name: str = None, limit: int = 50) -> list:
    conn = get_connection()
    cur = conn.cursor()
    
    query = "SELECT pull_id, source_name, pulled_at, status, records_count FROM pulls"
    params = []
    
    if source_name:
        query += " WHERE source_name = %s"
        params.append(source_name)
    
    query += " ORDER BY pulled_at DESC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return [
        {"pull_id": r[0], "source_name": r[1], "pulled_at": r[2], "status": r[3], "records_count": r[4]}
        for r in rows
    ]

if __name__ == "__main__":
    print("Available metrics:", list_available_metrics())

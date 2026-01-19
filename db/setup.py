import os
import psycopg2
from psycopg2 import sql

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def setup_database():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pulls (
            pull_id SERIAL PRIMARY KEY,
            source_name VARCHAR(100) NOT NULL,
            pulled_at TIMESTAMP NOT NULL DEFAULT NOW(),
            status VARCHAR(50) NOT NULL,
            records_count INTEGER DEFAULT 0
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            pulled_at TIMESTAMP NOT NULL,
            source VARCHAR(100) NOT NULL,
            asset VARCHAR(100) NOT NULL,
            metric_name VARCHAR(200) NOT NULL,
            value DOUBLE PRECISION
        );
    """)
    
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_unique 
        ON metrics (source, asset, metric_name, pulled_at);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Database setup complete.")

if __name__ == "__main__":
    setup_database()

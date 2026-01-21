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
            records_count INTEGER DEFAULT 0,
            notes TEXT
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            pulled_at TIMESTAMP NOT NULL,
            source VARCHAR(100) NOT NULL,
            asset VARCHAR(100) NOT NULL,
            metric_name VARCHAR(200) NOT NULL,
            value DOUBLE PRECISION,
            domain VARCHAR(50),
            exchange VARCHAR(100),
            entity_id INTEGER,
            granularity VARCHAR(20)
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS entities (
            entity_id SERIAL PRIMARY KEY,
            canonical_id VARCHAR(100) UNIQUE NOT NULL,
            name VARCHAR(200),
            symbol VARCHAR(50),
            entity_type VARCHAR(50),
            sector VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS entity_source_ids (
            id SERIAL PRIMARY KEY,
            entity_id INTEGER REFERENCES entities(entity_id),
            source VARCHAR(50) NOT NULL,
            source_id VARCHAR(100) NOT NULL,
            UNIQUE(source, source_id)
        );
    """)
    
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS metrics_unique_with_exchange 
        ON metrics (source, asset, metric_name, pulled_at, COALESCE(exchange, ''));
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_lookup 
        ON metrics (source, asset, metric_name, pulled_at);
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_ts 
        ON metrics (pulled_at);
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_domain 
        ON metrics (domain, pulled_at);
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_entity 
        ON metrics (entity_id, metric_name, pulled_at);
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_exchange 
        ON metrics (exchange, pulled_at) WHERE exchange IS NOT NULL;
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_granularity 
        ON metrics (granularity, pulled_at);
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_entity_source 
        ON entity_source_ids (source, source_id);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Database setup complete.")

if __name__ == "__main__":
    setup_database()

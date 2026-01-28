import os
import psycopg2
from psycopg2 import sql

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_connection(timeout=10):
    return psycopg2.connect(DATABASE_URL, connect_timeout=timeout)

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
            granularity VARCHAR(20),
            metric_date DATE
        );
    """)
    
    # Add metric_date column if it doesn't exist (for existing databases)
    cur.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'metrics' AND column_name = 'metric_date'
            ) THEN
                ALTER TABLE metrics ADD COLUMN metric_date DATE;
            END IF;
        END $$;
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
    
    # Check if entities need seeding
    cur.execute("SELECT COUNT(*) FROM entities")
    result = cur.fetchone()
    entity_count = result[0] if result else 0
    
    if entity_count == 0:
        print("Seeding entity data...")
        seed_entities(conn)
    else:
        print(f"Entities already populated ({entity_count} records)")
    
    # Always create/update views
    print("Creating views...")
    create_views(conn)
    
    cur.close()
    conn.close()
    print("Database setup complete.")

def seed_entities(conn):
    """Load entity seed data from SQL file."""
    import os
    
    seed_file = os.path.join(os.path.dirname(__file__), 'seed_entities.sql')
    
    if not os.path.exists(seed_file):
        print("  Seed file not found, skipping")
        return
    
    cur = conn.cursor()
    
    try:
        with open(seed_file, 'r') as f:
            sql = f.read()
        cur.execute(sql)
        conn.commit()
        print("  Entity seed data loaded successfully")
    except Exception as e:
        print(f"  Error loading seed data: {e}")
        conn.rollback()
    finally:
        cur.close()

def create_views(conn):
    """Create or replace database views."""
    import os
    
    views_file = os.path.join(os.path.dirname(__file__), 'create_views.sql')
    
    if not os.path.exists(views_file):
        print("Views file not found, skipping")
        return
    
    cur = conn.cursor()
    
    try:
        with open(views_file, 'r') as f:
            sql = f.read()
        cur.execute(sql)
        conn.commit()
        print("Views created successfully")
    except Exception as e:
        print(f"Error creating views: {e}")
        conn.rollback()
    finally:
        cur.close()

if __name__ == "__main__":
    setup_database()

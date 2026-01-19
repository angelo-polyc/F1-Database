from abc import ABC, abstractmethod
from datetime import datetime
from db.setup import get_connection

class BaseSource(ABC):
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        pass
    
    @abstractmethod
    def pull(self) -> int:
        pass
    
    def log_pull(self, status: str, records_count: int) -> int:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pulls (source_name, pulled_at, status, records_count)
            VALUES (%s, %s, %s, %s)
            RETURNING pull_id
            """,
            (self.source_name, datetime.utcnow(), status, records_count)
        )
        pull_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return pull_id
    
    def insert_metrics(self, records: list) -> int:
        if not records:
            return 0
        
        conn = get_connection()
        cur = conn.cursor()
        
        insert_query = """
            INSERT INTO metrics (pulled_at, source, asset, metric_name, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source, asset, metric_name, (pulled_at::date)) DO NOTHING
        """
        
        pulled_at = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        rows = [(pulled_at, self.source_name, r["asset"], r["metric_name"], r["value"]) for r in records]
        
        cur.executemany(insert_query, rows)
        conn.commit()
        cur.close()
        conn.close()
        
        return len(rows)

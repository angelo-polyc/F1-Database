#!/usr/bin/env python3
"""
Import entities and mappings into a fresh database.
Run this after publishing to populate the production database with entity master data.

Usage:
    python import_entities.py
"""

import csv
import os
from db.setup import get_connection, setup_database

def import_entities():
    setup_database()
    
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM entities")
    existing = cur.fetchone()[0]
    if existing > 0:
        print(f"Database already has {existing} entities. Skipping import.")
        print("To reimport, truncate entities and entity_source_ids tables first.")
        cur.close()
        conn.close()
        return
    
    entities_file = 'export_entities.csv'
    mappings_file = 'export_entity_source_ids.csv'
    
    if not os.path.exists(entities_file):
        print(f"ERROR: {entities_file} not found. Export from dev first.")
        return
    
    with open(entities_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        entities = list(reader)
    
    print(f"Importing {len(entities)} entities...")
    for e in entities:
        cur.execute("""
            INSERT INTO entities (entity_id, canonical_id, name, symbol, entity_type, sector)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (canonical_id) DO NOTHING
        """, (
            int(e['entity_id']),
            e['canonical_id'],
            e['name'],
            e['symbol'],
            e['entity_type'],
            e['sector'] or None
        ))
    
    cur.execute("SELECT setval('entities_entity_id_seq', (SELECT MAX(entity_id) FROM entities))")
    
    if os.path.exists(mappings_file):
        with open(mappings_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            mappings = list(reader)
        
        print(f"Importing {len(mappings)} source mappings...")
        for m in mappings:
            cur.execute("""
                INSERT INTO entity_source_ids (entity_id, source, source_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (source, source_id) DO NOTHING
            """, (int(m['entity_id']), m['source'], m['source_id']))
    
    conn.commit()
    
    cur.execute("SELECT COUNT(*) FROM entities")
    print(f"Done. {cur.fetchone()[0]} entities in database.")
    
    cur.execute("SELECT source, COUNT(*) FROM entity_source_ids GROUP BY source ORDER BY source")
    print("Mappings by source:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    import_entities()

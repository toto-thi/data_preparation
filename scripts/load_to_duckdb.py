import os
import pandas as pd
import duckdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# DuckDB database file path
db_path = "./animal_insights.duckdb"

datasets = {
    "iucn_species": "data/iucn_species_status.csv",
    "cites_trade": "data/cites_trade.csv",
    "gbif_sightings": "data/gbif_sightings.csv"
}

def load_csvs_to_duckdb():
    try:
        # Connect to DuckDB (will create the file if it doesn't exist)
        conn = duckdb.connect(db_path)
        logging.info(f"Connected to DuckDB at {db_path}")
        
        # Test connection
        conn.execute("SELECT 1")
        logging.info("Connection test passed.")
        
        # Create schema if it doesn't exist
        conn.execute("CREATE SCHEMA IF NOT EXISTS main")
        
        for table, path in datasets.items():
            try:
                logging.info(f"📥 Loading '{path}' into table '{table}'...")
                
                conn.execute(f"""
                    CREATE OR REPLACE TABLE {table} AS 
                    SELECT * FROM read_csv('{path}', auto_detect=true)
                """)
                
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                row_count = result[0] if result else 0
                
                logging.info(f"Loaded {row_count} rows into '{table}'")
            except Exception as e:
                logging.error(f"Error loading '{table}': {e}")
        
        conn.close()
        logging.info("DuckDB connection closed")

    except Exception as e:
        logging.critical(f"Could not connect to DuckDB: {e}")

if __name__ == "__main__":
    load_csvs_to_duckdb()
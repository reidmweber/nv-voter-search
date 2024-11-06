import sqlite3
import pandas as pd
import os

# Database setup - use absolute paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'voters.db')
CSV_PATH = os.path.join(BASE_DIR, 'data', 'voter_status.csv')

def init_db():
    """Initialize database and load data"""
    print(f"Initializing database at {DB_PATH}")
    print(f"Looking for CSV at {CSV_PATH}")
    
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    if not os.path.exists(CSV_PATH):
        alt_csv = os.path.join(BASE_DIR, 'Voter Status File 11-04 1800 CSV.csv')
        if os.path.exists(alt_csv):
            print(f"Copying {alt_csv} to {CSV_PATH}")
            os.system(f'cp "{alt_csv}" "{CSV_PATH}"')
        else:
            raise FileNotFoundError(f"Could not find CSV file at {CSV_PATH} or {alt_csv}")
    
    # Rest of your init_db function remains the same... 
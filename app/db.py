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
    
    print("Creating database...")
    conn = sqlite3.connect(DB_PATH)
    
    # Read CSV and clean column names
    print("Reading CSV file...")
    df = pd.read_csv(CSV_PATH, encoding='latin1', low_memory=False)
    
    # Clean column names
    df.columns = [col.replace('.', '_').replace(' ', '_') for col in df.columns]
    print("Cleaned columns:", df.columns.tolist())
    
    # Create table with dynamic columns plus ELECTION_YEAR
    column_defs = [f'"{col}" TEXT' for col in df.columns]
    column_defs.append('"ELECTION_YEAR" INTEGER')
    create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS voters (
            {', '.join(column_defs)}
        )
    '''
    print("Creating table...")
    conn.execute(create_table_sql)
    
    # Create indexes
    print("Creating indexes...")
    conn.execute('CREATE INDEX IF NOT EXISTS idx_name ON voters(VOTER_NAME)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_city ON voters(CITY)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_party ON voters(VOTER_REG_PARTY)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_precinct ON voters(PRECINCT)')
    
    # Create FTS table
    print("Creating FTS table...")
    searchable_columns = [
        'STATE_VOTERID',
        'VOTER_NAME',
        'STREET_NAME',
        'CITY',
        'ZIP',
        'VOTER_REG_PARTY',
        'PRECINCT',
        'VOTE_LOCATION'
    ]
    fts_columns = [col for col in searchable_columns if col in df.columns]
    fts_create_sql = f'''
        CREATE VIRTUAL TABLE IF NOT EXISTS voters_fts USING fts5(
            {', '.join(f'"{col}"' for col in fts_columns)},
            content='voters',
            content_rowid='rowid'
        )
    '''
    conn.execute(fts_create_sql)
    
    # Load data in chunks
    print("Loading data...")
    chunk_size = 10000
    total_records = 0
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size].copy()
        chunk['ELECTION_YEAR'] = 2024
        chunk.to_sql('voters', conn, if_exists='append', index=False)
        total_records += len(chunk)
        print(f"Loaded {total_records} records...")
    
    # Populate FTS table
    print("Populating search index...")
    fts_insert_sql = f'''
        INSERT INTO voters_fts(rowid, {', '.join(f'"{col}"' for col in fts_columns)})
        SELECT rowid, {', '.join(f'"{col}"' for col in fts_columns)}
        FROM voters
    '''
    conn.execute(fts_insert_sql)
    
    conn.commit()
    conn.close()
    print("Database initialized successfully")
import sqlite3
import pandas as pd
import os
import gc

# Database setup - use absolute paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'voters.db')
CSV_PATH = os.path.join(BASE_DIR, 'data', 'voter_status.csv')

# Define columns we actually need
NEEDED_COLUMNS = [
    'STATE_VOTERID',
    'VOTER_NAME',
    'STREET_NUMBER',
    'STREET_PREDIRECTION',
    'STREET_NAME',
    'STREET_TYPE',
    'UNIT',
    'CITY',
    'STATE',
    'ZIP',
    'VOTER_REG_PARTY',
    'PRECINCT',
    'BALLOT_TYPE',
    'BALLOT_VOTE_METHOD',
    'VOTE_LOCATION',
    'BALLOT_STATUS'
]

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
    
    # Create table with only needed columns
    column_defs = [f'"{col}" TEXT' for col in NEEDED_COLUMNS]
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
    fts_create_sql = f'''
        CREATE VIRTUAL TABLE IF NOT EXISTS voters_fts USING fts5(
            {', '.join(f'"{col}"' for col in searchable_columns)},
            content='voters',
            content_rowid='rowid'
        )
    '''
    conn.execute(fts_create_sql)
    
    # Load data in smaller chunks
    print("Loading data...")
    chunk_size = 5000  # Reduced chunk size
    total_records = 0
    
    # Use chunked reading with only needed columns
    for chunk in pd.read_csv(CSV_PATH, 
                           encoding='latin1',
                           usecols=NEEDED_COLUMNS,
                           chunksize=chunk_size,
                           dtype=str):  # Use string type for all columns to save memory
        
        chunk['ELECTION_YEAR'] = 2024
        chunk.to_sql('voters', conn, if_exists='append', index=False)
        total_records += len(chunk)
        print(f"Loaded {total_records} records...")
        
        # Force garbage collection after each chunk
        gc.collect()
    
    # Populate FTS table in chunks
    print("Populating search index...")
    chunk_size = 5000
    offset = 0
    
    while True:
        # Get a chunk of records
        records = conn.execute(f'''
            SELECT rowid, {', '.join(searchable_columns)}
            FROM voters
            LIMIT {chunk_size} OFFSET {offset}
        ''').fetchall()
        
        if not records:
            break
            
        # Insert into FTS table
        conn.executemany(f'''
            INSERT INTO voters_fts(rowid, {', '.join(searchable_columns)})
            VALUES ({', '.join(['?' for _ in range(len(searchable_columns) + 1)])})
        ''', records)
        
        offset += chunk_size
        print(f"Indexed {offset} records...")
        gc.collect()
    
    conn.commit()
    conn.close()
    print("Database initialized successfully")
import sqlite3
import pandas as pd
import os
import gc
import csv

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

def init_db(csv_path=None):
    """Initialize database and load data"""
    global CSV_PATH
    
    if csv_path:
        CSV_PATH = csv_path
    
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
    
    # Use a temporary database file during initialization
    temp_db = f"{DB_PATH}.temp"
    if os.path.exists(temp_db):
        os.remove(temp_db)
    
    print("Creating temporary database...")
    conn = sqlite3.connect(temp_db)
    
    try:
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
        
        # Import data directly
        print("Loading data...")
        with open(CSV_PATH, 'r', encoding='latin1') as f:
            # Read header
            header = next(csv.reader([f.readline()]))
            col_indices = [header.index(col) for col in NEEDED_COLUMNS]
            
            # Prepare insert statement
            insert_sql = f'''
                INSERT INTO voters ({', '.join(f'"{col}"' for col in NEEDED_COLUMNS)}, ELECTION_YEAR)
                VALUES ({','.join(['?' for _ in range(len(NEEDED_COLUMNS) + 1)])})
            '''
            
            batch = []
            batch_size = 1000
            total_records = 0
            
            for line in f:
                row = next(csv.reader([line]))
                values = [row[i] for i in col_indices]
                values.append(2024)  # Add ELECTION_YEAR
                batch.append(values)
                
                if len(batch) >= batch_size:
                    conn.executemany(insert_sql, batch)
                    conn.commit()  # Commit each batch
                    total_records += len(batch)
                    print(f"Loaded {total_records} records...")
                    batch = []
                    gc.collect()
            
            # Insert remaining records
            if batch:
                conn.executemany(insert_sql, batch)
                conn.commit()
                total_records += len(batch)
                print(f"Loaded {total_records} records...")
        
        # Create indexes
        print("Creating indexes...")
        conn.execute('CREATE INDEX IF NOT EXISTS idx_name ON voters(VOTER_NAME)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_city ON voters(CITY)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_party ON voters(VOTER_REG_PARTY)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_precinct ON voters(PRECINCT)')
        conn.commit()
        
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
        
        # Populate FTS table in chunks
        print("Populating search index...")
        chunk_size = 1000
        offset = 0
        
        while True:
            records = conn.execute(f'''
                SELECT rowid, {', '.join(searchable_columns)}
                FROM voters
                LIMIT {chunk_size} OFFSET {offset}
            ''').fetchall()
            
            if not records:
                break
            
            conn.executemany(f'''
                INSERT INTO voters_fts(rowid, {', '.join(searchable_columns)})
                VALUES ({', '.join(['?' for _ in range(len(searchable_columns) + 1)])})
            ''', records)
            
            offset += chunk_size
            print(f"Indexed {offset} records...")
            conn.commit()  # Commit each batch of FTS records
            gc.collect()
        
        conn.commit()
        conn.close()
        
        # Replace the old database with the new one
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        os.rename(temp_db, DB_PATH)
        
        print("Database initialized successfully")
        
    except Exception as e:
        print(f"Error during initialization: {str(e)}")
        conn.close()
        if os.path.exists(temp_db):
            os.remove(temp_db)
        raise
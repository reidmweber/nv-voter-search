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

def init_db(csv_path=None, format_type='standard'):
    """Initialize database and create tables"""
    global CSV_PATH
    
    if csv_path:
        CSV_PATH = csv_path
    
    print(f"Initializing database at {DB_PATH}")
    
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    
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
        
        # Create FTS table
        print("Creating FTS table...")
        conn.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS voters_fts USING fts5(
                STATE_VOTERID,
                VOTER_NAME,
                STREET_NUMBER,
                STREET_NAME,
                CITY,
                ZIP,
                VOTER_REG_PARTY,
                PRECINCT,
                content='voters',
                content_rowid='rowid'
            )
        ''')
        
        # Create triggers to keep FTS table in sync
        print("Creating FTS triggers...")
        conn.executescript('''
            CREATE TRIGGER IF NOT EXISTS voters_ai AFTER INSERT ON voters BEGIN
                INSERT INTO voters_fts(
                    rowid,
                    STATE_VOTERID,
                    VOTER_NAME,
                    STREET_NUMBER,
                    STREET_NAME,
                    CITY,
                    ZIP,
                    VOTER_REG_PARTY,
                    PRECINCT
                ) VALUES (
                    new.rowid,
                    new.STATE_VOTERID,
                    new.VOTER_NAME,
                    new.STREET_NUMBER,
                    new.STREET_NAME,
                    new.CITY,
                    new.ZIP,
                    new.VOTER_REG_PARTY,
                    new.PRECINCT
                );
            END;
            
            CREATE TRIGGER IF NOT EXISTS voters_ad AFTER DELETE ON voters BEGIN
                INSERT INTO voters_fts(voters_fts, rowid, STATE_VOTERID, VOTER_NAME, STREET_NUMBER, STREET_NAME, CITY, ZIP, VOTER_REG_PARTY, PRECINCT)
                VALUES('delete', old.rowid, old.STATE_VOTERID, old.VOTER_NAME, old.STREET_NUMBER, old.STREET_NAME, old.CITY, old.ZIP, old.VOTER_REG_PARTY, old.PRECINCT);
            END;
            
            CREATE TRIGGER IF NOT EXISTS voters_au AFTER UPDATE ON voters BEGIN
                INSERT INTO voters_fts(voters_fts, rowid, STATE_VOTERID, VOTER_NAME, STREET_NUMBER, STREET_NAME, CITY, ZIP, VOTER_REG_PARTY, PRECINCT)
                VALUES('delete', old.rowid, old.STATE_VOTERID, old.VOTER_NAME, old.STREET_NUMBER, old.STREET_NAME, old.CITY, old.ZIP, old.VOTER_REG_PARTY, old.PRECINCT);
                INSERT INTO voters_fts(rowid, STATE_VOTERID, VOTER_NAME, STREET_NUMBER, STREET_NAME, CITY, ZIP, VOTER_REG_PARTY, PRECINCT)
                VALUES (new.rowid, new.STATE_VOTERID, new.VOTER_NAME, new.STREET_NUMBER, new.STREET_NAME, new.CITY, new.ZIP, new.VOTER_REG_PARTY, new.PRECINCT);
            END;
        ''')
        
        if csv_path:
            import_data(csv_path, format_type)
            
    finally:
        conn.close()

def import_data(csv_path, format_type='standard'):
    """Import data from CSV file into existing database"""
    print(f"Importing data from {csv_path}")
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        with open(csv_path, 'r', encoding='latin1') as f:
            # Read header
            header = next(csv.reader([f.readline()]))
            
            # Define column mappings based on format
            if format_type == 'ev':
                column_mapping = {
                    'STATE_VOTERID': header.index('IDNUMBER'),
                    'VOTER_NAME': header.index('NAME'),
                    'PRECINCT': header.index('PRECINCT'),
                    'VOTER_REG_PARTY': header.index('PARTY'),
                    'CITY': header.index('CITY'),
                    'VOTE_LOCATION': header.index('EV SITE'),
                    'BALLOT_STATUS': header.index('STATUS')
                }
                # Set default values for unmapped columns
                default_values = {
                    'STREET_NUMBER': '',
                    'STREET_PREDIRECTION': '',
                    'STREET_NAME': '',
                    'STREET_TYPE': '',
                    'UNIT': '',
                    'STATE': 'NV',
                    'ZIP': '',
                    'BALLOT_TYPE': 'EV',
                    'BALLOT_VOTE_METHOD': 'Early Voting'
                }
            else:
                # Original format mapping
                column_mapping = {col: header.index(col) for col in NEEDED_COLUMNS if col in header}
                default_values = {}
            
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
                values = []
                
                # Build values list using mapping and defaults
                for col in NEEDED_COLUMNS:
                    if col in column_mapping:
                        values.append(row[column_mapping[col]])
                    else:
                        values.append(default_values.get(col, ''))
                
                values.append(2024)  # Add ELECTION_YEAR
                batch.append(values)
                
                if len(batch) >= batch_size:
                    conn.executemany(insert_sql, batch)
                    conn.commit()
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
            
            print("Creating indexes...")
            conn.execute('CREATE INDEX IF NOT EXISTS idx_name ON voters(VOTER_NAME)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_city ON voters(CITY)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_party ON voters(VOTER_REG_PARTY)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_precinct ON voters(PRECINCT)')
            conn.commit()
            
            print("Database updated successfully")
            
    except Exception as e:
        print(f"Error during import: {str(e)}")
        raise
    finally:
        conn.close()
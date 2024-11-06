import os
import sqlite3

def init_db():
    # Create the data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Database path
    db_path = os.path.join('data', 'voters.db')
    
    # Create and initialize the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create your tables here
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS voters (
            # Add your table schema here
            # For example:
            id INTEGER PRIMARY KEY,
            name TEXT,
            address TEXT
            # ... other fields
        )
    ''')
    
    # Add any initial data if needed
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {db_path}")

if __name__ == '__main__':
    init_db() 
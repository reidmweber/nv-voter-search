from flask import Flask, render_template, jsonify, request
import sqlite3
import pandas as pd
import os
from flask_cors import CORS
from .db import DB_PATH, init_db  # Import DB_PATH and init_db from db.py

def create_app():
    app = Flask(__name__)
    CORS(app)
    
    print(f"Using database at: {DB_PATH}")
    
    def get_db():
        try:
            if not os.path.exists(DB_PATH):
                print(f"Database not found at {DB_PATH}, initializing...")
                init_db()
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            print(f"Error connecting to database: {str(e)}")
            raise

    # Replace before_first_request with middleware
    @app.before_request
    def check_database():
        if not hasattr(app, '_database_checked'):
            try:
                print("Checking database on first request...")
                conn = get_db()
                cursor = conn.cursor()
                count = cursor.execute('SELECT COUNT(*) FROM voters').fetchone()[0]
                print(f"Database contains {count} records")
                conn.close()
                app._database_checked = True
            except Exception as e:
                print(f"Error checking database: {str(e)}")
                print("Attempting to initialize database...")
                init_db()
                app._database_checked = True

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/data')
    def get_data():
        search = request.args.get('search[value]', '')
        start = request.args.get('start', type=int, default=0)
        length = request.args.get('length', type=int, default=25)
        
        conn = get_db()
        cursor = conn.cursor()
        
        if search:
            # Use FTS for searching
            query = '''
                SELECT v.* FROM voters v
                JOIN voters_fts fts ON v.rowid = fts.rowid
                WHERE voters_fts MATCH ?
                LIMIT ? OFFSET ?
            '''
            count_query = '''
                SELECT COUNT(*) FROM voters v
                JOIN voters_fts fts ON v.rowid = fts.rowid
                WHERE voters_fts MATCH ?
            '''
            params = (search, length, start)
            count_params = (search,)
        else:
            query = 'SELECT * FROM voters LIMIT ? OFFSET ?'
            count_query = 'SELECT COUNT(*) FROM voters'
            params = (length, start)
            count_params = ()
        
        # Get total records
        total_records = cursor.execute('SELECT COUNT(*) FROM voters').fetchone()[0]
        
        # Get filtered records
        filtered_records = cursor.execute(count_query, count_params).fetchone()[0]
        
        # Get page of records
        records = cursor.execute(query, params).fetchall()
        
        return jsonify({
            'draw': request.args.get('draw', type=int, default=1),
            'recordsTotal': total_records,
            'recordsFiltered': filtered_records,
            'data': [dict(r) for r in records]
        })

    @app.route('/stats')
    def get_stats():
        conn = get_db()
        cursor = conn.cursor()
        
        stats = {}
        
        # City counts
        stats['city_counts'] = dict(cursor.execute('''
            SELECT CITY, COUNT(*) as count 
            FROM voters 
            GROUP BY CITY 
            ORDER BY count DESC 
            LIMIT 10
        ''').fetchall())
        
        # Party counts
        stats['party_counts'] = dict(cursor.execute('''
            SELECT VOTER_REG_PARTY, COUNT(*) as count 
            FROM voters 
            WHERE VOTER_REG_PARTY IS NOT NULL
            GROUP BY VOTER_REG_PARTY 
            ORDER BY count DESC
        ''').fetchall())
        
        # Precinct counts
        stats['precinct_counts'] = dict(cursor.execute('''
            SELECT PRECINCT, COUNT(*) as count 
            FROM voters 
            GROUP BY PRECINCT 
            ORDER BY count DESC 
            LIMIT 10
        ''').fetchall())
        
        # Ballot status counts
        stats['ballot_status_counts'] = dict(cursor.execute('''
            SELECT COALESCE(BALLOT_STATUS, 'Unknown') as status, COUNT(*) as count 
            FROM voters 
            GROUP BY BALLOT_STATUS 
            ORDER BY count DESC
        ''').fetchall())
        
        # Vote method counts
        stats['vote_method_counts'] = dict(cursor.execute('''
            SELECT COALESCE(BALLOT_VOTE_METHOD, 'Unknown') as method, COUNT(*) as count 
            FROM voters 
            GROUP BY BALLOT_VOTE_METHOD 
            ORDER BY count DESC
        ''').fetchall())
        
        # Ballot type counts
        stats['ballot_type_counts'] = dict(cursor.execute('''
            SELECT COALESCE(BALLOT_TYPE, 'Unknown') as type, COUNT(*) as count 
            FROM voters 
            GROUP BY BALLOT_TYPE 
            ORDER BY count DESC
        ''').fetchall())
        
        conn.close()
        return jsonify(stats)

    return app

# Create the application instance
app = create_app()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import pandas as pd
import numpy as np
import os
import requests
import io
import time

app = Flask(__name__)
CORS(app)

# Data source URLs - in order of preference
DATA_SOURCES = [
    {
        'name': 'Local File',
        'path': 'data/voter_status.csv'
    },
    {
        'name': 'GitHub LFS',
        'url': os.getenv('GITHUB_LFS_URL', '')
    },
    {
        'name': 'NVSOS',
        'url': 'https://www.nvsos.gov/sos/home/showpublisheddocument/15245/638663411283100000'
    }
]

def load_data():
    """Try multiple data sources to load the CSV with memory optimization"""
    last_error = None
    
    for source in DATA_SOURCES:
        try:
            print(f"Attempting to load data from {source['name']}...")
            
            # Define dtypes to reduce memory usage
            dtypes = {
                'STATE_VOTERID': 'string',
                'VOTER_NAME': 'string',
                'STREET_NUMBER': 'string',
                'STREET_PREDIRECTION': 'string',
                'STREET_NAME': 'string',
                'STREET_TYPE': 'string',
                'UNIT': 'string',
                'CITY': 'string',
                'STATE': 'string',
                'ZIP': 'string',
                'VOTER_REG_PARTY': 'category',
                'BALLOT_PARTY': 'category',
                'ELECTION_CODE': 'category',
                'PRECINCT': 'string',
                'BALLOT_TYPE': 'category',
                'BALLOT_VOTE_METHOD': 'category',
                'VOTE_LOCATION': 'string',
                'BALLOT_STATUS': 'category'
            }
            
            if 'url' in source:
                # Skip empty URLs
                if not source['url']:
                    continue
                    
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/csv,application/octet-stream'
                }
                response = requests.get(source['url'], headers=headers, timeout=30)
                response.raise_for_status()
                
                if b'<!DOCTYPE html>' in response.content[:100]:
                    raise ValueError("Received HTML instead of CSV")
                
                # Read CSV in chunks to reduce memory usage
                chunks = pd.read_csv(
                    io.StringIO(response.content.decode('latin1')),
                    encoding='latin1',
                    dtype=dtypes,
                    chunksize=10000,  # Process in chunks of 10,000 rows
                    usecols=list(dtypes.keys())  # Only load needed columns
                )
                df = pd.concat(chunks, ignore_index=True)
                
            else:
                if not os.path.exists(source['path']):
                    continue
                    
                # Read CSV in chunks to reduce memory usage
                chunks = pd.read_csv(
                    source['path'],
                    encoding='latin1',
                    dtype=dtypes,
                    chunksize=10000,  # Process in chunks of 10,000 rows
                    usecols=list(dtypes.keys())  # Only load needed columns
                )
                df = pd.concat(chunks, ignore_index=True)
            
            if len(df) == 0:
                continue
                
            print(f"Successfully loaded {len(df)} records from {source['name']}")
            print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
            
            return df
            
        except Exception as e:
            print(f"Error loading from {source['name']}: {str(e)}")
            last_error = e
            continue
    
    raise Exception(f"Failed to load data from any source. Last error: {str(last_error)}")

# Load the data when the app starts
print("Initializing application...")
retry_count = 0
max_retries = 3
retry_delay = 5  # seconds

while retry_count < max_retries:
    try:
        df = load_data()
        break
    except Exception as e:
        retry_count += 1
        if retry_count == max_retries:
            print(f"Failed to load data after {max_retries} attempts. Last error: {str(e)}")
            raise
        print(f"Attempt {retry_count} failed. Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)

def clean_dataframe(df):
    # Replace NaN, NaT, and inf values with None
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)
    # Convert float columns that should be integers to int, then to string
    float_cols = df.select_dtypes(include=['float64']).columns
    for col in float_cols:
        df[col] = df[col].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
    return df

def search_dataframe(df, search_value):
    """Optimized search function with smarter name matching"""
    if not search_value:
        return df
    
    try:
        # Convert search value to lowercase and split into terms
        search_terms = search_value.lower().split()
        
        # If we have multiple terms, assume it might be a name search
        if len(search_terms) > 1:
            # Create name mask for exact matches first
            names = df['VOTER_NAME'].astype(str).str.lower()
            
            # Create masks for different matching strategies
            exact_match_mask = names.str.contains(' '.join(search_terms), na=False, regex=False)
            
            # Create a mask that requires all terms to be present in the name
            all_terms_mask = pd.Series(True, index=df.index)
            for term in search_terms:
                all_terms_mask &= names.str.contains(term, na=False, regex=False)
            
            # Combine masks with priority to exact matches
            name_mask = exact_match_mask | all_terms_mask
            
            # If we found any matches with the name search, return those results
            if name_mask.any():
                return df[name_mask]
        
        # If no name matches found or single term search, fall back to general search
        mask = pd.Series(False, index=df.index)
        
        # Define searchable columns
        searchable_columns = [
            'STATE_VOTERID', 'VOTER_NAME', 'STREET_NAME', 'CITY', 
            'ZIP', 'VOTER_REG_PARTY', 'PRECINCT', 'VOTE_LOCATION'
        ]
        
        # Search each column for each term
        for col in searchable_columns:
            if col in df.columns:
                col_values = df[col].astype(str).str.lower()
                term_mask = pd.Series(True, index=df.index)
                for term in search_terms:
                    term_mask &= col_values.str.contains(term, na=False, regex=False)
                mask |= term_mask
        
        return df[mask]
    except Exception as e:
        print(f"Search error: {str(e)}")
        return df

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def get_data():
    try:
        # Get parameters from DataTables
        draw = request.args.get('draw', type=int, default=1)
        start = request.args.get('start', type=int, default=0)
        length = request.args.get('length', type=int, default=25)
        search_value = request.args.get('search[value]', type=str, default='')
        
        # Create a filtered dataframe
        filtered_df = search_dataframe(df, search_value)
        
        # Get total record count
        total_records = len(df)
        total_filtered = len(filtered_df)
        
        # Apply pagination
        paginated_df = filtered_df.iloc[start:start + length]
        
        # Clean the data for JSON serialization
        cleaned_df = clean_dataframe(paginated_df)
        
        # Convert to records
        records = cleaned_df.to_dict('records')
        
        # Prepare the response
        response = {
            'draw': draw,
            'recordsTotal': total_records,
            'recordsFiltered': total_filtered,
            'data': records
        }
        
        print(f"Sending response with {len(records)} records for search: '{search_value}'")
        return jsonify(response)
    except Exception as e:
        print(f"Error in /data route: {str(e)}")
        return jsonify({
            "draw": draw,
            "recordsTotal": 0,
            "recordsFiltered": 0,
            "data": [],
            "error": str(e)
        }), 200  # Return 200 even with error to prevent DataTables from retrying

@app.route('/stats')
def get_stats():
    try:
        stats = {
            'city_counts': df['CITY'].value_counts().head(10).to_dict(),
            'party_counts': df['VOTER_REG_PARTY'].value_counts().to_dict(),
            'precinct_counts': df['PRECINCT'].value_counts().head(10).to_dict(),
            'ballot_status_counts': df['BALLOT_STATUS'].value_counts().to_dict(),
            'vote_method_counts': df['BALLOT_VOTE_METHOD'].value_counts().to_dict(),
            'ballot_type_counts': df['BALLOT_TYPE'].value_counts().to_dict()
        }
        return jsonify(stats)
    except Exception as e:
        print(f"Error in /stats route: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
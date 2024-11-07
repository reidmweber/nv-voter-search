from app.db import init_db
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Initialize the voters database')
    parser.add_argument('--csv', help='Path to the CSV file to import')
    args = parser.parse_args()

    try:
        print("Starting database initialization...")
        init_db(csv_path=args.csv)
        print("Database initialization complete")
        sys.exit(0)
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
from db import init_db
import sys

if __name__ == "__main__":
    try:
        print("Starting database initialization...")
        init_db()
        print("Database initialization complete")
        sys.exit(0)
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        sys.exit(1) 
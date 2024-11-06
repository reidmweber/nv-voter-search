import click
import os
from app import init_db, DB_PATH

@click.group()
def cli():
    pass

@cli.command()
def init():
    """Initialize the database"""
    if os.path.exists(DB_PATH):
        click.confirm(f'Database already exists at {DB_PATH}. Delete and recreate?', abort=True)
        os.remove(DB_PATH)
    init_db()

@cli.command()
def reset():
    """Reset the database"""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()

@cli.command()
def force_init():
    """Force database initialization"""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()

if __name__ == '__main__':
    cli() 
import click
import os
from app.db import init_db, DB_PATH
from app.db_utils import download_from_gdrive, upload_to_gdrive

@click.group()
def cli():
    pass

@cli.command()
@click.option('--csv', help='Path to the CSV file to import')
def init(csv):
    """Initialize the database from CSV"""
    if os.path.exists(DB_PATH):
        click.confirm(f'Database exists at {DB_PATH}. Delete and recreate?', abort=True)
        os.remove(DB_PATH)
    init_db(csv_path=csv)

@cli.command()
def download():
    """Download database from Google Drive"""
    if os.path.exists(DB_PATH):
        click.confirm(f'Database exists at {DB_PATH}. Overwrite?', abort=True)
    download_from_gdrive()

@cli.command()
def upload():
    """Upload database to Google Drive"""
    if not os.path.exists(DB_PATH):
        click.echo(f'No database found at {DB_PATH}')
        return
    click.confirm('Upload database to Google Drive?', abort=True)
    upload_to_gdrive()

if __name__ == '__main__':
    cli() 
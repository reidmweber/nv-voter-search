import click
import os
from app.db import init_db, import_data, DB_PATH
from app.db_utils import download_from_gdrive, upload_to_gdrive

@click.group()
def cli():
    pass

@cli.command()
@click.option('--csv', help='Path to the CSV file to import')
@click.option('--format-type', default='standard', type=click.Choice(['standard', 'ev']), 
              help='Format of the CSV file (standard or ev)')
@click.option('--append/--no-append', default=False, 
              help='Append to existing database instead of recreating')
def init(csv, format_type, append):
    """Initialize or update the database from CSV"""
    if os.path.exists(DB_PATH) and not append:
        click.confirm(f'Database exists at {DB_PATH}. Delete and recreate?', abort=True)
        os.remove(DB_PATH)
        init_db(csv_path=csv, format_type=format_type)
    else:
        if not os.path.exists(DB_PATH):
            init_db(csv_path=csv, format_type=format_type)
        else:
            import_data(csv_path=csv, format_type=format_type)

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
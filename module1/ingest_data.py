import os
import sys
import gzip
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import typer

def main(
    user: str = typer.Option(os.getenv('USER'), help="Username for Postgres."),
    password: str = typer.Option(os.getenv('PASSWORD'), help="Password to the username for Postgres."),
    host: str = typer.Option(os.getenv('HOST'), help="Hostname for Postgres."),
    port: str = typer.Option(os.getenv('PORT'), help="Port for Postgres connection."),
    db: str = typer.Option(os.getenv('DB'), help="Database name for Postgres"),
    table_name: str = typer.Option(os.getenv('TABLE_NAME'), help="Destination table name for Postgres."),
    url: str = typer.Option(os.getenv('URL'), help="URL for .parquet, .csv, or .csv.gz file.")
):
    # Get the name of the file from url
    file_name = url.rsplit('/', 1)[-1].strip()
    print(f'Downloading {file_name} ...')
    
    # Download file from url
    os.system(f'curl -L {url.strip()} -o {file_name}')
    print('\n')

    # Create SQL engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read file based on csv, csv.gz or parquet
    if file_name.endswith(('.csv','.csv.gz')):
        df = pd.read_csv(file_name, nrows=10)
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
    elif file_name.endswith('.parquet'):
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)
    else:
        print('Error. Only .csv, .csv.gz, or .parquet files allowed.')
        sys.exit(1)

    # Create the table
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert values
    t_start = time()
    count = 0
    for batch in df_iter:
        count += 1
        if file_name.endswith('.parquet'):
            batch_df = batch.to_pandas()
        else:
            batch_df = batch
        print(f'inserting batch {count}...')
        b_start = time()
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        b_end = time()
        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')

    t_end = time()
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')

if __name__ == '__main__':
    typer.run(main)
import argparse
import gzip
import pandas as pd
from sqlalchemy import create_engine
import os 
import pyarrow.parquet as pq
import sys
from time import time

def main(input_file, db_host, db_port, db_name, db_user, db_password):
    if os.path.exists(input_file):
        print(f"{input_file} exists.")
    else:
        print(f"{input_file} does not exist.")
        sys.exit(1)
    
    # Read the parquet file
    if input_file.endswith('.csv'):
        df = pd.read_csv(input_file, nrows=10)
        df_iter = pd.read_csv(input_file, iterator=True, chunksize=100000)
    elif input_file.endswith('.csv.gz'):
        with gzip.open(input_file, 'rt') as f:
            df = pd.read_csv(f, nrows=10)
        df_iter = pd.read_csv(input_file, compression='gzip', iterator=True, chunksize=100000)
    elif input_file.endswith('.parquet'):
        file = pq.ParquetFile(input_file)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)
    else:
        print('Error. Only .csv, .csv.gz, or .parquet files allowed.')
        sys.exit(1)
    
    table_name = 'yellow_taxi_data'
    # Create a database connection
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    t_start = time()
    count = 0
    for batch in df_iter:
        count += 1
        if input_file.endswith('.parquet'):
            batch_df = batch.to_pandas()
        else:
            batch_df = batch
        print(f'inserting batch {count}...')
        b_start = time()
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        b_end = time()
        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')
        break # TODO: Only for test

    t_end = time()
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to PostgreSQL')
    parser.add_argument('--input_file', required=True, help='Input Parquet file path')
    parser.add_argument('--db_host', required=True, help='Database host')
    parser.add_argument('--db_port', required=True, help='Database port')
    parser.add_argument('--db_name', required=True, help='Database name')
    parser.add_argument('--db_user', required=True, help='Database user')
    parser.add_argument('--db_password', required=True, help='Database password')

    args = parser.parse_args()

    main(args.input_file, args.db_host, args.db_port, args.db_name, args.db_user, args.db_password)
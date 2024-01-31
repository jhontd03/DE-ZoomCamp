import argparse
import os
from time import time

import pandas as pd

from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name  = 'output.parquet'
    csv_name = 'output.csv'

    os.system(f'wget --no-check-certificate {url} -O {parquet_name}')

    df = pd.read_parquet(parquet_name)
    df.to_csv(csv_name, index=None)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    engine = create_engine(f'postgresql://root:root@pg-database:{int(port)}/ny_taxi')
    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{int(port)}/{db}')
    engine.connect()
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    run = True
    try:
        while True:
            t_start = time()
            df = next(df_iter)
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])                
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time() 
            print(f'inserted another chunk..., took {t_end-t_start: .2f} seconds')
    except Exception:
        run = False        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV Data to Postgres")
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for posgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table to write the taxi data to')
    parser.add_argument('--url', help='url of the parquet file')
    args = parser.parse_args()
    main(args)
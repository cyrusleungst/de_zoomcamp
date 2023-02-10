import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df = pd.read_csv(csv_name)    

    return df

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_data(df):
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    miss_count = df['passenger_count'].isin([0]).sum()
    print(f"pre: missing passenger count: {miss_count}")
    df = df[df['passenger_count'] != 0]
    miss_count = df['passenger_count'].isin([0]).sum()
    print(f"post: missing passenger count: {miss_count}")

    return df

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):

    connection_block = SqlAlchemyConnector.load("postgres-connector")

    with connection_block.get_connection(begin=False) as engine:
        df.to_sql(name=table_name, con=engine, chunksize=100000, if_exists='append')

@flow(name="Ingest Flow")
def main_flow():
    table_name = "yellow_taxi_trips"
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    
    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)


if __name__ == '__main__':
    main_flow()
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from pathlib import Path
# from prefect.tasks.database import PostgresQuery
# from prefect.tasks.database import PostgresExecute
# from prefect.schedules import IntervalSchedule
from prefect.server.schemas.schedules import CronSchedule

import pyarrow as pa
import pyarrow.parquet as pq
import argparse

from sqlalchemy import create_engine

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues and perform transformations"""
    # Remove rows where passenger count is equal to 0 or trip distance is equal to zero
    # df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date
    df["lpep_pickup_date"] = pd.to_datetime(df["lpep_pickup_datetime"]).dt.date

    # Rename columns in Camel Case to Snake Case
    df = df.rename(columns={
        'VendorID': 'vendor_id',
        'RatecodeID': 'ratecode_id',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id'
    })

    return df

@task()
def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Perform assertions on the DataFrame"""
    # assert df['vendor_id'].isin([1, 2]).all(), "vendor_id should have valid values"
    # assert (df['passenger_count'] > 0).all(), "passenger_count should be greater than 0"
    # assert (df['trip_distance'] > 0).all(), "trip_distance should be greater than 0"
    return df

@task()
def export_to_postgres(df: pd.DataFrame, connection_string: str, table_name: str) -> None:
    """Write the DataFrame to a Postgres table"""
    # query = f"""
    #     DROP TABLE IF EXISTS mage.{table_name};
    #     CREATE TABLE mage.{table_name} AS SELECT * FROM mage.{table_name} LIMIT 0;
    # """
    engine = create_engine(connection_string)

    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    # PostgresExecute(connection_string=connection_string, query=query).run()

    # Assuming you have a schema 'mage' and you want to create a new table
    # df.to_sql(table_name, connection_string, schema='mage', if_exists='replace', index=False)
    return

@task()
def export_to_gcs(df: pd.DataFrame, bucket_name: str, partition_column: str) -> None:
    """Write the DataFrame to GCP as Parquet files partitioned by lpep_pickup_date"""
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path=f'gs://{bucket_name}/parquet_data', partition_cols=[partition_column])
    return

# @flow(name="green_taxi_etl", schedule=CronSchedule(cron="0 5 * * *"))
@flow
def etl_web_to_gcs_postgres(year: int, months_list: list, color: str,
                             pg_connection_string: str, pg_table_name: str,
                             gcs_bucket_name: str, partition_column: str) -> None:
    """The main ETL function"""
    df_all = pd.DataFrame()  

    for month in months_list:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

        df = fetch(dataset_url)
        df_clean = clean(df)
        df_validated = validate(df_clean)
        df_all = pd.concat([df_all, df_validated], ignore_index=True)  # Concatenate the DataFrames

    dataset_file_for_list = f"{color}_tripdata_{year}-{'_'.join(map(str, months_list))}"
    print(dataset_file_for_list)

    # Export to Postgres
    export_to_postgres(df_all, pg_connection_string, pg_table_name)

    # Export to GCS as Parquet files partitioned by lpep_pickup_date
    export_to_gcs(df_all, gcs_bucket_name, partition_column)

if __name__ == "__main__":
    color = "green"
    months = [10, 11, 12]
    year = 2020

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    pg_connection_string = f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}'
    pg_table_name = args.table_name
    gcs_bucket_name = "dtc_data_lake_swift-emblem-402612"
    partition_column = "lpep_pickup_date"

    etl_web_to_gcs_postgres(year, months, color, pg_connection_string, pg_table_name, gcs_bucket_name, partition_column)

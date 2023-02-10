from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(colour: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_path = f"data/{colour}/{colour}_tripdata_{year}-{month:02}.parquet"
    
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../")

    return Path(f"../{gcs_path}")

@task(retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to Big Query"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="trips_data_all.yellow",
        project_id="de-zoomcamp-377022",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )

@flow(name="ETL gcs to bq")
def etl_gcs_to_bq(colour: str, year: int, month: int) -> int:
    """Main ETL flow to load data into Big Query from GCS"""
    path = extract_from_gcs(colour, year, month)
    df = pd.read_parquet(path)
    write_bq(df)

    return len(df)

@flow(log_prints=True)
def etl_parent_flow(months: list[str] = [1, 2], year: int = 2021, colour: str = "yellow"):
    rows_processed=0
    for month in months:
        num_rows = etl_gcs_to_bq(colour, year, month)
        rows_processed+=num_rows

    print(f"Total rows processed: {rows_processed}")

if __name__ == "__main__":
    colour="yellow"
    months=[2, 3]
    year=2019
    etl_parent_flow(months, year, colour)
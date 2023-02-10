from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)

    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    print(df.head(2))
    print(f"Columns: {df.dtypes}")
    print(f"Rows: {len(df)}")

    return df

@task()
def write_local(df: pd.DataFrame, colour: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"/Users/cyrus/development/de_zoomcamp/week_2/data/{colour}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    
    return path

@task()
def write_gcs(path: Path, gcs_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=gcs_path)


@flow(name="ETL web to GCS")
def etl_web_to_gcs(colour: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"{colour}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, colour, dataset_file)
    gcs_path = Path(f"data/{colour}/{dataset_file}.parquet")
    write_gcs(path, gcs_path)

@flow()
def etl_parent_flow(months: list[str] = [1, 2], year: int = 2021, colour: str = "yellow") -> None:
    for month in months:
        etl_web_to_gcs(colour, year, month)

if __name__ == '__main__':
    colour="green"
    months=[1]
    year=2020
    etl_parent_flow(months, year, colour)

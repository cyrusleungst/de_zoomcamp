from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    print(f"Fetching: {dataset_url}")

    df = pd.read_csv(dataset_url, engine="pyarrow")
    print(f"Columns: {df.dtypes}")

    return df


@task(log_prints=True)
def fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print("Fixing dtypes")

    return df.astype({
        'PUlocationID': 'Int64',
        'DOlocationID': 'Int64',
        'SR_Flag': 'Int64'
    })


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as gzipped csv file"""
    path = Path(f"/Users/cyrus/development/de_zoomcamp/week_3/data/fhv/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    
    return path


@task()
def write_gcs(path: Path, gcs_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=gcs_path)


@flow(name="ETL web to GCS")
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = fix_dtypes(df)
    path = write_local(df_clean, dataset_file)
    gcs_path = Path(f"data/fhv/{dataset_file}.csv.gz")
    write_gcs(path, gcs_path)

@flow(name="FHV data to GCS")
def etl_parent_flow(months: list[str] = [1, 2], year: int = 2021) -> None:
    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == '__main__':
    months=list(range(1, 13))
    year=2019
    etl_parent_flow(months, year)

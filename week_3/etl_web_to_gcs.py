from pathlib import Path
import pandas as pd
import os
import yaml
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
def fix_dtypes(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Fix dtype issues"""
    print("Fixing dtypes")

    return df.astype(schema)


@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str, colour: str) -> Path:
    """Write DataFrame out locally as gzipped csv file"""
    parent_dir = Path(f"/Users/cyrus/development/de_zoomcamp/week_3/data/{colour}")
    path = Path(f"{parent_dir}/{dataset_file}.parquet")

    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
        print(f"Creating {parent_dir}")

    df.to_parquet(path, engine="pyarrow")
    
    return path


@task()
def write_gcs(path: Path, gcs_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=gcs_path)


@task()
def load_schemas() -> dict:
    """Get dict of schemas for each colour"""
    with open("schema.yaml", "r") as file:
        return yaml.load(file, Loader=yaml.SafeLoader)


@flow(name="ETL web to GCS")
def etl_web_to_gcs(year: int, month: int, colour: str, schemas: dict) -> None:
    """The main ETL function"""
    dataset_file = f"{colour}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = fix_dtypes(df, schemas[colour])
    path = write_local(df_clean, dataset_file, colour)
    gcs_path = Path(f"data/{colour}/{dataset_file}.parquet")
    write_gcs(path, gcs_path)

@flow(name="NY Taxi data to GCS")
def etl_parent_flow(months: list[str] = [1, 2], years: list[int] = [2021, 2022], colour: str = "yellow") -> None:
    schemas = load_schemas()
    for year in years:
        for month in months:
            etl_web_to_gcs(year, month, colour, schemas)

if __name__ == '__main__':
    months=list(range(9, 13))
    years=[2020]
    colour = "green"
    etl_parent_flow(months, years, colour)

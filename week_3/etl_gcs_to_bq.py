from prefect_gcp.bigquery import BigQueryWarehouse
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@flow(name="GCS to BQ")
def etl_parent_flow(project: str = "de-zoomcamp-377022",dataset: str = "trips_data_all") -> None:
    bq_warehouse_block = BigQueryWarehouse.load("zoom-bq")

    operation = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.fhv_rides`
    OPTIONS (
        format = 'CSV',
        uris = ['gs://dtc_data_lake_de-zoomcamp-377022/data/fhv/*']
    )

    """

    bq_warehouse_block.execute(operation)

if __name__ == '__main__':
    etl_parent_flow()
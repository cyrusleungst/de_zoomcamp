from prefect_gcp.cloud_storage import GcsBucket

gcp_cloud_storage_block = GcsBucket(
    bucket="dtc_data_lake_de-zoomcamp-377022"
)

gcp_cloud_storage_block.save("zoom-gcs", overwrite=True)
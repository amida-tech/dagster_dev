import os
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dagster import asset, AssetIn

CONNECTION_STRING = os.environ["ADLS_CON_STRING"]

@asset(deps=["validate_csv_counts"])
def convert_csv_to_parquet() -> None:
    container = "med01nc-test-data"
    input_folder = "data/dagster"
    output_folder = "data/dagster/Load"

    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service.get_container_client(container)

    blobs = container_client.list_blobs(name_starts_with=input_folder)

    for blob in blobs:
        if not blob.name.endswith(".csv") or "counts.txt" in blob.name:
            continue

        file_name = os.path.basename(blob.name)
        base_name = os.path.splitext(file_name)[0]
        parquet_blob_name = f"{output_folder}/{base_name}.parquet"

        # Read CSV
        blob_client = container_client.get_blob_client(blob.name)
        stream = BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)
        df = pd.read_csv(stream)

        # Convert to Parquet
        out_stream = BytesIO()
        df.to_parquet(out_stream, index=False)
        out_stream.seek(0)

        # Upload Parquet file
        parquet_blob = container_client.get_blob_client(parquet_blob_name)
        parquet_blob.upload_blob(out_stream, overwrite=True)

    print("✅ All CSVs converted to Parquet.")

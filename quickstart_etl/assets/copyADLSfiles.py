import os
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dagster import asset

CONNECTION_STRING = os.environ["ADLS_CON_STRING"]

@asset
def process_and_count_csvs() -> str:
    container_name = "med01nc-test-data"
    source_prefix = "data/"
    target_prefix = "data/dagster/"
    counts = []

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container_name)

    # List all CSV files in the source directory
    blobs = container_client.list_blobs(name_starts_with=source_prefix)

    for blob in blobs:
        if not blob.name.endswith(".csv") or target_prefix in blob.name:
            continue

        file_name = os.path.basename(blob.name)
        source_blob_client = container_client.get_blob_client(blob.name)
        target_blob_path = f"{target_prefix}{file_name}"
        target_blob_client = container_client.get_blob_client(target_blob_path)

        # Download and load the CSV
        stream = BytesIO()
        source_blob_client.download_blob().readinto(stream)
        stream.seek(0)
        df = pd.read_csv(stream)

        # Upload the file to dagster/ path
        stream.seek(0)
        target_blob_client.upload_blob(stream, overwrite=True)

        # Store the count
        counts.append(f"{file_name},{len(df)}")

    # Create counts.txt
    counts_blob = container_client.get_blob_client(f"{target_prefix}counts.txt")
    counts_data = "\n".join(counts).encode("utf-8")
    counts_blob.upload_blob(BytesIO(counts_data), overwrite=True)

    return f"Processed {len(counts)} files and created counts.txt"

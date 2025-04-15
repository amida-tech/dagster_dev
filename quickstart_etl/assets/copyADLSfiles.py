import os
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dagster import asset

CONNECTION_STRING = os.environ["ADLS_CON_STRING"]

@asset
def copyADLSfiles() -> str:
    container_name = "med01nc-test-data"
    source_prefix = "data/"
    target_prefix = "data/dagster/"
    counts = []

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container_name)

    blobs = container_client.list_blobs(name_starts_with=source_prefix)
    for blob in blobs:
        if not blob.name.endswith(".csv") or target_prefix in blob.name:
            continue

        file_name = os.path.basename(blob.name)
        stream = BytesIO()
        container_client.get_blob_client(blob.name).download_blob().readinto(stream)
        stream.seek(0)
        df = pd.read_csv(stream)

        # Upload to dagster folder
        stream.seek(0)
        container_client.get_blob_client(f"{target_prefix}{file_name}").upload_blob(stream, overwrite=True)
        counts.append(f"{file_name},{len(df)}")

    # Write counts.txt
    counts_blob = BytesIO("\n".join(counts).encode("utf-8"))
    container_client.get_blob_client(f"{target_prefix}counts.txt").upload_blob(counts_blob, overwrite=True)

    return f"✅ Processed {len(counts)} files and wrote counts.txt"

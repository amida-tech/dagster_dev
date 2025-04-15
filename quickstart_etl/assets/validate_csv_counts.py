import os
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dagster import asset, AssetIn

# Pull ADLS connection string from environment
CONNECTION_STRING = os.environ["ADLS_CON_STRING"]

@asset(deps=[AssetIn("process_and_count_csvs")])
def validate_csv_counts() -> str:
    container = "med01nc-test-data"
    folder_path = "data/dagster"
    counts_file = f"{folder_path}/counts.txt"

    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service.get_container_client(container)

    # Read counts.txt
    blob_client = container_client.get_blob_client(counts_file)
    content = blob_client.download_blob().readall().decode("utf-8")

    for line in content.strip().split("\n"):
        filename, expected_count = line.split(",")
        expected_count = int(expected_count)
        blob_name = f"{folder_path}/{filename}"

        # Download CSV and count rows
        stream = BytesIO()
        container_client.get_blob_client(blob_name).download_blob().readinto(stream)
        stream.seek(0)
        actual_count = len(pd.read_csv(stream))

        if actual_count != expected_count:
            raise ValueError(f"❌ Count mismatch for {filename}: expected {expected_count}, got {actual_count}")

    return "✅ All counts match."

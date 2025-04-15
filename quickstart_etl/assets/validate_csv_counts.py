import os
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dagster import asset, AssetIn

CONNECTION_STRING = os.environ["ADLS_CON_STRING"]

@asset(deps=[AssetIn("copyADLSfiles")])
def validate_csv_counts() -> None:
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

        # Read CSV again
        blob = container_client.get_blob_client(blob_name)
        stream = BytesIO()
        blob.download_blob().readinto(stream)
        stream.seek(0)

        df = pd.read_csv(stream)
        actual_count = len(df)

        if actual_count != expected_count:
            raise ValueError(f"Count mismatch for {filename}: expected {expected_count}, got {actual_count}")

    print("✅ All counts match.")

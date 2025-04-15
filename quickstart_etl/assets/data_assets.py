import os
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dagster import asset

# Get the ADLS connection string from env variable
CONNECTION_STRING = os.environ["ADLS_CON_STRING"]

@asset
def count_records_from_adls() -> int:
    # Define ADLS details
    container_name = "med01nc-test-data"
    blob_name = "data/b_xref_tb.csv"

    # Connect to ADLS
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download blob and load into pandas
    stream = BytesIO()
    blob_data = blob_client.download_blob()
    blob_data.readinto(stream)
    stream.seek(0)

    df = pd.read_csv(stream)
    return len(df)

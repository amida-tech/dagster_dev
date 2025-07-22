import os
from dagster import resource, InitResourceContext
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError

@resource
def adls_access_keys_resource(context: InitResourceContext) -> DataLakeServiceClient:
    """
    Reads ADLS Gen2 credentials from environment:
      • ADLS2_CONNECTION_STRING
      • or (ADLS2_ACCOUNT_NAME + ADLS2_ACCOUNT_KEY)
    """
    connection_string   = os.getenv("ADLS2_CONNECTION_STRING")
    account_name  = os.getenv("ADLS2_ACCOUNT_NAME")
    account_key   = os.getenv("ADLS2_ACCOUNT_KEY")

    if connection_string:
        try:
            client = DataLakeServiceClient.from_connection_string(connection_string)
        except AzureError as error:
            raise Exception(f"ADLS2: connection_string invalid: {error}") from error

    elif account_name and account_key:
        url = f"https://{account_name}.dfs.core.windows.net"
        try:
            client = DataLakeServiceClient(account_url=url, credential=account_key)
        except AzureError as error:
            raise Exception(f"ADLS2: account/key invalid: {error}") from error

    else:
        raise Exception(
            "ADLS2_RESOURCE: set either ADLS2_CONNECTION_STRING, "
            "or both ADLS2_ACCOUNT_NAME and ADLS2_ACCOUNT_KEY in env."
        )

    context.log.info("✅ Connected to ADLS via Access Key")
    return client

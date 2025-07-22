from typing import List
from azure.storage.blob import BlobClient
from dagster_azure.adls2 import ADLS2Resource
from dagster import AssetExecutionContext

def copy_adls_to_adls(
    context: AssetExecutionContext,
    adls2_resource: ADLS2Resource,
    source_container: str,
    source_folder: str,
    dest_container: str,
    dest_folder: str,
    prefix_filter: str,
) -> List[str]:
    """
    1) List all blobs under source_container/source_prefix
    2) Filter to only those whose basename starts with prefix_filter
    3) Server-side copy each blob to dest_container/dest_prefix
    Returns the list of full paths that were successfully copied.
    """
    account   = adls2_resource.storage_account
    sas_token = adls2_resource.credential.token

    # 1) get the filesystem client
    service_client = adls2_resource.adls2_client
    fs = service_client.get_file_system_client(source_container)

    # 2) enumerate + filter
    paths = fs.get_paths(path=source_folder, recursive=True)
    to_copy = [
        p.name[len(source_folder)+1 :]  
        for p in paths
        if not p.is_directory
           and p.name.split("/")[-1].startswith(prefix_filter)
    ]

    context.log.info(f" 🔍 Found {len(to_copy)} files matching '{prefix_filter}' under '{source_folder}': {to_copy}")

    # 3) COPY FILES
    copied: List[str] = []
    for name in to_copy:
        source_url = (
            f"https://{account}.blob.core.windows.net/"
            f"{source_container}/{source_folder}/{name}?{sas_token}"
        )
        dest_blob_name = f"{dest_folder}/{name}"
        destination_blob = BlobClient(
            account_url=f"https://{account}.blob.core.windows.net",
            container_name=dest_container,
            blob_name=dest_blob_name,
            credential=sas_token,
        )

        try:
            destination_blob.start_copy_from_url(source_url)
            context.log.info(f"✅ Copied {name!r} → {dest_blob_name!r}")
            copied.append(dest_blob_name)
        except Exception as err:
            context.log.error(f"❌ Failed to copy {name!r}: {err}")

    return copied
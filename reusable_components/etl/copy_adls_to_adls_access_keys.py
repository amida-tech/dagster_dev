import os
from typing import List
from azure.storage.filedatalake import DataLakeServiceClient
from dagster import AssetExecutionContext
from reusable_components.error_handling.alert import with_alerts  

@with_alerts()
def copy_adls_to_adls_access_key(
    context: AssetExecutionContext,
    client: DataLakeServiceClient,
    source_container: str,
    source_folder: str,
    dest_container: str,
    dest_folder: str,
    prefix_filter: str,
) -> List[str]:
    """
    Copies only files whose names start with `prefix_filter` under 
    `source_folder` in `source_container` into `dest_folder` in `dest_container`.
    Returns the list of destination file paths that were written.
    """

    source_path = source_folder.strip("/")
    destination_path = dest_folder.strip("/")

    context.log.info(
        f"🔄 Starting copy: {source_container}/{source_path} → {dest_container}/{destination_path} "
        f"(filter: '{prefix_filter}')"
    )

    source_filesystem = client.get_file_system_client(source_container)
    destination_filesystem = client.get_file_system_client(dest_container)

    # collect all source paths
    all_paths = list(source_filesystem.get_paths(path=source_path))
    context.log.info(f"Found {len(all_paths)} items under {source_container}/{source_path}")
    context.log.info(f"All Paths: {all_paths}")

    copied_paths: List[str] = []
    for path_item in all_paths:
        # skip directories
        if path_item.is_directory:
            continue

        src_path = path_item.name  
        filename = os.path.basename(src_path)

        # apply prefix filter
        if not filename.startswith(prefix_filter):
            context.log.debug(f"Skipping '{src_path}' (does not match prefix)")
            continue

        # build destination path
        rel_path = src_path[len(source_path):].lstrip("/")
        dst_path = f"{destination_path}/{rel_path}"

        context.log.info(f"➡️ Copying '{src_path}' → '{dst_path}'")
        # download/upload
        src_file = source_filesystem.get_file_client(src_path)
        data = src_file.download_file().readall()

        dst_file = destination_filesystem.get_file_client(dst_path)
        dst_file.create_file()
        dst_file.append_data(data, offset=0, length=len(data))
        dst_file.flush_data(len(data))

        copied_paths.append(dst_path)

    context.log.info(
        f"✅ Completed copy of {len(copied_paths)} file(s) matching '{prefix_filter}'"
    )
    return copied_paths
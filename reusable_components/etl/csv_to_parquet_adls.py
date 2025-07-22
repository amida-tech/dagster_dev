import io
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List
from dagster import AssetExecutionContext
from dagster_azure.adls2 import ADLS2Resource

def convert_csv_to_parquet_adls(
    context: AssetExecutionContext,
    adls2: ADLS2Resource,
    source_container: str,
    dest_container: str,
    source_folder: str,
    dest_folder: str,
    prefix_filter: str,
) -> List[str]:
    """
    Lists all CSV files in `source_container/source_folder` whose basenames
    start with `prefix_filter`, converts each to Parquet, and writes it into
    `dest_container/dest_folder` with the same basename but a .parquet extension.

    Returns the list of full destination paths written.
    """
    log = context.log
    log.info(
        f"🔄 Starting CSV→Parquet conversion: "
        f"src={source_container}/{source_folder}, "
        f"dst={dest_container}/{dest_folder}, "
        f"filter={prefix_filter}"
    )
    
    # Initialize file system clients
    source_fs = adls2.adls2_client.get_file_system_client(source_container)
    dest_fs = adls2.adls2_client.get_file_system_client(dest_container)
    written_paths: List[str] = []

    # Iterate over CSV files under source_folder
    for path in source_fs.get_paths(path=source_folder, recursive=True):
        if path.is_directory or not path.name.lower().endswith(".csv"):
            continue

        # Compute relative path and destination path
        rel_path = path.name[len(source_folder):].lstrip("/")
        base_name = os.path.splitext(os.path.basename(rel_path))[0]

        # Apply file-prefix filter
        if prefix_filter and not base_name.startswith(prefix_filter):
            continue

        parquet_name = f"{base_name}.parquet"
        dest_path = f"{dest_folder.rstrip('/')}/{parquet_name}" if dest_folder else parquet_name

        # 1) Download CSV into memory
        source_client = source_fs.get_file_client(path.name)
        stream = io.BytesIO()
        source_client.download_file().readinto(stream)
        stream.seek(0)
        log.info(f"Processing file: {path.name} → {dest_path}")

        # 2) Read entire CSV → pandas DataFrame
        csv_df = pd.read_csv(stream)

        # 3) Convert to Arrow Table + write to in-memory Parquet
        table = pa.Table.from_pandas(csv_df)
        pq_buffer = io.BytesIO()
        pq.write_table(table, pq_buffer)
        pq_buffer.seek(0)

        # 4) Upload Parquet to destination container
        data = pq_buffer.read()
        dest_client = dest_fs.get_file_client(dest_path)
        dest_client.create_file()
        dest_client.append_data(data, offset=0)
        dest_client.flush_data(len(data))
        log.info(f"Uploaded: {dest_path}")

        written_paths.append(dest_path)

    log.info(f"Conversion completed: {len(written_paths)} files written")
    return written_paths
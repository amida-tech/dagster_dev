from typing import List, Tuple, Optional, Union
from dagster import AssetExecutionContext
import re

def load_column_mappings(mapping_file_path: str) -> Tuple[str, str]:
    """
    Reads SQL file and returns:
      - target_columns: comma-joined list of quoted Snowflake column names, e.g. '"NPI", "ENTITY_TYPE_CODE", …'
    """
    projection_lines = []
    target_columns = []

    with open(mapping_file_path, "r") as f:
        for line in f:
            line = line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            if " AS " not in line.upper():
                raise ValueError(f"Invalid mapping line: {line}")
            src_part, tgt_part = line.rsplit(" AS ", 1)
            # build both sides so projection_lines stays compatible if you ever want it back
            projection_lines.append(f'{src_part.strip()} AS "{tgt_part.strip()}"')  
            target_columns.append(f'"{tgt_part.strip()}"')

    return ",\n".join(projection_lines), ", ".join(target_columns)


def load_csv_to_iceberg_with_mapping(
    context: AssetExecutionContext,
    adls_client,
    snowpark_session,
    program_name: str,
    subject_area: str,
    batch_id: int,
    container: str,
    folder: str,
    file_name: str,
    mapping_file_path: str,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> List[str]:
    log = context.log
    db = "ANALYTYXONE_DEV"
    schema = "BRONZE"
    stage_name = "CSV_STAGE"
    
    def file_matches_criteria(filename: str) -> bool:
        if prefix:
            if isinstance(prefix, list):
                if not any(filename.startswith(p) for p in prefix):
                    return False
            else:
                if not filename.startswith(prefix):
                    return False
        if suffix:
            if isinstance(suffix, list):
                if not any(filename.endswith(s) for s in suffix):
                    return False
            else:
                if not filename.endswith(suffix):
                    return False
        if contains:
            if isinstance(contains, list):
                if not any(c in filename for c in contains):
                    return False
            else:
                if contains not in filename:
                    return False
        if not_contains:
            if isinstance(not_contains, list):
                if any(nc in filename for nc in not_contains):
                    return False
            else:
                if not_contains in filename:
                    return False
        if regex:
            if not re.search(regex, filename):
                return False
        if extension:
            if isinstance(extension, list):
                if not any(filename.lower().endswith(ext.lower()) for ext in extension):
                    return False
            else:
                if not filename.lower().endswith(extension.lower()):
                    return False
        return True
    
    _, target_columns = load_column_mappings(mapping_file_path)

    # List CSVs in ADLS folder
    file_system = adls_client.get_file_system_client(container)
    paths = []
    
    for path in file_system.get_paths(path=folder):
        if path.is_directory:
            continue
        filename = path.name.split('/')[-1]
        if (filename.lower().endswith('.csv') and 
            (not file_name or filename == file_name) and
            file_matches_criteria(filename)):
            paths.append(path.name)
    loaded_tables: List[str] = []
    for full_path in paths:
        filename = full_path.rsplit("/", 1)[-1]
        table_name = filename[:-4]
        full_table = f"{db}.{schema}.{table_name}"
        stage_path = f"@{db}.{schema}.{stage_name}/{full_path}"

        log.info(f"➡️ Copying {filename} → Iceberg table {full_table}")

        copy_sql = f"""
            COPY INTO {full_table} ({target_columns})
            FROM {stage_path}
            FILE_FORMAT = {db}.{schema}.bronze_csv_column_matching
            FORCE = TRUE
        """
        log.info(f" SQL:\n{copy_sql}")
        snowpark_session.sql(copy_sql).collect()

        # Back-fill metadata
        log.info(f"📝 Back-filling metadata on {full_table}")
        metadata_sql = f"""
            UPDATE {full_table}
            SET
                "META_PROGRAM_NAME" = '{program_name}',
                "META_SUBJECT_AREA" = '{subject_area}',
                "META_BATCH_ID" = {batch_id},
                "META_ROW_ID" = DEFAULT,
                "META_DATE_INSERT" = CURRENT_TIMESTAMP()
            WHERE "META_BATCH_ID" IS NULL
        """
        snowpark_session.sql(metadata_sql).collect()

        loaded_tables.append(table_name)
        log.info(f"✅ Loaded: {table_name}")
    return loaded_tables
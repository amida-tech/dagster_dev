import os
import io
import zipfile
import re
from typing import List, Optional, Union
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from snowflake.snowpark import Session
from dagster_azure.adls2 import ADLS2Resource
from datetime import date


def load_dq_transactions_with_result(
    context: AssetExecutionContext,
    snowpark_session: Session,
    adls_client: ADLS2Resource,
    audit_batch_id: int,
    container_name: str,
    directory_path: str,
    program_name: str,
    subject_area: str,
    pipeline_name: str,
    copy_result: Optional[dict] = None,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> MaterializeResult:
    """
    Complete DQ transactions logic: validation + transaction loading + MaterializeResult.
    Returns MaterializeResult ready for the asset to return.
    """
    
    if copy_result is not None:
            copy_status = copy_result.get("status", "unknown")
            
            if copy_status != "completed":
                context.log.info(f"❌ Skipping {pipeline_name} DQ transactions - copy status: {copy_status}")
                
                return MaterializeResult(
                    value=[],
                    metadata={
                        "status": MetadataValue.text("⏭️ SKIPPED"),
                        "reason": MetadataValue.text(f"Copy operation status: {copy_status}"),
                        "pipeline_name": MetadataValue.text(pipeline_name),
                        "batch_id": MetadataValue.int(audit_batch_id)
                    }
                )
            else:
                context.log.info(f"✅ {pipeline_name} copy validation passed - proceeding with DQ transactions")
    else:
        context.log.info(f"📊 {pipeline_name} - No copy validation needed, proceeding with DQ transactions")
    
    context.log.info(f"📊 {pipeline_name} loading DQ transactions for batch {audit_batch_id}")
    
    try:
        # Load DQ transactions using internal component
        transaction_ids = _load_dq_transactions_internal(
            context=context,
            snowpark_session=snowpark_session,
            adls_client=adls_client,
            container_name=container_name,
            directory_path=directory_path,
            batch_id=audit_batch_id,
            program_name=program_name,
            subject_area=subject_area,
            prefix=prefix,
            suffix=suffix,
            contains=contains,
            not_contains=not_contains,
            regex=regex,
            extension=extension
        )
        
        context.log.info(f"✅ {pipeline_name} loaded {len(transaction_ids)} file transactions")
        
        # Return successful MaterializeResult
        return MaterializeResult(
            value=transaction_ids,
            metadata={
                "status": MetadataValue.text("✅ SUCCESS"),
                "transactions_loaded": MetadataValue.int(len(transaction_ids)),
                "batch_id": MetadataValue.int(audit_batch_id),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "file_criteria_used": MetadataValue.text(f"prefix={prefix}, extension={extension}")
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ {pipeline_name} DQ transactions failed: {str(e)}")
        
        # Return failed MaterializeResult
        return MaterializeResult(
            value=[],
            metadata={
                "status": MetadataValue.text("❌ ERROR"),
                "error": MetadataValue.text(str(e)[:200] + "..." if len(str(e)) > 200 else str(e)),
                "pipeline_name": MetadataValue.text(pipeline_name),
                "batch_id": MetadataValue.int(audit_batch_id),
                "transactions_loaded": MetadataValue.int(0)
            }
        )
        raise


def _load_dq_transactions_internal(
    context: AssetExecutionContext,
    snowpark_session: Session,
    adls_client: ADLS2Resource,
    container_name: str,
    directory_path: str, 
    batch_id: int,
    program_name: str,
    subject_area: str,
    prefix: Optional[Union[str, List[str]]] = None,
    suffix: Optional[Union[str, List[str]]] = None,
    contains: Optional[Union[str, List[str]]] = None,
    not_contains: Optional[Union[str, List[str]]] = None,
    regex: Optional[str] = None,
    extension: Optional[Union[str, List[str]]] = None
) -> List[str]:  
    """
    Internal function that does the actual DQ transactions work.
    - Lists files under container_name/directory_path in ADLS, filtering by criteria.
    - Inserts a parent transaction record for each file, retrieves its auto-generated ID_TRANSACTION.
    - If the file is a ZIP, inspects its CSV contents and inserts a child record for each CSV,
      setting CHILD_ID_TRANSACTION to the parent's ID_TRANSACTION.

    Returns a list of generated parent and child ID_TRANSACTION values as strings.
    """
    # Hard-coded parameters
    transaction_direction = "INBOUND"
    frequency_code = "DAILY"
    updated_by = "ETL"
    date_str = date.today().isoformat()

    fs_client = adls_client.get_file_system_client(container_name)
    base_url = fs_client.url
    transaction_counter = 0
    inserted_ids: List[str] = [] 

    context.log.info(f"📁 Scanning ADLS directory: {container_name}/{directory_path}")

    # Map extensions to file type labels
    ext_map = {
        ".csv": "CSV",            
        ".parquet": "PARQUET",    
        ".txt": "TEXT",           
        ".xlsx": "EXCEL",         
        ".xls": "EXCEL",          
    }

    # Helper function to check if file matches criteria
    def file_matches_criteria(filename: str) -> bool:
        """Check if file matches all specified criteria"""
        
        # Check prefix criteria
        if prefix:
            if isinstance(prefix, list):
                if not any(filename.startswith(p) for p in prefix):
                    return False
            else:
                if not filename.startswith(prefix):
                    return False
        
        # Check suffix criteria
        if suffix:
            if isinstance(suffix, list):
                if not any(filename.endswith(s) for s in suffix):
                    return False
            else:
                if not filename.endswith(suffix):
                    return False
        
        # Check contains criteria
        if contains:
            if isinstance(contains, list):
                if not any(c in filename for c in contains):
                    return False
            else:
                if contains not in filename:
                    return False
        
        # Check not_contains criteria
        if not_contains:
            if isinstance(not_contains, list):
                if any(nc in filename for nc in not_contains):
                    return False
            else:
                if not_contains in filename:
                    return False
        
        # Check regex criteria
        if regex:
            if not re.search(regex, filename):
                return False
        
        # Check extension criteria
        if extension:
            if isinstance(extension, list):
                if not any(filename.lower().endswith(ext.lower()) for ext in extension):
                    return False
            else:
                if not filename.lower().endswith(extension.lower()):
                    return False
        
        return True

    for path_props in fs_client.get_paths(path=directory_path):
        if path_props.is_directory:
            continue

        file_name = os.path.basename(path_props.name)
        
        # Apply file criteria filtering
        if not file_matches_criteria(file_name):
            context.log.info(f"⏭️ Skipping file: {file_name} (doesn't match criteria)")
            continue
        
        # Generate new transaction ID
        transaction_counter += 1
        transaction_id = f"{program_name}_{subject_area}_{date_str}_{batch_id}_{transaction_counter}"

        file_fqn = f"{base_url}/{path_props.name}"
        is_zip = file_name.lower().endswith(".zip")
        if is_zip:
            file_type = "ZIP"
        else:
            ext = os.path.splitext(file_name)[1].lower()
            file_type = ext_map.get(ext, ext.lstrip(".").upper())
        
        size_bytes = getattr(path_props, "content_length", None)
        file_size = f"{size_bytes}B" if size_bytes is not None else "UNKNOWN"
        date_inbound = path_props.creation_time.isoformat()
        
        context.log.info(f"→ File: {file_name} | Created: {date_inbound}")

        # Insert parent record
        insert_parent_sql = f"""
            INSERT INTO ANALYTYXONE_DEV.DATALOOM.DQ_TRANSACTIONS (
              ID_TRANSACTION,
              CHILD_ID_TRANSACTION,
              FILE_NAME,
              FILE_PATH_FQN,
              CODE_FILE_TYPE,
              CODE_TRANSACTION_TYPE,
              DATE_INBOUND,
              SUBJECT_AREA,
              ID_BATCH,
              CODE_FREQUENCY,
              FILE_SIZE,
              UPDATED_BY
            ) VALUES (
              '{transaction_id}',
              NULL,
              '{file_name}',
              '{file_fqn}',
              '{file_type}',
              '{transaction_direction}',
              '{date_inbound}',
              '{subject_area}',
              {batch_id},
              '{frequency_code}',
              '{file_size}',
              '{updated_by}'
            )
        """
        snowpark_session.sql(insert_parent_sql).collect()
        context.log.info(f"✔ Inserted parent ID_TRANSACTION={transaction_id}")
        inserted_ids.append(transaction_id)

        # If ZIP, preview and insert child records
        if is_zip:
            file_client = fs_client.get_file_client(path_props.name)
            stream = io.BytesIO()
            file_client.download_file().readinto(stream)
            stream.seek(0)

            with zipfile.ZipFile(stream, 'r') as zf:
                child_counter = 0
                for zip_info in zf.infolist():
                    child_counter += 1
                    child_name = os.path.basename(zip_info.filename)

                    ext = os.path.splitext(child_name)[1].lower()
                    child_file_type = ext_map.get(ext, ext.lstrip(".").upper())
                    child_size = f"{zip_info.file_size}B" 

                    insert_child_sql = f"""
                        INSERT INTO ANALYTYXONE_DEV.DATALOOM.DQ_TRANSACTIONS (
                          ID_TRANSACTION,
                          CHILD_ID_TRANSACTION,
                          FILE_NAME,
                          FILE_PATH_FQN,
                          CODE_FILE_TYPE,
                          CODE_TRANSACTION_TYPE,
                          DATE_INBOUND,
                          SUBJECT_AREA,
                          ID_BATCH,
                          CODE_FREQUENCY,
                          FILE_SIZE,
                          UPDATED_BY
                        ) VALUES (
                          '{transaction_id}',
                          '{child_counter}',
                          '{child_name}',
                          '{file_fqn}/{child_name}',
                          '{child_file_type}',
                          '{transaction_direction}',
                          '{date_inbound}',
                          '{subject_area}',
                          {batch_id},
                          '{frequency_code}',
                          '{child_size}',
                          '{updated_by}'
                        )
                    """
                    snowpark_session.sql(insert_child_sql).collect()
                    context.log.info(
                        f"✔ Inserted child ID_TRANSACTION={transaction_id}, "
                        f"CHILD_ID_TRANSACTION={child_counter} for {child_name}"
                    )
                    inserted_ids.append(f"{transaction_id}.{child_counter}")

    context.log.info(f"📊 Total records created: {len(inserted_ids)}")
    return inserted_ids
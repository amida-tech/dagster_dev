from dagster import AssetExecutionContext
from typing import List, Dict, Any
import logging

def cleanup_pipeline_directories(
    context: AssetExecutionContext,
    adls_client,
    container_name: str,
    directories_to_clean: List[Dict[str, str]]
) -> Dict[str, Any]:
    """
    Simple cleanup function to delete files from stage and load directories
    
    Args:
        context: Dagster context
        adls_client: ADLS client
        container_name: Container name (e.g., "srcfiles")
        directories_to_clean: List of dicts with pipeline_name and directories
    
    Returns:
        Dict with cleanup results
    """
    
    context.log.info("🧹 Starting pipeline cleanup...")
    
    cleaned_dirs = []
    errors = []
    
    for pipeline_config in directories_to_clean:
        pipeline_name = pipeline_config["pipeline_name"]
        stage_dir = pipeline_config.get("stage_directory")
        load_dir = pipeline_config.get("load_directory")
        
        context.log.info(f"Cleaning {pipeline_name} directories...")
        
        # Clean stage directory
        if stage_dir:
            try:
                _delete_files_in_directory(adls_client, container_name, stage_dir, context)
                cleaned_dirs.append(f"{pipeline_name} stage")
            except Exception as e:
                errors.append(f"{pipeline_name} stage: {str(e)}")
        
        # Clean load directory
        if load_dir:
            try:
                _delete_files_in_directory(adls_client, container_name, load_dir, context)
                cleaned_dirs.append(f"{pipeline_name} load")
            except Exception as e:
                errors.append(f"{pipeline_name} load: {str(e)}")
    
    if errors:
        raise Exception(f"Cleanup errors: {'; '.join(errors)}")
    
    context.log.info(f"✅ Cleanup completed: {len(cleaned_dirs)} directories cleaned")
    
    return {
        "status": "success",
        "cleaned_directories": cleaned_dirs,
        "total_cleaned": len(cleaned_dirs)
    }

def _delete_files_in_directory(adls_client, container_name: str, directory_path: str, context: AssetExecutionContext):
    """Helper function to delete files in a directory"""
    try:
        fs_client = adls_client.get_file_system_client(container_name)
        directory_client = fs_client.get_directory_client(directory_path)
        
        # List and delete files
        files = [item.name for item in directory_client.list_paths() if not item.is_directory]
        
        if not files:
            context.log.info(f"  {directory_path}: already empty")
            return
        
        context.log.info(f"  {directory_path}: deleting {len(files)} files")
        
        for file_path in files:
            try:
                file_client = fs_client.get_file_client(file_path)
                file_client.delete_file()
            except Exception as e:
                context.log.warning(f"  Failed to delete {file_path}: {str(e)}")
                
    except Exception as e:
        context.log.error(f"  Error cleaning {directory_path}: {str(e)}")
        raise 
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue

def cleanup_pipeline_directories_with_result(
    context: AssetExecutionContext,
    adls_client,
    config: dict
) -> MaterializeResult:
    container = config["stage_container"]
    stage_dir = config["stage_directory"] 
    load_dir = config["load_directory"]
    pipeline_name = config["pipeline_name"]
    
    try:
        fs_client = adls_client.get_file_system_client(container)
        cleaned_dirs = []
        for dir_name, dir_path in [("stage", stage_dir), ("load", load_dir)]:
            try:
                paths = fs_client.get_paths(path=dir_path)
                files = [path for path in paths if not path.is_directory]
                
                if files:
                    for file_path in files:
                        fs_client.get_file_client(file_path.name).delete_file()
                    cleaned_dirs.append(f"{dir_name} ({len(files)} files)")
                    context.log.info(f"Deleted {len(files)} files from {dir_path}")
                    
            except Exception as e:
                context.log.warning(f"Could not clean {dir_path}: {str(e)}")
        
        context.log.info(f"✅ Cleanup completed for {pipeline_name}")
        
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("✅ CLEANED"),
                "directories": MetadataValue.text(", ".join(cleaned_dirs) if cleaned_dirs else "No files found"),
                "pipeline": MetadataValue.text(pipeline_name)
            }
        )
        
    except Exception as e:
        context.log.error(f"❌ Cleanup failed for {pipeline_name}: {str(e)}")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("❌ FAILED"),
                "error": MetadataValue.text(str(e)[:200])
            }
        )
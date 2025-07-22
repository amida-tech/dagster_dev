import requests
from datetime import datetime
from dagster import get_dagster_logger, AssetExecutionContext
from functools import wraps
from typing import Dict, Any, Optional


# Default alert configurations for different pipelines
DEFAULT_ALERT_CONFIGS = {
    "MEDICAID_RECIPIENT": {
        "logic_app_url": "https://prod-47.eastus2.logic.azure.com:443/workflows/09f114d3198a487db73ec504e0277148/triggers/Receive_Medicaid_Alert_Request/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FReceive_Medicaid_Alert_Request%2Frun&sv=1.0&sig=QdaGnWj1_gZp0nzZZCWzkgn-1nj_r9LGG-al5f1NPrE",
        "environment": "Development",
        "application_name": "Dagster - Medicaid Processing",
        "program_name": "Medicaid Data Processing",
        "recipients": {
            "email": ["greeshmanjali@amida.com"]
        },
        "send_success_alerts": True
    },
    "MEDICAID_PROVIDER": {
        "logic_app_url": "https://prod-47.eastus2.logic.azure.com:443/workflows/09f114d3198a487db73ec504e0277148/triggers/Receive_Medicaid_Alert_Request/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FReceive_Medicaid_Alert_Request%2Frun&sv=1.0&sig=QdaGnWj1_gZp0nzZZCWzkgn-1nj_r9LGG-al5f1NPrE",
        "environment": "Development",
        "application_name": "Dagster - Medicaid Processing",
        "program_name": "Medicaid Provider Processing",
        "recipients": {
            "email": ["greeshmanjali@amida.com"]
        },
        "send_success_alerts": True
    },
    "MEDICAID_CLAIMS": {
        "logic_app_url": "https://prod-47.eastus2.logic.azure.com:443/workflows/09f114d3198a487db73ec504e0277148/triggers/Receive_Medicaid_Alert_Request/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FReceive_Medicaid_Alert_Request%2Frun&sv=1.0&sig=QdaGnWj1_gZp0nzZZCWzkgn-1nj_r9LGG-al5f1NPrE",
        "environment": "Development",
        "application_name": "Dagster - Medicaid Processing",
        "program_name": "Medicaid Claims Processing",
        "recipients": {
            "email": ["greeshmanjali@amida.com"]
        },
        "send_success_alerts": True
    },
    "DEFAULT": {
        "logic_app_url": "https://prod-47.eastus2.logic.azure.com:443/workflows/09f114d3198a487db73ec504e0277148/triggers/Receive_Medicaid_Alert_Request/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FReceive_Medicaid_Alert_Request%2Frun&sv=1.0&sig=QdaGnWj1_gZp0nzZZCWzkgn-1nj_r9LGG-al5f1NPrE",
        "environment": "Development",
        "application_name": "Dagster",
        "program_name": "Data Processing Pipeline",
        "recipients": {
            "email": ["greeshmanjali@amida.com"]
        },
        "send_success_alerts": True
    }
}


def get_pipeline_alert_config(pipeline_name: str, custom_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Get alert configuration for a specific pipeline
    
    Args:
        pipeline_name: Name of the pipeline (e.g., "MEDICAID_RECIPIENT")
        custom_config: Optional custom configuration overrides
    
    Returns:
        Dict containing alert configuration
    """
    # Get base config for pipeline or default
    base_config = DEFAULT_ALERT_CONFIGS.get(pipeline_name, DEFAULT_ALERT_CONFIGS["DEFAULT"]).copy()
    
    # Apply custom overrides if provided
    if custom_config:
        base_config.update(custom_config)
    
    return base_config


def send_pipeline_alert(
    context: AssetExecutionContext,
    pipeline_name: str,
    trigger_type: str,
    message: str,
    error_details: Optional[str] = None,
    alert_config: Optional[Dict[str, Any]] = None,
    additional_metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Send standardized alert for any pipeline
    
    Args:
        context: Dagster asset execution context
        pipeline_name: Name of the pipeline (e.g., "MEDICAID_RECIPIENT")
        trigger_type: "info" (success), "error" (failure), or "log" (warning)
        message: Main alert message
        error_details: Additional error information (for error/log types)
        alert_config: Optional custom alert configuration
        additional_metadata: Additional metadata to include in alert
    
    Returns:
        bool: True if alert sent successfully, False if failed
    """
    logger = get_dagster_logger()
    
    # Get alert configuration for this pipeline
    config = get_pipeline_alert_config(pipeline_name, alert_config)
    
    # Map trigger types to severity levels
    severity_mapping = {
        "error": "High",
        "info": "Informational", 
        "log": "Medium"
    }
    
    # Get asset name from context
    asset_name = getattr(context, 'asset_key', {})
    if hasattr(asset_name, 'path'):
        asset_name = asset_name.path[-1] if asset_name.path else "unknown_asset"
    else:
        asset_name = "unknown_asset"
    
    # Build alert payload
    alert_payload = {
        "triggerType": trigger_type,
        "environment": config.get("environment", "Development"),
        "applicationName": config.get("application_name", "Dagster"),
        "programName": config.get("program_name", pipeline_name),
        "pipelineName": pipeline_name,
        "assetName": asset_name,
        "displayMessage": message,
        "subjectMessage": f"[{pipeline_name}] {message}",
        "runID": context.run_id,
        "severity": severity_mapping.get(trigger_type, "Informational"),
        "AlertTimestamp": datetime.utcnow().isoformat() + "Z",
        "recipients": config.get("recipients", {"email": ["greeshmanjali@amida.com"]}),
        "pipeline_metadata": {
            "pipeline_name": pipeline_name,
            "asset_name": asset_name,
            "run_id": context.run_id,
            "trigger_type": trigger_type
        }
    }
    
    # Add error details for error/log alerts
    if error_details and trigger_type in ["error", "log"]:
        alert_payload["errorMessage"] = {
            "message": error_details,
            "code": trigger_type.upper(),
            "stackTrace": error_details,
            "asset_name": asset_name,
            "pipeline_name": pipeline_name
        }
    
    # Add any additional metadata
    if additional_metadata:
        alert_payload["additional_metadata"] = additional_metadata
    
    # Send alert to Logic App
    try:
        logic_app_url = config.get("logic_app_url")
        if not logic_app_url:
            logger.error(f"❌ No Logic App URL configured for pipeline: {pipeline_name}")
            return False
        
        response = requests.post(
            url=logic_app_url,
            json=alert_payload,
            timeout=30
        )
        response.raise_for_status()
        
        logger.info(f"✅ {trigger_type.upper()} alert sent for {pipeline_name} - {asset_name}")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to send {trigger_type} alert for {pipeline_name}: {str(e)}")
        return False
        
    except Exception as e:
        logger.error(f"❌ Unexpected error sending {trigger_type} alert for {pipeline_name}: {str(e)}")
        return False


def with_pipeline_alerts(
    pipeline_name: str,
    alert_config: Optional[Dict[str, Any]] = None,
    send_success_alerts: Optional[bool] = None
):
    """
    Decorator that automatically adds standardized alerting to any Dagster asset
    
    This decorator:
    1. Runs your asset function
    2. Sends SUCCESS alert if completed without errors (if enabled)
    3. Sends ERROR alert if failed, then re-throws error to Dagster
    4. Uses pipeline-specific configuration
    
    Args:
        pipeline_name: Name of the pipeline (e.g., "MEDICAID_RECIPIENT")
        alert_config: Optional custom alert configuration
        send_success_alerts: Override success alert setting
    
    Usage:
        @asset(name="my_asset")
        @with_pipeline_alerts(pipeline_name="MEDICAID_RECIPIENT")
        def my_asset(context):
            # Your asset code here
            return result
    """
    
    def decorator(asset_function):
        @wraps(asset_function)
        def wrapper(context, *args, **kwargs):
            logger = get_dagster_logger()
            asset_name = asset_function.__name__
            
            # Get pipeline configuration
            config = get_pipeline_alert_config(pipeline_name, alert_config)
            should_send_success = send_success_alerts if send_success_alerts is not None else config.get("send_success_alerts", True)
            
            try:
                logger.info(f"🚀 [{pipeline_name}] Starting asset: {asset_name}")
                
                # Execute the original asset function
                result = asset_function(context, *args, **kwargs)
                
                logger.info(f"✅ [{pipeline_name}] Asset completed: {asset_name}")
                
                # Send success alert if enabled
                if should_send_success:
                    send_pipeline_alert(
                        context=context,
                        pipeline_name=pipeline_name,
                        trigger_type="info",
                        message=f"Asset '{asset_name}' completed successfully",
                        alert_config=alert_config,
                        additional_metadata={
                            "execution_status": "success",
                            "asset_name": asset_name
                        }
                    )
                
                return result
                
            except Exception as error:
                # Log error
                error_message = str(error)
                logger.error(f"❌ [{pipeline_name}] Asset failed: {asset_name} - {error_message}")
                
                # Send error alert
                send_pipeline_alert(
                    context=context,
                    pipeline_name=pipeline_name,
                    trigger_type="error",
                    message=f"Asset '{asset_name}' failed: {error_message}",
                    error_details=error_message,
                    alert_config=alert_config,
                    additional_metadata={
                        "execution_status": "failed",
                        "asset_name": asset_name,
                        "error_type": type(error).__name__
                    }
                )
                
                # Re-raise the error so Dagster marks the asset as failed
                raise error
        
        return wrapper
    return decorator


# Legacy compatibility functions (for existing code)
def send_alert(
    logic_app_url: str,
    trigger_type: str,
    message: str,
    pipeline_name: str,
    run_id: str,
    error_details: str = None
) -> bool:
    """
    Legacy compatibility function - use send_pipeline_alert instead
    
    This function is kept for backward compatibility with existing code.
    New code should use send_pipeline_alert() instead.
    """
    logger = get_dagster_logger()
    logger.warning("⚠️ Using legacy send_alert function. Consider upgrading to send_pipeline_alert.")
    
    # Create a minimal context-like object for compatibility
    class LegacyContext:
        def __init__(self, run_id, pipeline_name):
            self.run_id = run_id
            self.asset_key = type('obj', (object,), {'path': [pipeline_name]})()
    
    context = LegacyContext(run_id, pipeline_name)
    
    # Use new alert system with legacy parameters
    custom_config = {"logic_app_url": logic_app_url}
    
    return send_pipeline_alert(
        context=context,
        pipeline_name=pipeline_name,
        trigger_type=trigger_type,
        message=message,
        error_details=error_details,
        alert_config=custom_config
    )


def with_alerts(
    logic_app_url: str = None,
    send_success_alerts: bool = True
):
    """
    Legacy compatibility decorator - use with_pipeline_alerts instead
    
    This decorator is kept for backward compatibility with existing code.
    New code should use with_pipeline_alerts() instead.
    """
    def decorator(asset_function):
        @wraps(asset_function)
        def wrapper(context, *args, **kwargs):
            logger = get_dagster_logger()
            logger.warning("⚠️ Using legacy with_alerts decorator. Consider upgrading to with_pipeline_alerts.")
            
            asset_name = asset_function.__name__
            
            try:
                logger.info(f"Starting execution of asset: {asset_name}")
                result = asset_function(context, *args, **kwargs)
                logger.info(f"Asset {asset_name} completed successfully")
                
                if send_success_alerts and logic_app_url:
                    send_alert(
                        logic_app_url=logic_app_url,
                        trigger_type="info",
                        message=f"Asset '{asset_name}' completed successfully.",
                        pipeline_name=asset_name,
                        run_id=context.run_id
                    )
                
                return result
                
            except Exception as error:
                error_message = str(error)
                logger.error(f"Asset '{asset_name}' failed with error: {error_message}")
                
                if logic_app_url:
                    send_alert(
                        logic_app_url=logic_app_url,
                        trigger_type="error",
                        message=f"Asset '{asset_name}' failed: {error_message}",
                        pipeline_name=asset_name,
                        run_id=context.run_id,
                        error_details=error_message
                    )
                
                raise error
        
        return wrapper
    return decorator


# Utility functions for testing and configuration
def test_pipeline_alerts(pipeline_name: str, alert_config: Optional[Dict[str, Any]] = None) -> bool:
    """
    Test alert functionality for a specific pipeline
    
    Args:
        pipeline_name: Name of the pipeline to test
        alert_config: Optional custom configuration
    
    Returns:
        bool: True if test alert sent successfully
    """
    logger = get_dagster_logger()
    
    # Create a test context
    class TestContext:
        def __init__(self):
            self.run_id = f"test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.asset_key = type('obj', (object,), {'path': ['test_asset']})()
    
    context = TestContext()
    
    logger.info(f"🧪 Testing alerts for pipeline: {pipeline_name}")
    
    # Test info alert
    success = send_pipeline_alert(
        context=context,
        pipeline_name=pipeline_name,
        trigger_type="info",
        message=f"Test alert for {pipeline_name} pipeline - all systems operational",
        alert_config=alert_config,
        additional_metadata={"test": True, "timestamp": datetime.now().isoformat()}
    )
    
    if success:
        logger.info(f"✅ Test alert sent successfully for {pipeline_name}")
    else:
        logger.error(f"❌ Test alert failed for {pipeline_name}")
    
    return success


def get_available_pipelines() -> list:
    """
    Get list of available pipeline configurations
    
    Returns:
        List of configured pipeline names
    """
    return [name for name in DEFAULT_ALERT_CONFIGS.keys() if name != "DEFAULT"]


def update_pipeline_alert_config(pipeline_name: str, new_config: Dict[str, Any]) -> None:
    """
    Update alert configuration for a specific pipeline
    
    Args:
        pipeline_name: Name of the pipeline
        new_config: New configuration values to merge
    """
    if pipeline_name not in DEFAULT_ALERT_CONFIGS:
        DEFAULT_ALERT_CONFIGS[pipeline_name] = DEFAULT_ALERT_CONFIGS["DEFAULT"].copy()
    
    DEFAULT_ALERT_CONFIGS[pipeline_name].update(new_config)
    
    logger = get_dagster_logger()
    logger.info(f"📝 Updated alert configuration for pipeline: {pipeline_name}")
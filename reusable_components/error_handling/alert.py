import requests                      
from datetime import datetime        
from dagster import get_dagster_logger  
from functools import wraps         

def send_alert(
    logic_app_url: str,              
    trigger_type: str,               
    message: str,                    
    pipeline_name: str,              
    run_id: str,                     
    error_details: str = None       
):
    """
    Sends alert notifications to Azure Logic App
    
    This function creates a structured payload and sends it to your Logic App,
    which then distributes the alert to email, Teams, Slack, etc.
    
    Args:
        logic_app_url: The HTTP trigger URL of your Azure Logic App
        trigger_type: "info" (success), "error" (failure), or "log" (warning)
        message: The main message to send in the alert
        pipeline_name: Name of the Dagster asset that triggered this alert
        run_id: Unique identifier for this Dagster run
        error_details: Additional error information (for error/log types)
    
    Returns:
        bool: True if alert sent successfully, False if failed
    """
    logger = get_dagster_logger()
    
    severity_mapping = {
        "error": "High",             
        "info": "Informational",     
        "log": "Medium"              
    }
    
    alert_payload = {
        "triggerType": trigger_type,                           
        "environment": "Development",                          
        "applicationName": "Dagster",                          
        "programName": "Medicaid Data Processing",             
        "pipelineName": pipeline_name,                        
        "displayMessage": message,                             
        "subjectMessage": message,                             
        "runID": run_id,                                       
        "severity": severity_mapping.get(trigger_type, "Informational"),  
        "AlertTimestamp": datetime.utcnow().isoformat() + "Z", 
        "recipients": {
            "email": [
                "greeshmanjali@amida.com"                     
            ]
        }
    }
    
    # detailed error information for error and log alerts
    if error_details and trigger_type in ["error", "log"]:
        alert_payload["errorMessage"] = {
            "message": error_details,                         
            "code": trigger_type.upper(),                      
            "stackTrace": error_details                        
        }
    
    # Send alert to Logic App via HTTP POST
    try:
        # Make HTTP request to Logic App trigger endpoint
        response = requests.post(
            url=logic_app_url,                                 
            json=alert_payload,                                
            timeout=30                                         
        )
        response.raise_for_status()
        logger.info(f"✅ {trigger_type.upper()} alert sent successfully to Logic App")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to send {trigger_type} alert to Logic App: {str(e)}")
        return False
        
    except Exception as e:
        logger.error(f"❌ Unexpected error sending {trigger_type} alert: {str(e)}")
        return False

def with_alerts(
    logic_app_url: str = "https://prod-47.eastus2.logic.azure.com:443/workflows/09f114d3198a487db73ec504e0277148/triggers/Receive_Medicaid_Alert_Request/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FReceive_Medicaid_Alert_Request%2Frun&sv=1.0&sig=QdaGnWj1_gZp0nzZZCWzkgn-1nj_r9LGG-al5f1NPrE",
    send_success_alerts: bool = True     
):
    """
    Decorator that automatically adds alert functionality to any Dagster asset
    
    This decorator wraps your asset function and:
    1. Runs your original asset function
    2. Sends SUCCESS alert if it completes without errors
    3. Sends ERROR alert if it fails, then re-throws the error to Dagster
    
    Usage:
        @asset(name="my_asset")
        @with_alerts()                   # Just add this line!
        def my_asset(context):
            # Your asset code here
            return result
    
    Args:
        logic_app_url: Azure Logic App endpoint URL (has default)
        send_success_alerts: Whether to send alerts on successful completion
    
    Returns:
        Decorated function with automatic alert capabilities
    """
    
    def decorator(asset_function):
        """
        Inner decorator function that wraps the asset
        """
        
        @wraps(asset_function)           
        def wrapper(context, *args, **kwargs):
            """
            Wrapper function that runs instead of the original asset
            This adds the alert logic around your asset execution
            """
            
            logger = get_dagster_logger()
            asset_name = asset_function.__name__        
            
            try:
                logger.info(f"Starting execution of asset: {asset_name}")
                result = asset_function(context, *args, **kwargs)
                logger.info(f"Asset {asset_name} completed successfully")
                
                # If asset completed successfully and success alerts are enabled
                if send_success_alerts:
                    send_alert(
                        logic_app_url=logic_app_url,
                        trigger_type="info",                   
                        message=f"Asset '{asset_name}' completed successfully.",
                        pipeline_name=asset_name,
                        run_id=context.run_id                   
                    )
                
                # Return the original result from your asset
                return result
                
            except Exception as error:
                # If your asset fails for any reason, we catch the exception here
                # Log the error in Dagster logs
                error_message = str(error)
                logger.error(f"Asset '{asset_name}' failed with error: {error_message}")
        
                # Send notification to external systems (email, Teams, Slack) via Logic App
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

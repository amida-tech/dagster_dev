import os
from dagster import resource
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization

@resource()
def snowpark_session(context):
    """
    Dagster resource that creates a Snowpark Session via key-pair auth
    """
    # 1) Read your env vars (Dagster+ injects these from the secret store)
    sf_account   = os.getenv("SNOWFLAKE_ACCOUNT")
    sf_service_user      = os.getenv("SNOWFLAKE_USER")
    sf_private_key_text  = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    sf_role      = os.getenv("SNOWFLAKE_ROLE")
    sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

    missing_values = [key for key,value in {
        "SNOWFLAKE_ACCOUNT":   sf_account,
        "SNOWFLAKE_USER":      sf_service_user,
        "SNOWFLAKE_PRIVATE_KEY": sf_private_key_text,
        "SNOWFLAKE_ROLE":      sf_role,
        "SNOWFLAKE_WAREHOUSE": sf_warehouse,
    }.items() if not value]
    if missing_values:
        raise Exception(f"Missing Snowflake config/secrets: {missing_values}")

    # 2) Convert PEM → DER bytes
    private_key_obj = serialization.load_pem_private_key(
        sf_private_key_text.encode("utf-8"), password=None
    )
    private_key_bytes = private_key_obj.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    # 3) Build the session using .config
    session = (
        Session.builder
        .config("account", sf_account)
        .config("user", sf_service_user)
        .config("private_key", private_key_bytes)
        .config("role", sf_role)
        .config("warehouse", sf_warehouse)
        .create()
    )
    context.log.info(f"🔗 Snowpark session established for account {sf_account}")
    return session
import os
from dagster import resource, InitResourceContext
from dagster_azure.adls2 import ADLS2Resource, ADLS2SASToken

@resource
def adls2_resource(context: InitResourceContext) -> ADLS2Resource:
    account = os.getenv("ADLS2_ACCOUNT")
    token   = os.getenv("ADLS2_SAS_TOKEN")

    if not account or not token:
        raise Exception(
            "ADLS2_RESOURCE: Must set both ADLS2_ACCOUNT and ADLS2_SAS_TOKEN in environment or Dagster+ secrets."
        )

    return ADLS2Resource(
        storage_account=account,
        credential=ADLS2SASToken(token=token),
    )

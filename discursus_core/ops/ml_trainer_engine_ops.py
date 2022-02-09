from dagster import op, AssetMaterialization, Output

import boto3
from io import StringIO
import pandas as pd



@op(
    required_resource_keys = {"airtable_client"}
)
def create_records(context): 
    response_json = context.resources.airtable_client.create_record()
    context.log.info(response_json)

    return None
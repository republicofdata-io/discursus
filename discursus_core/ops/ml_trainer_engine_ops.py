from dagster import op, AssetMaterialization, Output

import boto3
from io import StringIO
import pandas as pd



@op(
    required_resource_keys = {"airtable_client"}
)
def create_records(context): 
    context.log.info("Hello")

    return None
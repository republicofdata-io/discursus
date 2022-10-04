from dagster import op, AssetMaterialization, Output

from io import StringIO
import pandas as pd

@op(
    required_resource_keys = {"aws_resource"}
)
def s3_put(context, df_data_asset, file_path): 
    bucket_name = 'discursus-io'
    df_data_asset = context.resources.aws_resource.s3_put(df_data_asset, bucket_name, file_path)
    return df_data_asset


@op(
    required_resource_keys = {"aws_resource"}
)
def s3_get(context): 
    bucket_name = 'discursus-io'
    file_path = context.op_config["file_path"].split("s3://" + bucket_name + "/")[1]

    df_data_asset = context.resources.aws_resource.s3_get(bucket_name, file_path)
    return df_data_asset
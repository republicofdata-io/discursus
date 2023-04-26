from dagster import asset, AssetIn, Output, FreshnessPolicy, AutoMaterializePolicy
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
import pandas as pd
import boto3
from io import StringIO

from discursus_data_platform.utils.resources import my_resources


@asset(
    description = "List of events mined on GDELT",
    key_prefix = ["gdelt"],
    group_name = "sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=10),
)
def gdelt_events(context):
    # Build source path
    latest_events_url = context.resources.gdelt_resource.get_url_to_latest_asset("events")
    gdelt_asset_filename_zip = str(latest_events_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_events = context.resources.gdelt_resource.mine_latest_asset(latest_events_url)
    df_latest_events_filtered = context.resources.gdelt_resource.filter_latest_events(df_latest_events, 14, ['US', 'CA'])
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_events_filtered, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_events = "alter pipe gdelt_events_pipe refresh;"
    context.resources.snowflake_resource.execute_query(q_load_gdelt_events)

    # Return asset
    return Output(
        value = df_latest_events_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_latest_events_filtered.index.size
        }
    )


@asset(
    ins = {"gdelt_events": AssetIn(key_prefix = "gdelt")},
    description = "List of mentions mined from GDELT",
    key_prefix = ["gdelt"],
    group_name = "sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def gdelt_mentions(context, gdelt_events):
    # Build source path
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_mentions = context.resources.gdelt_resource.mine_latest_asset(latest_mentions_url)
    df_latest_mentions_filtered = context.resources.gdelt_resource.filter_latest_mentions(df_latest_mentions, gdelt_events)
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_mentions_filtered, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_mentions_events = "alter pipe gdelt_mentions_pipe refresh;"
    context.resources.snowflake_resource.execute_query(q_load_gdelt_mentions_events)

    # Return asset
    return Output(
        value = df_latest_mentions_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_latest_mentions_filtered.index.size
        }
    )

from dagster import op, AssetMaterialization, Output

from io import StringIO
import pandas as pd

@op(
    required_resource_keys = {"gdelt_resource"}
)
def get_url_to_latest_events(context): 
    latest_events_url = context.resources.gdelt_resource.get_url_to_latest_asset("events")
    return latest_events_url


@op(
    required_resource_keys = {"gdelt_resource"}
)
def get_url_to_latest_mentions(context): 
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    return latest_mentions_url


@op
def build_file_path(context, gdelt_asset_url): 
    gdelt_asset_filename_zip = str(gdelt_asset_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_file_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    return gdelt_asset_file_path


@op(
    required_resource_keys = {"gdelt_resource"}
)
def mine_latest_asset(context, latest_events_url): 
    df_latest_events = context.resources.gdelt_resource.mine_latest_asset(latest_events_url)
    return df_latest_events


@op(
    required_resource_keys = {"gdelt_resource"}
)
def filter_latest_events(context, df_latest_events): 
    df_latest_events_filtered = context.resources.gdelt_resource.filter_latest_events(df_latest_events, 14, ['US', 'CA'])
    return df_latest_events_filtered


@op(
    required_resource_keys = {"gdelt_resource"}
)
def filter_latest_mentions(context, df_latest_mentions, df_latest_events_filtered): 
    df_latest_mentions_filtered = context.resources.gdelt_resource.filter_latest_mentions(df_latest_mentions, df_latest_events_filtered)
    return df_latest_mentions_filtered
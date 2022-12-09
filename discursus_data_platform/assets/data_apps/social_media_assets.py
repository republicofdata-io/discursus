from dagster import (
    asset, 
    Output, 
    MetadataValue
)
import resources.my_resources
import pandas as pd 
from datetime import date
import PIL.Image as Image
import io

from dagster_hex.types import HexOutput
from dagster_hex.resources import DEFAULT_POLL_INTERVAL


@asset(
    description = "Hex daily assets refresh",
    group_name = "data_apps",
    resource_defs = {
        'hex_resource': resources.my_resources.my_hex_resource
    },
)
def hex_daily_assets_refresh(context):
    hex_output: HexOutput = context.resources.hex_resource.run_and_poll(
        project_id = "fa81df75-74df-4bb7-b7fe-9920dc00d99e",
        inputs = None,
        update_cache = True,
        kill_on_timeout = True,
        poll_interval = DEFAULT_POLL_INTERVAL,
        poll_timeout = None,
    )
    asset_name = ["hex", hex_output.run_response["projectId"]]

    return Output(
        value = asset_name, 
        metadata = {
            "run_url": MetadataValue.url(hex_output.run_response["runUrl"]),
            "run_status_url": MetadataValue.url(
                hex_output.run_response["runStatusUrl"]
            ),
            "trace_id": MetadataValue.text(hex_output.run_response["traceId"]),
            "run_id": MetadataValue.text(hex_output.run_response["runId"]),
            "elapsed_time": MetadataValue.int(
                hex_output.status_response["elapsedTime"]
            ),
        },
    )


@asset(
    non_argument_deps = {"hex_daily_assets_refresh"},
    description = "Twitter share of daily summary assets",
    group_name = "data_apps",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'twitter_resource': resources.my_resources.my_twitter_resource
    },
)
def twitter_share_daily_assets(context):
    # Retrieve daily summary assets
    today = date.today().strftime("%Y%m%d")
    protest_movements_map_file_name = "top_protests_map.png"
    protest_movements_map_file_path = "assets/daily_summary/" + today + "_" + protest_movements_map_file_name
    protest_movements_csv_file_path = "assets/daily_summary/" + today + "_top_protests.csv"

    protest_movements_map = context.resources.aws_resource.s3_get(
            bucket_name = 'discursus-io', 
            file_path = protest_movements_map_file_path,
            object_type = 'png')
    image = Image.open(io.BytesIO(protest_movements_map))
    image.save(protest_movements_map_file_name)
    
    df_protest_movements = context.resources.aws_resource.s3_get(
            bucket_name = 'discursus-io', 
            file_path = protest_movements_csv_file_path,
            object_type = 'csv',
            dataframe_conversion = True)


    # Upload map to Twitter
    twitter_media = context.resources.twitter_resource.upload_media(protest_movements_map_file_name)
    context.log.info(twitter_media)
    

    # Create text for tweet
    tweet = f""" Here are the top protest movements for {today}.
    
    The {df_protest_movements['movement_name'].iloc[0]} protest movement has been the most active with {df_protest_movements['events_count'].iloc[0]} events captured. 
    
    Visit the dashboard for further insights: https://app.hex.tech/bca77dcf-0dcc-4d33-8a23-c4c73f6b11c3/app/d6824152-38b4-4f39-8f5e-c3a963cc48c8/latest"""


    # Post tweet
    twitter_status = context.resources.twitter_resource.post(tweet, [twitter_media.media_id_string])
    context.log.info(twitter_status)


    # Return asset
    return Output(
        value = twitter_status, 
        metadata = {
            "Id": twitter_status.id_str
        }
    )
    
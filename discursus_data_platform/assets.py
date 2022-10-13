from dagster import asset, Output
import resources.my_resources
from ops.gdelt_ops import build_file_path

@asset(
    description = "List of events mined on GDELT",
    group_name = "sources",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource
    }
)
def gdelt_events(context):
    latest_events_url = context.resources.gdelt_resource.get_url_to_latest_asset("events")
    latest_events_source_path = build_file_path(latest_events_url)
    df_latest_events = context.resources.gdelt_resource.mine_latest_asset(latest_events_url)
    df_latest_events_filtered = context.resources.gdelt_resource.filter_latest_events(df_latest_events, 14, ['US', 'CA'])
    context.resources.aws_resource.s3_put(df_latest_events_filtered, 'discursus-io', latest_events_source_path)

    return Output(
        df_latest_events_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + latest_events_source_path,
            "rows": df_latest_events_filtered.index.size
        }
    )




@asset(
    non_argument_deps = {"gdelt_events"},
    description = "List of mentions mined from GDELT",
    group_name = "sources",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource
    }
)
def gdelt_mentions(context):
    file_path = context.op_config["file_path"].split("s3://discursus-io/")[1]

    df_latest_events_filtered = context.resources.aws_resource.s3_get('discursus-io', file_path)
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    latest_mentions_source_path = build_file_path(latest_mentions_url)
    df_latest_mentions = context.resources.gdelt_resource.mine_latest_asset(latest_mentions_url)
    df_latest_mentions_filtered = context.resources.gdelt_resource.filter_latest_mentions(df_latest_mentions, df_latest_events_filtered)
    context.resources.aws_resource.s3_put(df_latest_mentions_filtered, 'discursus-io', latest_mentions_source_path)

    return Output(
        df_latest_mentions_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + latest_mentions_source_path,
            "rows": df_latest_mentions_filtered.index.size
        }
    )



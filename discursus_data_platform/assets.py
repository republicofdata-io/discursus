from dagster import asset

import resources.my_resources

@asset
def my_asset():
    return [1, 2, 3]

@asset(
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource
    },
    config = {
        "ops": {
            "materialize_data_asset": {
                "config": {
                    "asset_key_parent": "sources",
                    "asset_key_child": "gdelt_events",
                    "asset_description": "List of events mined on GDELT"
                }
            }
        }
    }
)
def gdelt_events():
    latest_events_url = get_url_to_latest_events()
    latest_events_source_path = build_file_path(latest_events_url)
    df_latest_events = mine_latest_asset(latest_events_url)
    df_latest_events_filtered = filter_latest_events(df_latest_events)
    s3_put(df_latest_events_filtered, latest_events_source_path)
    materialize_data_asset(df_latest_events_filtered, latest_events_source_path)



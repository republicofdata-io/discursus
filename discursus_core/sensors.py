from dagster import AssetKey, asset_sensor, RunRequest

from jobs import (
    mine_gdelt_mentions, 
    load_gdelt_assets_to_snowflake, 
    classify_gdelt_mentions_relevancy,
    load_classified_gdelt_mentions_to_snowflake
)

@asset_sensor(asset_key = AssetKey(["sources", "gdelt_events"]), job = mine_gdelt_mentions)
def mining_gdelt_mentions_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
        run_config={
            "ops": {
                "get_saved_data_asset": {
                    "config": {
                        "asset_key": asset_event.dagster_event.asset_key.path,
                        "asset_materialization_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
                    }
                },
                "get_url_to_latest_asset": {
                    "config": {
                        "gdelt_asset": "mentions"
                    }
                },
                "materialize_data_asset": {
                    "config": {
                        "asset_key_parent": "sources",
                        "asset_key_child": "gdelt_mentions",
                        "asset_description": "List of enhanced articles mined from GDELT"
                    }
                }
            }
        }
    )

@asset_sensor(asset_key = AssetKey(["sources", "gdelt_events"]), job = load_gdelt_assets_to_snowflake)
def load_gdelt_assets_to_snowflake_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
    )

@asset_sensor(asset_key = AssetKey(["sources", "gdelt_mentions"]), job = classify_gdelt_mentions_relevancy)
def classify_gdelt_mentions_relevancy_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
        run_config={
            "ops": {
                "classify_mentions_relevancy": {
                    "config": {
                        "asset_key": asset_event.dagster_event.asset_key.path,
                        "asset_materialization_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
                    }
                }
            }
        }
    )

@asset_sensor(asset_key = AssetKey(["sources", "ml_enrichment_files"]), job = load_classified_gdelt_mentions_to_snowflake)
def load_classified_gdelt_mentions_to_snowflake_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
    )
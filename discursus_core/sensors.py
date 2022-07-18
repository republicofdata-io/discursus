from dagster import AssetKey, asset_sensor, RunRequest

from jobs import mine_gdelt_mentions, load_gdelt_assets_to_snowflake, classify_gdelt_mentions_relevancy

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
                }
            }
        },
    )

@asset_sensor(asset_key = AssetKey(["sources", "gdelt_events"]), job = load_gdelt_assets_to_snowflake)
def load_gdelt_assets_to_snowflake_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
        run_config={
            "ops": {
                "launch_gdelt_events_snowpipe": {
                    "config": {
                        "asset_key": asset_event.dagster_event.asset_key.path,
                        "asset_materialization_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
                    }
                }
            }
        },
    )

@asset_sensor(asset_key = AssetKey(["sources", "gdelt_articles"]), job = classify_gdelt_mentions_relevancy)
def classify_gdelt_mentions_relevancy_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
        run_config={
            "ops": {
                "get_saved_data_asset": {
                    "config": {
                        "asset_key": asset_event.dagster_event.asset_key.path,
                        "asset_materialization_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
                    }
                }
            }
        },
    )

@asset_sensor(asset_key = AssetKey(["sources", "ml_enrichment_files"]), job = load_classified_gdelt_mentions_to_snowflake)
def load_classified_gdelt_mentions_to_snowflake_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
        run_config={
            "ops": {
                "launch_ml_enriched_articles_snowpipe": {
                    "config": {
                        "asset_key": asset_event.dagster_event.asset_key.path,
                        "asset_materialization_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
                    }
                }
            }
        }
from dagster import AssetKey, asset_sensor, RunRequest
from jobs import (
    gdelt_mentions_job, 
    load_gdelt_assets_to_snowflake,
    gdelt_enhanced_mentions_job, 
    classify_gdelt_mentions_relevancy,
    load_classified_gdelt_mentions_to_snowflake
)

@asset_sensor(asset_key = AssetKey(["gdelt_events"]), job = gdelt_mentions_job)
def gdelt_mentions_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
        run_config={
            "ops": {
                "gdelt_mentions": {
                    "config": {
                        "file_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
                    }
                }
            }
        }
    )

@asset_sensor(asset_key = AssetKey(["gdelt_mentions"]), job = gdelt_enhanced_mentions_job)
def gdelt_enhanced_mentions_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
        run_config={
            "ops": {
                "gdelt_enhanced_mentions": {
                    "config": {
                        "file_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
                        "asset_materialization_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
                    }
                }
            }
        }
    )

@asset_sensor(asset_key = AssetKey(["sources", "gdelt_enhanced_mentions"]), job = load_gdelt_assets_to_snowflake)
def load_gdelt_assets_to_snowflake_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
    )

@asset_sensor(asset_key = AssetKey(["sources", "gdelt_enhanced_mentions"]), job = classify_gdelt_mentions_relevancy)
def classify_gdelt_mentions_relevancy_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
        run_config={
            "ops": {
                "classify_mentions_relevancy": {
                    "config": {
                        "asset_materialization_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
                    }
                }
            }
        }
    )

@asset_sensor(asset_key = AssetKey(["sources", "gdelt_ml_enriched_mentions"]), job = load_classified_gdelt_mentions_to_snowflake)
def load_classified_gdelt_mentions_to_snowflake_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
    )
from dagster import AssetKey, asset_sensor, RunRequest

from jobs import enrich_mined_data


@asset_sensor(asset_key = AssetKey(["sources", "gdelt_articles"]), job = enrich_mined_data)
def gdelt_enhanced_articles_sensor(context, asset_event):
    yield RunRequest(
        run_key = asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text,
        run_config={
            "ops": {
                "classify_protest_relevancy": {
                    "config": {
                        "asset_key": asset_event.dagster_event.asset_key.path,
                        "asset_materialization_path": asset_event.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.text
                    }
                }
            }
        },
    )
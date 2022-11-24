from dagster import job, define_asset_job
import resources.my_resources
from ops.airtable_ops import (
    get_latest_ml_enrichments, 
    create_records
)


source_gdelt_assets_job = define_asset_job(
    name = "source_gdelt_assets_job", 
    selection = [
        "gdelt_events",
        "gdelt_mentions",
        "gdelt_mentions_enhanced",
        "gdelt_mentions_relevancy_ml_jobs",
        "snowpipe_transfers_gdelt_assets"
    ]
)


enrich_gdelt_assets_job = define_asset_job(
    name = "enrich_gdelt_assets_job", 
    selection = [
        "gdelt_mentions_relevancy",
        "snowpipe_transfers_classified_gdelt_mentions"
    ]
)


build_data_warehouse_job = define_asset_job(
    name = "build_data_warehouse_job", 
    selection = [
        "dw_seeds",
        "dw_staging_layer",
        "dw_integration_layer",
        "dw_entity_layer",
        "dw_data_tests",
        "dw_clean_up",
        "hex_main_dashboard_refresh"
    ]
)


share_daily_summary_assets_job = define_asset_job(
    name = "share_daily_summary_assets_job", 
    selection = [
        "hex_daily_assets_refresh"
    ]
)


@job(
    description = "Feed our ML training engine",
    resource_defs = {
        'airtable_client': resources.my_resources.my_airtable_resource
    }
)
def feed_ml_trainer_engine():
    df_latest_enriched_events_sample = get_latest_ml_enrichments()
    create_records(df_latest_enriched_events_sample)
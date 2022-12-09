from dagster import job, define_asset_job
import resources.my_resources
from ops.airtable_ops import (
    get_latest_ml_enrichments, 
    create_records
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


@job(
    description = "Feed our ML training engine",
    resource_defs = {
        'airtable_client': resources.my_resources.my_airtable_resource
    }
)
def feed_ml_trainer_engine():
    df_latest_enriched_events_sample = get_latest_ml_enrichments()
    create_records(df_latest_enriched_events_sample)
from dagster import (
    AssetSelection, 
    build_asset_reconciliation_sensor,
    file_relative_path,
    repository, 
    with_resources
)
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

from assets.sources import gdelt_source_assets, airbyte_source_assets
from assets.data_prep import gdelt_enriched_assets
from assets.data_apps import public_dashboard_assets, social_media_assets


DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")

protest_assets = with_resources(
    load_assets_from_dbt_project(
        project_dir = DBT_PROJECT_DIR, 
        profiles_dir = DBT_PROFILES_DIR, 
        key_prefix = ["data_warehouse"],
        use_build_command = True
    ) + [
        gdelt_source_assets.gdelt_events,
        gdelt_source_assets.gdelt_mentions,
        airbyte_source_assets.protest_groupings,
        gdelt_enriched_assets.gdelt_mentions_enhanced,
        gdelt_enriched_assets.gdelt_ml_enriched_mentions,
        public_dashboard_assets.hex_main_dashboard_refresh,
        social_media_assets.hex_daily_assets_refresh,
        social_media_assets.twitter_share_daily_assets
    ],
    resource_defs = {
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROFILES_DIR,
                "profiles_dir": DBT_PROJECT_DIR,
            },
        ),
        "io_manager": s3_pickle_io_manager.configured(
            {"s3_bucket": "discursus-io", "s3_prefix": "platform"}
        ),
        "s3": s3_resource,
    },
)

asset_sensor = [
    build_asset_reconciliation_sensor(
        asset_selection=AssetSelection.all(),
        name="asset_reconciliation_sensor",
        minimum_interval_seconds = 60 * 5
    )
]


@repository
def discursus_repo():
    return protest_assets + asset_sensor
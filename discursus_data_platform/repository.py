from dagster import (
    repository, 
    AssetSelection, 
    build_asset_reconciliation_sensor,
    load_assets_from_package_module,
    with_resources,
    file_relative_path
)
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

from assets.sources import gdelt_source_assets, airbyte_source_assets
from assets.data_prep import gdelt_enriched_assets
from assets.data_apps import public_dashboard_assets, social_media_assets


DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")

protest_wh_assets = with_resources(
    load_assets_from_dbt_project(
        project_dir = DBT_PROJECT_DIR, 
        profiles_dir = DBT_PROFILES_DIR, 
        key_prefix = ["data_warehouse"],
        use_build_command = True
    ),
    {
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROFILES_DIR,
                "profiles_dir": DBT_PROJECT_DIR,
            },
        ),
    },
)

platform_assets = [
    gdelt_source_assets.gdelt_events,
    gdelt_source_assets.gdelt_mentions,
    airbyte_source_assets.protest_groupings,
    gdelt_enriched_assets.gdelt_mentions_enhanced,
    gdelt_enriched_assets.gdelt_ml_enriched_mentions,
    public_dashboard_assets.hex_main_dashboard_refresh,
    social_media_assets.hex_daily_assets_refresh,
    social_media_assets.twitter_share_daily_assets,

    build_asset_reconciliation_sensor(
        asset_selection=AssetSelection.all(),
        name="asset_reconciliation_sensor"
    )        
]


@repository
def discursus_repo():
    return protest_wh_assets + platform_assets
from dagster import repository

from assets.sources import gdelt_source_assets
from assets.data_prep import gdelt_enriched_assets
from assets.warehouses import protest_wh_assets
from assets.data_apps import public_dashboard_assets
from assets.data_apps import social_media_assets


@repository
def discursus_repo():
    assets = [
        gdelt_source_assets.gdelt_events,
        gdelt_source_assets.gdelt_mentions,
        gdelt_enriched_assets.gdelt_mentions_enhanced,
        gdelt_enriched_assets.gdelt_mentions_relevancy,
        protest_wh_assets.dw_seeds,
        protest_wh_assets.dw_staging_layer,
        protest_wh_assets.dw_integration_layer,
        protest_wh_assets.dw_entity_layer,
        protest_wh_assets.dw_data_tests,
        protest_wh_assets.dw_clean_up,
        public_dashboard_assets.hex_main_dashboard_refresh,
        social_media_assets.hex_daily_assets_refresh,
        social_media_assets.twitter_share_daily_assets
    ]

    return assets
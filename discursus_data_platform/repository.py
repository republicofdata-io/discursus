from dagster import repository
from assets.sources.gdelt_source_assets import(
    gdelt_events,
    gdelt_mentions
)
from assets.data_prep.gdelt_enriched_assets import(
    gdelt_mentions_enhanced,
    gdelt_mentions_relevancy
)
from assets.warehouses.protest_wh_assets import(
    dw_seeds,
    dw_staging_layer,
    dw_integration_layer,
    dw_entity_layer,
    dw_data_tests,
    dw_clean_up
)
from assets.data_apps.public_dashboard_assets import(
    hex_main_dashboard_refresh
)
from assets.data_apps.social_media_assets import(
    hex_daily_assets_refresh,
    twitter_share_daily_assets
)
from jobs import (
    build_data_warehouse_job,
    feed_ml_trainer_engine
)
from schedules import (
    feed_ml_trainer_engine_schedule,
    build_data_warehouse_schedule
)


@repository
def discursus_repo():
    assets = [
        gdelt_events,
        gdelt_mentions,
        gdelt_mentions_enhanced,
        gdelt_mentions_relevancy,
        dw_seeds,
        dw_staging_layer,
        dw_integration_layer,
        dw_entity_layer,
        dw_data_tests,
        dw_clean_up,
        hex_main_dashboard_refresh,
        hex_daily_assets_refresh,
        twitter_share_daily_assets
    ]
    jobs = [
        build_data_warehouse_job,
        feed_ml_trainer_engine
    ]
    schedules = [
        build_data_warehouse_schedule,
        feed_ml_trainer_engine_schedule
    ]

    return assets + jobs + schedules
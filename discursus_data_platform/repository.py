from dagster import repository
from assets.source_gdelt_assets import (
    gdelt_events,
    gdelt_mentions,
    gdelt_mentions_enhanced,
    gdelt_mentions_relevancy_ml_jobs,
    snowpipe_transfers_gdelt_assets
)
from assets.enrich_gdelt_assets import (
    gdelt_mentions_relevancy,
    snowpipe_transfers_classified_gdelt_mentions
)
from assets.dw_assets import (
    dw_seeds,
    dw_staging_layer,
    dw_integration_layer,
    dw_entity_layer,
    dw_data_tests,
    dw_clean_up
)
from assets.hex_assets import (
    hex_main_dashboard_refresh,
    hex_daily_assets_refresh
)
from assets.twitter_assets import (
    twitter_share_daily_assets
)
from jobs import (
    source_gdelt_assets_job,
    enrich_gdelt_assets_job,
    build_data_warehouse_job,
    share_daily_summary_assets_job,
    feed_ml_trainer_engine
)
from schedules import (
    source_gdelt_assets_schedule, 
    enrich_gdelt_assets_schedule,
    feed_ml_trainer_engine_schedule,
    build_data_warehouse_schedule,
    share_daily_summary_assets_schedule
)


@repository
def discursus_repo():
    source_gdelt_assets = [
        gdelt_events,
        gdelt_mentions,
        gdelt_mentions_enhanced,
        gdelt_mentions_relevancy_ml_jobs,
        snowpipe_transfers_gdelt_assets
    ]
    prep_gdelt_assets = [
        gdelt_mentions_relevancy,
        snowpipe_transfers_classified_gdelt_mentions
    ]
    dw_assets = [
        dw_seeds,
        dw_staging_layer,
        dw_integration_layer,
        dw_entity_layer,
        dw_data_tests,
        dw_clean_up
    ]
    hex_assets = [
        hex_main_dashboard_refresh,
        hex_daily_assets_refresh
    ]
    twitter_assets = [
        twitter_share_daily_assets
    ]
    jobs = [
        source_gdelt_assets_job,
        enrich_gdelt_assets_job,
        build_data_warehouse_job,
        share_daily_summary_assets_job,
        feed_ml_trainer_engine
    ]
    schedules = [
        source_gdelt_assets_schedule, 
        enrich_gdelt_assets_schedule,
        feed_ml_trainer_engine_schedule,
        build_data_warehouse_schedule,
        share_daily_summary_assets_schedule
    ]

    return source_gdelt_assets + prep_gdelt_assets + dw_assets + hex_assets + twitter_assets + jobs + schedules
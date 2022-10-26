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
    dw_clean_up,
    hex_project_refresh
)
from jobs import (
    source_gdelt_assets_job,
    enrich_gdelt_assets_job,
    build_data_warehouse_job,
    refresh_hex_job,
    feed_ml_trainer_engine
)
from schedules import (
    source_gdelt_assets_schedule, 
    enrich_gdelt_assets_schedule,
    feed_ml_trainer_engine_schedule,
    build_data_warehouse_schedule
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
        dw_clean_up,
        hex_project_refresh
    ]
    jobs = [
        source_gdelt_assets_job,
        enrich_gdelt_assets_job,
        build_data_warehouse_job,
        refresh_hex_job,
        feed_ml_trainer_engine
    ]
    schedules = [
        source_gdelt_assets_schedule, 
        enrich_gdelt_assets_schedule,
        feed_ml_trainer_engine_schedule,
        build_data_warehouse_schedule
    ]

    return source_gdelt_assets + prep_gdelt_assets + dw_assets + jobs + schedules
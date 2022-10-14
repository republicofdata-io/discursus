from dagster import repository
from assets import (
    gdelt_events,
    gdelt_mentions,
    gdelt_mentions_enhanced,
    gdelt_mentions_relevant
)
from jobs import (
    load_gdelt_assets_to_snowflake, 
    classify_gdelt_mentions_relevancy, 
    load_classified_gdelt_mentions_to_snowflake,
    build_data_warehouse,
    feed_ml_trainer_engine
)
from sensors import (
    gdelt_mentions_sensor,
    gdelt_mentions_enhanced_sensor,
    load_gdelt_assets_to_snowflake_sensor,
    classify_gdelt_mentions_relevancy_sensor,
    load_classified_gdelt_mentions_to_snowflake_sensor
)
from schedules import (
    gdelt_events_schedule, 
    gdelt_mentions_relevant_schedule,
    feed_ml_trainer_engine_schedule,
    build_data_warehouse_schedule
)


@repository
def discursus_repo():
    assets = [
        gdelt_events,
        gdelt_mentions,
        gdelt_mentions_enhanced,
        gdelt_mentions_relevant
    ]
    jobs = [
        load_gdelt_assets_to_snowflake, 
        classify_gdelt_mentions_relevancy, 
        load_classified_gdelt_mentions_to_snowflake,
        build_data_warehouse,
        feed_ml_trainer_engine
    ]
    sensors = [
        gdelt_mentions_sensor,
        gdelt_mentions_enhanced_sensor,
        load_gdelt_assets_to_snowflake_sensor,
        classify_gdelt_mentions_relevancy_sensor,
        load_classified_gdelt_mentions_to_snowflake_sensor
    ]
    schedules = [
        gdelt_events_schedule, 
        gdelt_mentions_relevant_schedule,
        feed_ml_trainer_engine_schedule,
        build_data_warehouse_schedule
    ]

    return assets + jobs + sensors + schedules
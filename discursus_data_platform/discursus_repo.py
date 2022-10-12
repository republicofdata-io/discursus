from dagster import repository

from assets import (
    gdelt_events
)

from jobs import (
    mine_gdelt_mentions,
    enhance_gdelt_mentions,
    load_gdelt_assets_to_snowflake, 
    classify_gdelt_mentions_relevancy, 
    get_relevancy_classification_of_gdelt_mentions,
    load_classified_gdelt_mentions_to_snowflake,
    build_data_warehouse,
    feed_ml_trainer_engine
)
from sensors import (
    mining_gdelt_mentions_sensor,
    load_gdelt_assets_to_snowflake_sensor,
    enhance_gdelt_mentions_sensor,
    classify_gdelt_mentions_relevancy_sensor,
    load_classified_gdelt_mentions_to_snowflake_sensor
)
from schedules import (
    mine_gdelt_events_schedule, 
    get_relevancy_classification_of_gdelt_mentions_schedule,
    feed_ml_trainer_engine_schedule,
    build_data_warehouse_schedule
)


@repository
def discursus_repo():
    assets = [
        gdelt_events
    ]
    jobs = [
        mine_gdelt_mentions,
        enhance_gdelt_mentions,
        load_gdelt_assets_to_snowflake, 
        classify_gdelt_mentions_relevancy, 
        get_relevancy_classification_of_gdelt_mentions,
        load_classified_gdelt_mentions_to_snowflake,
        build_data_warehouse,
        feed_ml_trainer_engine
    ]
    sensors = [
        mining_gdelt_mentions_sensor,
        load_gdelt_assets_to_snowflake_sensor,
        enhance_gdelt_mentions_sensor,
        classify_gdelt_mentions_relevancy_sensor,
        load_classified_gdelt_mentions_to_snowflake_sensor
    ]
    schedules = [
        mine_gdelt_events_schedule, 
        get_relevancy_classification_of_gdelt_mentions_schedule,
        feed_ml_trainer_engine_schedule,
        build_data_warehouse_schedule
    ]

    return assets + jobs + sensors + schedules
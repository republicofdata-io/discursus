from dagster import repository

from jobs import (
    mine_gdelt_events, 
    mine_gdelt_mentions,
    enhance_gdelt_mentions,
    load_gdelt_assets_to_snowflake, 
    classify_gdelt_mentions_relevancy, 
    get_relevancy_classification_of_gdelt_mentions,
    load_classified_gdelt_mentions_to_snowflake,
    build_data_warehouse
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
    build_data_warehouse_schedule
)


@repository
def dpf_repository():
    jobs = [
        mine_gdelt_events, 
        mine_gdelt_mentions,
        enhance_gdelt_mentions,
        load_gdelt_assets_to_snowflake, 
        classify_gdelt_mentions_relevancy, 
        get_relevancy_classification_of_gdelt_mentions,
        load_classified_gdelt_mentions_to_snowflake,
        build_data_warehouse
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
        build_data_warehouse_schedule
    ]

    return jobs + sensors + schedules
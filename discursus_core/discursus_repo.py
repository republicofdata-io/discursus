from dagster import repository

from jobs import mine_gdelt_data, enrich_mined_data, build_data_warehouse, get_enriched_mined_data, feed_ml_trainer_engine
from sensors import gdelt_enhanced_articles_sensor
from schedules import mine_gdelt_data_schedule, get_enriched_mined_data_schedule, feed_ml_trainer_engine_schedule, build_data_warehouse_schedule


@repository
def discursus_repository():
    jobs = [
        mine_gdelt_data,
        enrich_mined_data,
        build_data_warehouse,
        get_enriched_mined_data,
        feed_ml_trainer_engine
    ]
    sensors = [
        gdelt_enhanced_articles_sensor
    ]
    schedules = [
        mine_gdelt_data_schedule, 
        get_enriched_mined_data_schedule,
        feed_ml_trainer_engine_schedule,
        build_data_warehouse_schedule,
    ]

    return jobs + sensors + schedules
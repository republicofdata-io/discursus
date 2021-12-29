from dagster import repository

from jobs import mine_gdelt_data, enrich_mined_data, build_data_warehouse
from sensors import gdelt_enhanced_articles_sensor
from schedules import mine_gdelt_data_schedule, build_data_warehouse_schedule


@repository
def discursus_repository():
    jobs = [
        mine_gdelt_data,
        enrich_mined_data,
        build_data_warehouse
    ]
    sensors = [
        gdelt_enhanced_articles_sensor
    ]
    schedules = [
        mine_gdelt_data_schedule,  
        build_data_warehouse_schedule,
    ]

    return jobs + sensors + schedules
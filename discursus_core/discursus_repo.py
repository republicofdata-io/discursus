from dagster import repository

from pipelines import mine_gdelt_data
from pipelines import build_data_warehouse
from schedules import mine_gdelt_data_schedule, build_data_warehouse_schedule


@repository
def discursus_repository():
    pipelines = [
        mine_gdelt_data, 
        build_data_warehouse
    ]
    schedules = [
        mine_gdelt_data_schedule,  
        build_data_warehouse_schedule
    ]

    return pipelines + schedules
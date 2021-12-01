from dagster import ScheduleDefinition
from jobs import mine_gdelt_data
from jobs import build_data_warehouse

mine_gdelt_data_schedule = ScheduleDefinition(job = mine_gdelt_data, cron_schedule = "2,17,32,47 * * * *")
build_data_warehouse_schedule = ScheduleDefinition(job = build_data_warehouse, cron_schedule = "15 3,9,15,21 * * *")
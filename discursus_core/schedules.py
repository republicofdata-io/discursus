from dagster import ScheduleDefinition
from jobs import mine_gdelt_data, get_enriched_mined_data, feed_ml_trainer_engine, build_data_warehouse

mine_gdelt_data_schedule = ScheduleDefinition(job = mine_gdelt_data, cron_schedule = "2,17,32,47 * * * *")
get_enriched_mined_data_schedule = ScheduleDefinition(job = get_enriched_mined_data, cron_schedule = "7,22,37,52 * * * *")
feed_ml_trainer_engine_schedule = ScheduleDefinition(job = feed_ml_trainer_engine, cron_schedule = "28,58 * * * *")
build_data_warehouse_schedule = ScheduleDefinition(job = build_data_warehouse, cron_schedule = "15 3,9,15,21 * * *")
from dagster import (
    schedule, 
    ScheduleDefinition, 
    ScheduleEvaluationContext, 
    RunRequest
)
from jobs import (
    source_gdelt_assets_job, 
    enrich_gdelt_assets_job, 
    build_data_warehouse_job, 
    feed_ml_trainer_engine
)

source_gdelt_assets_schedule = ScheduleDefinition(job = source_gdelt_assets_job, cron_schedule = "2,17,32,47 * * * *")
enrich_gdelt_assets_schedule = ScheduleDefinition(job = enrich_gdelt_assets_job, cron_schedule = "7,22,37,52 * * * *")
feed_ml_trainer_engine_schedule = ScheduleDefinition(job = feed_ml_trainer_engine, cron_schedule = "13,28,43,58 * * * *")

@schedule(job=build_data_warehouse_job, cron_schedule="15 3,9,15,21 * * *")
def build_data_warehouse_schedule(context: ScheduleEvaluationContext):
    return RunRequest(
        run_key=None,
        run_config={
            "ops": {
                "dw_staging_layer": {"config": {"full_refresh_flag": False}},
                "dw_integration_layer": {"config": {"full_refresh_flag": False}},
                "dw_entity_layer": {"config": {"full_refresh_flag": False}}
            }
        }
    )

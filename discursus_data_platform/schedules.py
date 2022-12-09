from dagster import (
    schedule, 
    ScheduleDefinition, 
    ScheduleEvaluationContext, 
    RunRequest
)
from jobs import (
    build_data_warehouse_job,
    feed_ml_trainer_engine
)

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
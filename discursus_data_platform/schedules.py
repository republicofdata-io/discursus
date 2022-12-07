from dagster import (
    schedule, 
    ScheduleDefinition, 
    ScheduleEvaluationContext, 
    RunRequest
)
from jobs import (
    source_and_classify_relevancy_of_gdelt_assets_job, 
    get_relevancy_classifications_job, 
    build_data_warehouse_job,
    share_daily_summary_assets_job,
    feed_ml_trainer_engine
)

source_and_classify_relevancy_of_gdelt_assets_schedule = ScheduleDefinition(job = source_and_classify_relevancy_of_gdelt_assets_job, cron_schedule = "2,17,32,47 * * * *")
get_relevancy_classifications_schedule = ScheduleDefinition(job = get_relevancy_classifications_job, cron_schedule = "7,22,37,52 * * * *")
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

share_daily_summary_assets_schedule = ScheduleDefinition(job = share_daily_summary_assets_job, cron_schedule = "15 6 * * *")

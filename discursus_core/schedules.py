from dagster import schedule, ScheduleDefinition, ScheduleEvaluationContext, RunRequest
from jobs import mine_gdelt_data, get_enriched_mined_data, build_data_warehouse, feed_ml_trainer_engine

mine_gdelt_data_schedule = ScheduleDefinition(job = mine_gdelt_data, cron_schedule = "2,17,32,47 * * * *")

get_enriched_mined_data_schedule = ScheduleDefinition(job = get_enriched_mined_data, cron_schedule = "7,22,37,52 * * * *")
feed_ml_trainer_engine_schedule = ScheduleDefinition(job = feed_ml_trainer_engine, cron_schedule = "13,28,43,58 * * * *")

@schedule(job=build_data_warehouse, cron_schedule="15 3,9,15,21 * * *")
def build_data_warehouse_schedule(context: ScheduleEvaluationContext):
    return RunRequest(
        run_key=None,
        run_config={
            "ops": {
                "build_dw_staging_layer": {"config": {"full_refresh_flag": False}},
                "build_dw_integration_layer": {"config": {"full_refresh_flag": False}},
                "build_dw_warehouse_layer": {"config": {"full_refresh_flag": False}}
            }
        }
    )

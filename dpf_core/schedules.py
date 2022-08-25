from dagster import schedule, ScheduleDefinition, ScheduleEvaluationContext, RunRequest
from jobs import mine_gdelt_events, get_relevancy_classification_of_gdelt_mentions, build_data_warehouse

mine_gdelt_events_schedule = ScheduleDefinition(job = mine_gdelt_events, cron_schedule = "2,17,32,47 * * * *")

get_relevancy_classification_of_gdelt_mentions_schedule = ScheduleDefinition(job = get_relevancy_classification_of_gdelt_mentions, cron_schedule = "7,22,37,52 * * * *")

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
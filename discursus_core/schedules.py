from dagster import (
    schedule, 
    ModeDefinition, 
    PresetDefinition
)
from dagster_snowflake import snowflake_resource


prod_mode = ModeDefinition(
    name = 'prod',
    resource_defs = {
        'snowflake': snowflake_resource
    }
)

prod_presets = PresetDefinition.from_files(
    name='prod',
    mode='prod',
    config_files=['environments/prod.yaml'],
)


@schedule(
    cron_schedule = "2,17,32,47 * * * *",
    pipeline_name = "mine_gdelt_data", 
    mode = "prod"
)  # Every 15 minutes
def mine_gdelt_data_schedule(context):
    return prod_presets.run_config


@schedule(
    cron_schedule = "15 3,9,15,21 * * *",
    pipeline_name = "build_data_warehouse", 
    mode = "prod"
)  # Every hour
def build_data_warehouse_schedule(context):
    return prod_presets.run_config
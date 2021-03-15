import csv
import os

from dagster import (
    repository, 
    pipeline, 
    solid, 
    schedule, 
    ModeDefinition, 
    PresetDefinition, 
    file_relative_path
)
from dagster_shell import create_shell_command_solid
from dagster_snowflake import snowflake_resource
from dagster_dbt import dbt_cli_run, dbt_cli_test


DBT_PROFILES_DIR = "."
DBT_PROJECT_DIR = file_relative_path(__file__, "../dw")


###########################
# MODES AND RESOURCES
###########################
prod_mode = ModeDefinition(
    name='prod',
    resource_defs={
        'snowflake': snowflake_resource
    }
)

prod_presets = PresetDefinition.from_files(
    name='prod',
    mode='prod',
    config_files=['configs/prod.yaml'],
)


###########################
# DATA MINING
###########################
@pipeline(
    mode_defs=[prod_mode], 
    preset_defs=[prod_presets]
)
def data_mining_pipeline():
    #gdelt_miner = create_shell_script_solid(file_relative_path(__file__, "../miners/gdelt/gdelt_miner.zsh"), name="gdelt_miner_solid")
    gdelt_miner = create_shell_command_solid("zsh < miners/gdelt/gdelt_miner.zsh", name="gdelt_miner_solid") 
    gdelt_miner()


@schedule(
    cron_schedule="2,17,32,47 * * * *",
    pipeline_name="data_mining_pipeline", 
    mode="prod"
)  # Every 15 minutes
def schedule_data_mining_pipeline(context):
    return prod_presets.run_config



###########################
# 1. LOADING DATA TO DW
# 2. RUN DBT TRANSFORMATION
###########################
q_load_gdelt_events = "alter pipe gdelt_events_pipe refresh;"

@solid(required_resource_keys={"snowflake"})
def launch_snowpipe(context):
    context.resources.snowflake.execute_query(q_load_gdelt_events)


run_dbt_transformation = dbt_cli_run.configured(
    config_or_config_fn={"project-dir": DBT_PROJECT_DIR, "profiles-dir": DBT_PROFILES_DIR},
    name="run_dbt_transformation",
)


test_dbt_transformation = dbt_cli_test.configured(
    config_or_config_fn={"project-dir": DBT_PROJECT_DIR, "profiles-dir": DBT_PROFILES_DIR},
    name="test_dbt_transformation",
)


@pipeline(
    mode_defs=[prod_mode], 
    preset_defs=[prod_presets]
)
def load_data_to_dw_pipeline():
    snowpipe_result = launch_snowpipe()
    dbt_run_result = run_dbt_transformation(start_after=snowpipe_result)
    dbt_test_result = test_dbt_transformation(start_after=dbt_run_result)


@schedule(
    cron_schedule="15 3,9,15,21 * * *",
    pipeline_name="load_data_to_dw_pipeline", 
    mode="prod"
)  # Every hour
def schedule_load_data_to_dw_pipeline(context):
    return prod_presets.run_config


###########################
# REPOSITORY
###########################
@repository
def dio_repository():
    return [
        data_mining_pipeline, 
        schedule_data_mining_pipeline, 
        load_data_to_dw_pipeline, 
        schedule_load_data_to_dw_pipeline
    ]
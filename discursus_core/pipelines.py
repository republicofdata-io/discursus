from dagster import (
    pipeline, 
    ModeDefinition, 
    PresetDefinition, 
    file_relative_path
)
from dagster_snowflake import snowflake_resource
from dagster_shell import create_shell_command_solid

from solids import (
    enhance_gdelt_mention,
    launch_snowpipes, 
    run_dbt_transformation, 
    test_dbt_transformation
)


DBT_PROFILES_DIR = "."
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")


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


@pipeline(
    mode_defs = [prod_mode], 
    preset_defs = [prod_presets]
)
def mine_gdelt_data():
    gdelt_events_miner = create_shell_command_solid(
        "zsh < $DISCURSUS_MINER_GDELT_HOME/gdelt_events_miner.zsh", 
        name = "gdelt_events_miner_solid") 
    gdelt_events_miner()

    gdelt_mentions_miner = create_shell_command_solid(
        "zsh < $DISCURSUS_MINER_GDELT_HOME/gdelt_mentions_miner.zsh", 
        name = "gdelt_mentions_miner_solid") 
    gdelt_mentions_miner()

    enhance_gdelt_mention()


@pipeline(
    mode_defs = [prod_mode], 
    preset_defs = [prod_presets]
)
def build_data_warehouse():
    snowpipes_result = launch_snowpipes()
    dbt_run_result = run_dbt_transformation(start_after = snowpipes_result)
    dbt_test_result = test_dbt_transformation(start_after = dbt_run_result)
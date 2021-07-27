from dagster import (
    pipeline, 
    ModeDefinition, 
    PresetDefinition, 
    file_relative_path
)
from dagster_snowflake import snowflake_resource
from dagster_shell import create_shell_command_solid

from solids.dw_solids import (
    launch_snowpipes, 
    run_dbt_transformation, 
    test_dbt_transformation
)
from solids.enhance_mentions_solid import (
    enhance_mentions
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
    gdelt_mentions_miner_results = gdelt_mentions_miner()

    enhance_mentions(gdelt_mentions_miner_results)


@pipeline(
    mode_defs = [prod_mode], 
    preset_defs = [prod_presets]
)
def build_data_warehouse():
    snowpipes_result = launch_snowpipes()
    dbt_run_result = run_dbt_transformation(snowpipes_result)
    test_dbt_transformation(dbt_run_result)
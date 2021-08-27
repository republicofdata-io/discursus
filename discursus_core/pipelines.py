from dagster import (
    pipeline, 
    ModeDefinition, 
    PresetDefinition, 
    file_relative_path
)
from dagster_snowflake import snowflake_resource
from dagster_shell import create_shell_command_solid
from dagster_dbt import dbt_cli_resource

from solids.dw_solids import (
    launch_snowpipes,
    seed_dw_staging_layer,
    build_dw_staging_layer,
    test_dw_staging_layer,
    build_dw_integration_layer,
    test_dw_integration_layer,
    build_dw_warehouse_layer,
    test_dw_warehouse_layer,
    data_test_warehouse
)
from solids.enhance_mentions_solid import (
    enhance_mentions
)


DBT_PROFILES_DIR = "."
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")

my_dbt_resource = dbt_cli_resource.configured({
    "profiles_dir": DBT_PROFILES_DIR, 
    "project_dir": DBT_PROJECT_DIR})


prod_mode = ModeDefinition(
    name = 'prod',
    resource_defs = {
        'snowflake': snowflake_resource,
        'dbt': my_dbt_resource
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
    seed_dw_staging_layer_result = seed_dw_staging_layer(snowpipes_result)
    build_dw_staging_layer_result = build_dw_staging_layer(seed_dw_staging_layer_result)
    test_dw_staging_layer_result = test_dw_staging_layer(build_dw_staging_layer_result)
    build_dw_integration_layer_result = build_dw_integration_layer(test_dw_staging_layer_result)
    test_dw_integration_layer_result = test_dw_integration_layer(build_dw_integration_layer_result)
    build_dw_warehouse_layer_result = build_dw_warehouse_layer(test_dw_integration_layer_result)
    test_dw_warehouse_layer_result = test_dw_warehouse_layer(build_dw_warehouse_layer_result)
    test_dw_staging_layer_result = data_test_warehouse(test_dw_warehouse_layer_result)
from dagster import (
    job, 
    file_relative_path,
    config_from_files
)
from dagster_snowflake import snowflake_resource
from dagster_shell import create_shell_command_op
from dagster_dbt import dbt_cli_resource

from ops.dw_ops import (
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
from ops.enhance_mentions_op import (
    enhance_mentions
)


DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")

my_dbt_resource = dbt_cli_resource.configured({
    "profiles_dir": DBT_PROFILES_DIR, 
    "project_dir": DBT_PROJECT_DIR})


snowflake_env_variables = config_from_files(['environments/snowflake_env_variables.yaml'])


@job(
    resource_defs = {
        'snowflake': snowflake_resource
    },
    config = snowflake_env_variables
)
def mine_gdelt_data():
    gdelt_events_miner = create_shell_command_op(
        "zsh < $DISCURSUS_MINER_GDELT_HOME/gdelt_events_miner.zsh", 
        name = "gdelt_events_miner_op") 
    gdelt_events_miner_results = gdelt_events_miner()

    enhance_mentions_result = enhance_mentions(gdelt_events_miner_results)
    launch_snowpipes(enhance_mentions_result)


@job(
    resource_defs = {
        'snowflake': snowflake_resource,
        'dbt': my_dbt_resource
    },
    config = snowflake_env_variables
)
def build_data_warehouse():
    seed_dw_staging_layer_result = seed_dw_staging_layer()
    build_dw_staging_layer_result = build_dw_staging_layer(seed_dw_staging_layer_result)
    test_dw_staging_layer_result = test_dw_staging_layer(build_dw_staging_layer_result)
    build_dw_integration_layer_result = build_dw_integration_layer(test_dw_staging_layer_result)
    test_dw_integration_layer_result = test_dw_integration_layer(build_dw_integration_layer_result)
    build_dw_warehouse_layer_result = build_dw_warehouse_layer(test_dw_integration_layer_result)
    test_dw_warehouse_layer_result = test_dw_warehouse_layer(build_dw_warehouse_layer_result)
    test_dw_staging_layer_result = data_test_warehouse(test_dw_warehouse_layer_result)
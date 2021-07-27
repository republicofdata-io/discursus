from dagster import (
    solid, 
    file_relative_path
)
from dagster_dbt import dbt_cli_run, dbt_cli_test


DBT_PROFILES_DIR = "."
DBT_PROJECT_DIR = file_relative_path(__file__, "../dw")


@solid(required_resource_keys = {"snowflake"})
def launch_snowpipes(context):
    q_load_gdelt_events = "alter pipe gdelt_events_pipe refresh;"
    q_load_mentions_events = "alter pipe gdelt_mentions_pipe refresh;"
    q_load_enhanced_mentions_events = "alter pipe gdelt_enhanced_mentions_pipe refresh;"

    context.resources.snowflake.execute_query(q_load_gdelt_events)
    context.resources.snowflake.execute_query(q_load_mentions_events)
    context.resources.snowflake.execute_query(q_load_enhanced_mentions_events)


run_dbt_transformation = dbt_cli_run.configured(
    config_or_config_fn = {"project-dir": DBT_PROJECT_DIR, "profiles-dir": DBT_PROFILES_DIR},
    name = "run_dbt_transformation",
)


test_dbt_transformation = dbt_cli_test.configured(
    config_or_config_fn = {"project-dir": DBT_PROJECT_DIR, "profiles-dir": DBT_PROFILES_DIR},
    name = "test_dbt_transformation",
)
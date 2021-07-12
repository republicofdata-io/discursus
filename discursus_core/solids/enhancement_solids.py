from dagster import (
    solid, 
    file_relative_path
)
from dagster_dbt import dbt_cli_run, dbt_cli_test


DBT_PROFILES_DIR = "."
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")


@solid(required_resource_keys = {"snowflake"})
def enhance_gdelt_mention(context):
    print("Hello")
from dagster_dbt import load_assets_from_dbt_project

from dagster import file_relative_path

DBT_PROFILES_DIR = file_relative_path(__file__, "./../../dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "./../../dw")

protest_wh_assets = load_assets_from_dbt_project(
    project_dir = DBT_PROJECT_DIR, 
    profiles_dir = DBT_PROFILES_DIR, 
    key_prefix = ["data_warehouse"]
)
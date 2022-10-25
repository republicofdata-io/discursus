from dagster import (
    file_relative_path,
    config_from_files
)
from dagster_dbt import dbt_cli_resource
from dagster_snowflake import snowflake_resource

from saf_aws import aws_resource
from saf_gdelt import gdelt_resource
from saf_novacene import novacene_resource
from saf_web_scraper import web_scraper_resource
from resources.airtable_resource import airtable_api_client

DBT_PROFILES_DIR = file_relative_path(__file__, "./../dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "./../dw")

snowflake_configs = config_from_files(['configs/snowflake_configs.yaml'])
novacene_configs = config_from_files(['configs/novacene_configs.yaml'])
airtable_configs = config_from_files(['configs/airtable_configs.yaml'])

my_gdelt_resource = gdelt_resource.initiate_gdelt_resource.configured(None)
my_snowflake_resource = snowflake_resource.configured(snowflake_configs)
my_novacene_resource = novacene_resource.initiate_novacene_resource.configured(novacene_configs)
my_aws_resource = aws_resource.initiate_aws_resource.configured(None)
my_web_scraper_resource = web_scraper_resource.initiate_web_scraper_resource.configured(None)
my_airtable_client = airtable_api_client.configured(airtable_configs)

my_dbt_resource = dbt_cli_resource.configured({
    "profiles_dir": DBT_PROFILES_DIR, 
    "project_dir": DBT_PROJECT_DIR})
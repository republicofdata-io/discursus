from dagster import config_from_pkg_resources, file_relative_path, config_from_files
from dagster_snowflake import snowflake_resource
from dagster_dbt import dbt_cli_resource
from dagster_hex.resources import hex_resource 

from saf_aws import aws_resource
from saf_gdelt import gdelt_resource
from saf_novacene import novacene_resource
from saf_web_scraper import web_scraper_resource

from discursus_data_platform.utils.resources import airtable_resource, twitter_resource

DBT_PROFILES_DIR = file_relative_path(__file__, "./../../dp_data_warehouse/config/")
DBT_PROJECT_DIR = file_relative_path(__file__, "./../../dp_data_warehouse/")


snowflake_configs = config_from_pkg_resources(
    pkg_resource_defs=[
        ('discursus_data_platform.utils.configs', 'snowflake_configs.yaml')
    ],
)
novacene_configs = config_from_pkg_resources(
    pkg_resource_defs=[
        ('discursus_data_platform.utils.configs', 'novacene_configs.yaml')
    ],
)
airtable_configs = config_from_pkg_resources(
    pkg_resource_defs=[
        ('discursus_data_platform.utils.configs', 'airtable_configs.yaml')
    ],
)
twitter_configs = config_from_pkg_resources(
    pkg_resource_defs=[
        ('discursus_data_platform.utils.configs', 'twitter_configs.yaml')
    ],
)
hex_configs = config_from_pkg_resources(
    pkg_resource_defs=[
        ('discursus_data_platform.utils.configs', 'hex_configs.yaml')
    ],
)

my_gdelt_resource = gdelt_resource.initiate_gdelt_resource.configured(None)
my_snowflake_resource = snowflake_resource.configured(snowflake_configs)
my_dbt_resource = dbt_cli_resource.configured({
    "profiles_dir": DBT_PROFILES_DIR, 
    "project_dir": DBT_PROJECT_DIR})
my_novacene_resource = novacene_resource.initiate_novacene_resource.configured(novacene_configs)
my_aws_resource = aws_resource.initiate_aws_resource.configured(None)
my_web_scraper_resource = web_scraper_resource.initiate_web_scraper_resource.configured(None)
my_airtable_resource = airtable_resource.airtable_api_client.configured(airtable_configs)
my_twitter_resource = twitter_resource.twitter_api_client.configured(twitter_configs)
my_hex_resource = hex_resource.configured(hex_configs)
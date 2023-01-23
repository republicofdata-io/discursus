from dagster import config_from_pkg_resources
from dagster_snowflake import snowflake_resource
from dagster_hex.resources import hex_resource 

from saf_aws import aws_resource
from saf_gdelt import gdelt_resource
from saf_novacene import novacene_resource
from saf_web_scraper import web_scraper_resource

from discursus_data_platform.utils.resources import airtable_resource, twitter_resource


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
my_novacene_resource = novacene_resource.initiate_novacene_resource.configured(novacene_configs)
my_aws_resource = aws_resource.initiate_aws_resource.configured(None)
my_web_scraper_resource = web_scraper_resource.initiate_web_scraper_resource.configured(None)
my_airtable_resource = airtable_resource.airtable_api_client.configured(airtable_configs)
my_twitter_resource = twitter_resource.twitter_api_client.configured(twitter_configs)
my_hex_resource = hex_resource.configured(hex_configs)
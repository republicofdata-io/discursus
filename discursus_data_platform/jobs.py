from dagster import (
    job, 
    file_relative_path,
    config_from_files
)
from dagster_snowflake import snowflake_resource
from dagster_dbt import dbt_cli_resource

from saf_aws import aws_resource
from saf_gdelt import gdelt_resource
from saf_novacene import novacene_resource
from saf_web_scraper import web_scraper_resource

from ops.aws_ops import (
    s3_put,
    s3_get
)
from ops.gdelt_ops import (
    get_url_to_latest_events,
    get_url_to_latest_mentions,
    build_file_path, 
    mine_latest_asset,
    filter_latest_events,
    filter_latest_mentions
)
from ops.novacene_ops import (
    classify_mentions_relevancy, 
    get_relevancy_classifications, 
    store_relevancy_classifications
)
from ops.snowflake_ops import (
    launch_gdelt_events_snowpipe,
    launch_gdelt_mentions_snowpipe,
    launch_gdelt_enhanced_mentions_snowpipe,
    launch_ml_enriched_articles_snowpipe
)
from ops.dbt_ops import (
    seed_dw_staging_layer,
    build_dw_staging_layer,
    test_dw_staging_layer,
    build_dw_integration_layer,
    test_dw_integration_layer,
    build_dw_warehouse_layer,
    test_dw_warehouse_layer,
    data_test_warehouse,
    drop_old_relations
)
from ops.airtable_ops import (
    get_latest_ml_enrichments, 
    create_records
)
from ops.utils_ops import (
    get_enhanced_mentions_source_path,
    materialize_data_asset
)
from ops.web_scraper_ops import (
    scrape_urls
)

from resources.airtable_resource import airtable_api_client


# Resources
#################
DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")

snowflake_configs = config_from_files(['configs/snowflake_configs.yaml'])
novacene_configs = config_from_files(['configs/novacene_configs.yaml'])
airtable_configs = config_from_files(['configs/airtable_configs.yaml'])

my_gdelt_resource = gdelt_resource.initiate_gdelt_resource.configured(None)
my_novacene_resource = novacene_resource.initiate_novacene_resource.configured(novacene_configs)
my_aws_resource = aws_resource.initiate_aws_resource.configured(None)
my_web_scraper_resource = web_scraper_resource.initiate_web_scraper_resource.configured(None)
my_airtable_client = airtable_api_client.configured(airtable_configs)

my_dbt_client = dbt_cli_resource.configured({
    "profiles_dir": DBT_PROFILES_DIR, 
    "project_dir": DBT_PROJECT_DIR})


################
# Job to mine GDELT events
@job(
    resource_defs = {
        'aws_resource': my_aws_resource,
        'gdelt_resource': my_gdelt_resource
    },
    config = {
        "ops": {
            "materialize_data_asset": {
                "config": {
                    "asset_key_parent": "sources",
                    "asset_key_child": "gdelt_events",
                    "asset_description": "List of events mined on GDELT"
                }
            }
        }
    }
)
def mine_gdelt_events():
    latest_events_url = get_url_to_latest_events()
    latest_events_source_path = build_file_path(latest_events_url)
    df_latest_events = mine_latest_asset(latest_events_url)
    df_latest_events_filtered = filter_latest_events(df_latest_events)
    s3_put(df_latest_events_filtered, latest_events_source_path)
    materialize_data_asset(df_latest_events_filtered, latest_events_source_path)


################
# Job to mine GDELT mentions
@job(
    resource_defs = {
        'aws_resource': my_aws_resource,
        'gdelt_resource': my_gdelt_resource
    }
)
def mine_gdelt_mentions():
    df_latest_events_filtered = s3_get()
    latest_mentions_url = get_url_to_latest_mentions()
    latest_mentions_source_path = build_file_path(latest_mentions_url)
    df_latest_mentions = mine_latest_asset(latest_mentions_url)
    df_latest_mentions_filtered = filter_latest_mentions(df_latest_mentions, df_latest_events_filtered)
    s3_put(df_latest_mentions_filtered, latest_mentions_source_path)
    materialize_data_asset(df_latest_mentions_filtered, latest_mentions_source_path)


################
# Job to get meta data of GDELT mentions
@job(
    resource_defs = {
        'aws_resource': my_aws_resource,
        'web_scraper_resource': my_web_scraper_resource
    }
)
def enhance_gdelt_mentions():
    df_latest_mentions_filtered = s3_get()
    df_gdelt_enhanced_mentions = scrape_urls(df_latest_mentions_filtered)
    enhanced_mentions_source_path = get_enhanced_mentions_source_path(df_gdelt_enhanced_mentions)
    s3_put(df_gdelt_enhanced_mentions, enhanced_mentions_source_path)
    materialize_data_asset(df_gdelt_enhanced_mentions, enhanced_mentions_source_path)


################
# Job to load GDELT assets to Snowflake
@job(
    resource_defs = {
        'snowflake': snowflake_resource
    },
    config = snowflake_configs
)
def load_gdelt_assets_to_snowflake():
    launch_gdelt_events_snowpipe_result = launch_gdelt_events_snowpipe()
    launch_gdelt_mentions_snowpipe_result = launch_gdelt_mentions_snowpipe(launch_gdelt_events_snowpipe_result)
    launch_gdelt_enhanced_mentions_snowpipe(launch_gdelt_mentions_snowpipe_result)


################
# Job to classify relevancy of GDELT mentions
@job(
    resource_defs = {
        'novacene_resource': my_novacene_resource
    }
)
def classify_gdelt_mentions_relevancy():
    # Classify articles that are relevant protest events
    classify_mentions_relevancy()


################
# Job to get classification results of GDELT mentions
@job(
    resource_defs = {
        'novacene_resource': my_novacene_resource
    }
)
def get_relevancy_classification_of_gdelt_mentions():
    df_relevancy_classifications = get_relevancy_classifications()
    store_relevancy_classifications(df_relevancy_classifications)


################
# Job to load classified GDELT mentions to Snowflake
@job(
    resource_defs = {
        'snowflake': snowflake_resource
    },
    config = snowflake_configs
)
def load_classified_gdelt_mentions_to_snowflake():
    launch_ml_enriched_articles_snowpipe()


################
# Job to feed our ML training engine
@job(
    resource_defs = {
        'airtable_client': my_airtable_client
    }
)
def feed_ml_trainer_engine():
    df_latest_enriched_events_sample = get_latest_ml_enrichments()
    create_records_result = create_records(df_latest_enriched_events_sample)



################
# Job to build Snowflake data warehouse
@job(
    resource_defs = {
        'snowflake': snowflake_resource,
        'dbt': my_dbt_client
    },
    config = snowflake_configs
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
    drop_old_relations(test_dw_staging_layer_result)
from dagster import (
    job, 
    file_relative_path,
    config_from_files
)
from dagster_snowflake import snowflake_resource
from dagster_dbt import dbt_cli_resource

from discursus_gdelt import gdelt_mining_ops, gdelt_resources
from discursus_utils import persistance_ops

from ops.dw_ops import (
    launch_gdelt_events_snowpipe,
    launch_enhanced_articles_snowpipe,
    launch_ml_enriched_articles_snowpipe,
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
from ops.ml_enrichment_ops import classify_mentions_relevancy, get_ml_enrichment_files, store_ml_enrichment_files
from resources.novacene_ml_resource import novacene_ml_api_client
from resources.aws_resource import aws_client


# Resources
#################
DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")

snowflake_configs = config_from_files(['configs/snowflake_configs.yaml'])
novacene_configs = config_from_files(['configs/novacene_configs.yaml'])
aws_configs = config_from_files(['configs/aws_configs.yaml'])
gdelt_configs = config_from_files(['configs/gdelt_configs.yaml'])

my_dbt_resource = dbt_cli_resource.configured({
    "profiles_dir": DBT_PROFILES_DIR, 
    "project_dir": DBT_PROJECT_DIR})

my_novacene_client_client = novacene_ml_api_client.configured(novacene_configs)
my_aws_client = aws_client.configured(aws_configs)
my_gdelt_client = gdelt_resources.gdelt_client.configured(gdelt_configs)


################
# Job to mine GDELT events
@job(
    resource_defs = {
        'aws_client': my_aws_client,
        'gdelt_client': my_gdelt_client
    },
    config = {
        "ops": {
            "get_url_to_latest_asset": {
                "config": {
                    "gdelt_asset": "events"
                }
            },
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
    latest_events_url = gdelt_mining_ops.get_url_to_latest_asset()
    latest_events_source_path = gdelt_mining_ops.build_file_path(latest_events_url)
    df_latest_events = gdelt_mining_ops.mine_latest_asset(latest_events_url)
    df_latest_events_filtered = gdelt_mining_ops.filter_latest_events(df_latest_events)
    persistance_ops.save_data_asset(df_latest_events_filtered, latest_events_source_path)
    persistance_ops.materialize_data_asset(df_latest_events_filtered, latest_events_source_path)


################
# Job to mine GDELT mentions
@job(
    resource_defs = {
        'aws_client': my_aws_client,
        'gdelt_client': my_gdelt_client
    }
)
def mine_gdelt_mentions():
    df_latest_events_filtered = persistance_ops.get_saved_data_asset()
    latest_mentions_url = gdelt_mining_ops.get_url_to_latest_asset()
    latest_mentions_source_path = gdelt_mining_ops.build_file_path(latest_mentions_url)
    df_latest_mentions = gdelt_mining_ops.mine_latest_asset(latest_mentions_url)
    df_latest_mentions_filtered = gdelt_mining_ops.filter_latest_mentions(df_latest_mentions, df_latest_events_filtered)
    persistance_ops.save_data_asset(df_latest_mentions_filtered, latest_mentions_source_path)
    persistance_ops.materialize_data_asset(df_latest_mentions_filtered, latest_mentions_source_path)

    # Enhance, save and materialize article urls with their metadata
    # df_gdelt_enhanced_articles = gdelt_mining_ops.enhance_articles(latest_gdelt_events_s3_location)
    # materialize_enhanced_articles_asset_result = gdelt_mining_ops.materialize_enhanced_articles_asset(df_gdelt_enhanced_articles, latest_gdelt_events_s3_location)


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
    launch_enhanced_articles_snowpipe(launch_gdelt_events_snowpipe_result)


################
# Job to classify relevancy of GDELT mentions
@job(
    resource_defs = {
        'novacene_client': my_novacene_client_client
    }
)
def classify_gdelt_mentions_relevancy():
    # Classify articles that are relevant protest events
    classify_mentions_relevancy()


################
# Job to get classification results of GDELT mentions
@job(
    resource_defs = {
        'novacene_client': my_novacene_client_client
    }
)
def get_relevancy_classification_of_gdelt_mentions():
    df_ml_enrichment_files = get_ml_enrichment_files()
    store_ml_enrichment_files(df_ml_enrichment_files)


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
# Job to build Snowflake data warehouse
@job(
    resource_defs = {
        'snowflake': snowflake_resource,
        'dbt': my_dbt_resource
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
    drop_old_relations_result = drop_old_relations(test_dw_staging_layer_result)
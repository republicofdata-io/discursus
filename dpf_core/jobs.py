from dagster import (
    job, 
    file_relative_path,
    config_from_files
)
from dagster_snowflake import snowflake_resource
from dagster_dbt import dbt_cli_resource

from dpf_gdelt import gdelt_mining_ops
from dpf_utils import persistance_ops, scraping_ops

from ops.dw_ops import (
    launch_gdelt_events_snowpipe,
    launch_gdelt_mentions_snowpipe,
    launch_gdelt_enhanced_mentions_snowpipe,
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
from ops.my_ops import get_enhanced_mentions_source_path
from resources.novacene_ml_resource import novacene_ml_api_client
from resources.aws_resource import aws_client


# Resources
#################
DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")

snowflake_configs = config_from_files(['configs/snowflake_configs.yaml'])
novacene_configs = config_from_files(['configs/novacene_configs.yaml'])
aws_configs = config_from_files(['configs/aws_configs.yaml'])

my_dbt_resource = dbt_cli_resource.configured({
    "profiles_dir": DBT_PROFILES_DIR, 
    "project_dir": DBT_PROJECT_DIR})

my_novacene_client_client = novacene_ml_api_client.configured(novacene_configs)
my_aws_client = aws_client.configured(aws_configs)


################
# Job to mine GDELT events
@job(
    resource_defs = {
        'aws_client': my_aws_client
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
            },
            "filter_latest_events": {
                "config": {
                    "filter_event_code": 14,
                    "filter_countries": {
                        "US",
                        "CA"
                    }
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
        'aws_client': my_aws_client
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


################
# Job to get meta data of GDELT mentions
@job(
    resource_defs = {
        'aws_client': my_aws_client
    },
    config = {
        "ops": {
            "get_meta_data": {
                "config": {
                    "url_field_index": 5
                }
            }
        }
    }
)
def enhance_gdelt_mentions():
    df_latest_mentions_filtered = persistance_ops.get_saved_data_asset()
    df_gdelt_enhanced_mentions = scraping_ops.get_meta_data(df_latest_mentions_filtered)
    enhanced_mentions_source_path = get_enhanced_mentions_source_path(df_gdelt_enhanced_mentions)
    persistance_ops.save_data_asset(df_gdelt_enhanced_mentions, enhanced_mentions_source_path)
    persistance_ops.materialize_data_asset(df_gdelt_enhanced_mentions, enhanced_mentions_source_path)


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
    drop_old_relations(test_dw_staging_layer_result)
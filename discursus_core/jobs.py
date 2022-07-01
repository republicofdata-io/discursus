from dagster import (
    job, 
    file_relative_path,
    config_from_files
)
from dagster_snowflake import snowflake_resource
from dagster_dbt import dbt_cli_resource

from discursus_gdelt import gdelt_mining_ops, gdelt_resources

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
from ops.ml_enrichment_ops import classify_protest_relevancy, get_ml_enrichment_files, store_ml_enrichment_files
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


#Jobs
################
@job(
    resource_defs = {
        'snowflake': snowflake_resource,
        'aws_client': my_aws_client,
        'gdelt_client': my_gdelt_client
    },
    config = snowflake_configs
)
def mine_gdelt_data():
    # Mine data from GDELT
    latest_gdelt_events_s3_location = gdelt_mining_ops.mine_gdelt_events()

    # Materialize gdelt mining asset
    gdelt_mining_ops.materialize_gdelt_mining_asset(latest_gdelt_events_s3_location)

    # Enhance article urls with their metadata
    df_gdelt_enhanced_articles = gdelt_mining_ops.enhance_articles(latest_gdelt_events_s3_location)

    # Materialize enhanced articles asset
    materialize_enhanced_articles_asset_result = gdelt_mining_ops.materialize_enhanced_articles_asset(df_gdelt_enhanced_articles, latest_gdelt_events_s3_location)

    # Load to Snowflake
    launch_gdelt_events_snowpipe_result = launch_gdelt_events_snowpipe(materialize_enhanced_articles_asset_result)
    launch_enhanced_articles_snowpipe_result = launch_enhanced_articles_snowpipe(launch_gdelt_events_snowpipe_result)


@job(
    resource_defs = {
        'novacene_client': my_novacene_client_client
    }
)
def enrich_mined_data():
    # Classify articles that are relevant protest events
    classify_protest_relevancy_result = classify_protest_relevancy()


@job(
    resource_defs = {
        'snowflake': snowflake_resource,
        'novacene_client': my_novacene_client_client
    },
    config = snowflake_configs
)
def get_enriched_mined_data():
    df_ml_enrichment_files = get_ml_enrichment_files()
    store_ml_enrichment_files_result = store_ml_enrichment_files(df_ml_enrichment_files)
    
    launch_ml_enriched_articles_snowpipe_result = launch_ml_enriched_articles_snowpipe(store_ml_enrichment_files_result)


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
from dagster import (
    job, 
    file_relative_path,
    config_from_files
)
from dagster_snowflake import snowflake_resource
from dagster_shell import create_shell_command_op
from dagster_dbt import dbt_cli_resource

from ops.dw_ops import (
    launch_snowpipes,
    seed_dw_staging_layer,
    build_dw_staging_layer,
    test_dw_staging_layer,
    build_dw_integration_layer,
    test_dw_integration_layer,
    build_dw_warehouse_layer,
    test_dw_warehouse_layer,
    data_test_warehouse
)
from ops.gdelt_mining_ops import enhance_articles, materialize_gdelt_mining_asset, materialize_enhanced_articles_asset
from ops.ml_enrichment_ops import classify_protest_relevancy
from resources.novacene_ml_resource import novacene_ml_api_client


# Resources
#################
DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "./dw")

snowflake_env_variables = config_from_files(['environments/snowflake_env_variables.yaml'])
novacene_env_variables = config_from_files(['environments/novacene_env_variables.yaml'])

my_dbt_resource = dbt_cli_resource.configured({
    "profiles_dir": DBT_PROFILES_DIR, 
    "project_dir": DBT_PROJECT_DIR})

my_novacene_client_client = novacene_ml_api_client.configured(novacene_env_variables)


#Jobs
################
@job(
    resource_defs = {
        'snowflake': snowflake_resource
    },
    config = snowflake_env_variables
)
def mine_gdelt_data():
    # Mine data from GDELT
    gdelt_events_miner = create_shell_command_op(
        "zsh < $DISCURSUS_MINER_GDELT_HOME/gdelt_events_miner.zsh", 
        name = "gdelt_events_miner_op") 
    gdelt_mined_events_filename = gdelt_events_miner()

    # Materialize gdelt mining asset
    materialize_gdelt_mining_asset(gdelt_mined_events_filename)

    # Enhance article urls with their metadata
    df_gdelt_enhanced_articles = enhance_articles(gdelt_mined_events_filename)

    # Materialize enhanced articles asset
    materialize_enhanced_articles_asset_result = materialize_enhanced_articles_asset(df_gdelt_enhanced_articles, gdelt_mined_events_filename)

    # Load to Snowflake
    launch_snowpipes(materialize_enhanced_articles_asset_result)


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
        'dbt': my_dbt_resource
    }
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
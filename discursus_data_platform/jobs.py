from dagster import job, define_asset_job
from dagster_snowflake import snowflake_resource
import resources.my_resources
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


gdelt_events_job = define_asset_job(
    name = "gdelt_events_job", 
    selection = "gdelt_events"
)

gdelt_mentions_job = define_asset_job(
    name = "gdelt_mentions_job", 
    selection = "gdelt_mentions"
)

gdelt_enhanced_mentions_job = define_asset_job(
    name = "gdelt_enhanced_mentions_job", 
    selection = "gdelt_enhanced_mentions"
)


@job(
    description = "Load GDELT assets to Snowflake",
    resource_defs = {
        'snowflake': snowflake_resource
    },
    config = resources.my_resources.snowflake_configs
)
def load_gdelt_assets_to_snowflake():
    launch_gdelt_events_snowpipe_result = launch_gdelt_events_snowpipe()
    launch_gdelt_mentions_snowpipe_result = launch_gdelt_mentions_snowpipe(launch_gdelt_events_snowpipe_result)
    launch_gdelt_enhanced_mentions_snowpipe(launch_gdelt_mentions_snowpipe_result)


@job(
    description = "Classify relevancy of GDELT mentions",
    resource_defs = {
        'novacene_resource': resources.my_resources.my_novacene_resource
    }
)
def classify_gdelt_mentions_relevancy():
    # Classify articles that are relevant protest events
    classify_mentions_relevancy()


@job(
    description = "Get classification results of GDELT mentions",
    resource_defs = {
        'novacene_resource': resources.my_resources.my_novacene_resource
    }
)
def get_relevancy_classification_of_gdelt_mentions():
    df_relevancy_classifications = get_relevancy_classifications()
    store_relevancy_classifications(df_relevancy_classifications)


@job(
    description = "Load classified GDELT mentions to Snowflake",
    resource_defs = {
        'snowflake': snowflake_resource
    },
    config = resources.my_resources.snowflake_configs
)
def load_classified_gdelt_mentions_to_snowflake():
    launch_ml_enriched_articles_snowpipe()


@job(
    description = "Feed our ML training engine",
    resource_defs = {
        'airtable_client': resources.my_resources.my_airtable_client
    }
)
def feed_ml_trainer_engine():
    df_latest_enriched_events_sample = get_latest_ml_enrichments()
    create_records_result = create_records(df_latest_enriched_events_sample)



@job(
    description = "Build Snowflake data warehouse",
    resource_defs = {
        'snowflake': snowflake_resource,
        'dbt': resources.my_resources.my_dbt_client
    },
    config = resources.my_resources.snowflake_configs
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
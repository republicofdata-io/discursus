from dagster import (
    op, 
    file_relative_path,
    Output
)
from dagster_dbt import DbtCliOutput
from dagster_dbt.utils import generate_materializations

DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "../dw")


@op(required_resource_keys = {"snowflake"})
def launch_gdelt_events_snowpipe(context):
    q_load_gdelt_events = "alter pipe gdelt_events_pipe refresh;"
    context.resources.snowflake.execute_query(q_load_gdelt_events)

@op(required_resource_keys = {"snowflake"})
def launch_gdelt_mentions_snowpipe(context, launch_gdelt_events_snowpipe_result):
    q_load_gdelt_mentions_events = "alter pipe gdelt_mentions_pipe refresh;"
    context.resources.snowflake.execute_query(q_load_gdelt_mentions_events)

@op(required_resource_keys = {"snowflake"})
def launch_gdelt_enhanced_mentions_snowpipe(context, launch_gdelt_mentions_snowpipe_result):
    q_load_gdelt_enhanced_mentions_events = "alter pipe gdelt_enhanced_mentions_pipe refresh;"
    context.resources.snowflake.execute_query(q_load_gdelt_enhanced_mentions_events)

@op(required_resource_keys = {"snowflake"})
def launch_ml_enriched_articles_snowpipe(context):
    q_load_ml_enriched_mentions = "alter pipe gdelt_ml_enriched_mentions_pipe refresh;"
    context.resources.snowflake.execute_query(q_load_ml_enriched_mentions)

@op(required_resource_keys={"dbt"})
def seed_dw_staging_layer(context) -> DbtCliOutput:
    context.log.info(f"Seeding the data warehouse")
    dbt_result = context.resources.dbt.seed()
    return dbt_result

@op(required_resource_keys={"dbt"})
def build_dw_staging_layer(context, seed_dw_staging_layer_result: DbtCliOutput) -> DbtCliOutput:
    context.log.info(f"Building the staging layer")
    dbt_result = context.resources.dbt.run(select=["staging"], full_refresh=context.op_config["full_refresh_flag"])
    return dbt_result

@op(required_resource_keys={"dbt"})
def test_dw_staging_layer(context, build_dw_staging_layer_result: DbtCliOutput):
    context.log.info(f"Testing the staging layer")
    dbt_result = context.resources.dbt.test(select=["staging,test_type:generic"])
    return dbt_result

@op(required_resource_keys={"dbt"})
def build_dw_integration_layer(context, test_dw_staging_layer_result) -> DbtCliOutput:
    context.log.info(f"Building the integration layer")
    dbt_result = context.resources.dbt.run(select=["integration"], full_refresh=context.op_config["full_refresh_flag"])
    return dbt_result

@op(required_resource_keys={"dbt"})
def test_dw_integration_layer(context, build_dw_integration_layer_result: DbtCliOutput):
    context.log.info(f"Testing the integration layer")
    dbt_result = context.resources.dbt.test(select=["integration,test_type:generic"])
    return dbt_result

@op(required_resource_keys={"dbt"})
def build_dw_warehouse_layer(context, test_dw_integration_layer_result) -> DbtCliOutput:
    context.log.info(f"Building the warehouse layer")
    dbt_result = context.resources.dbt.run(select=["warehouse"], full_refresh=context.op_config["full_refresh_flag"])
    for materialization in generate_materializations(dbt_result, asset_key_prefix = ["protest_movement_core_entities"]):
        yield materialization
    yield Output(dbt_result)

@op(required_resource_keys={"dbt"})
def test_dw_warehouse_layer(context, build_dw_warehouse_layer_result: DbtCliOutput):
    context.log.info(f"Testing the warehouse layer")
    dbt_result = context.resources.dbt.test(select=["warehouse,test_type:generic"])
    return dbt_result

@op(required_resource_keys={"dbt"})
def data_test_warehouse(context, test_dw_warehouse_layer_result):
    context.log.info(f"Data tests")
    dbt_result = context.resources.dbt.test(models=["test_type:singular"])
    return dbt_result

@op(required_resource_keys={"dbt"})
def drop_old_relations(context, test_dw_warehouse_layer_result):
    context.log.info(f"Droping old relations")
    dbt_result = context.resources.dbt.run_operation(macro="drop_old_relations")
    return dbt_result
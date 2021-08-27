from dagster import (
    solid, 
    file_relative_path
)
from dagster_dbt import DbtCliOutput

DBT_PROFILES_DIR = "."
DBT_PROJECT_DIR = file_relative_path(__file__, "../dw")


@solid(required_resource_keys = {"snowflake"})
def launch_snowpipes(context):
    q_load_gdelt_events = "alter pipe gdelt_events_pipe refresh;"
    q_load_mentions_events = "alter pipe gdelt_mentions_pipe refresh;"
    q_load_enhanced_mentions_events = "alter pipe gdelt_enhanced_mentions_pipe refresh;"

    context.resources.snowflake.execute_query(q_load_gdelt_events)
    context.resources.snowflake.execute_query(q_load_mentions_events)
    context.resources.snowflake.execute_query(q_load_enhanced_mentions_events)

@solid(required_resource_keys={"dbt"})
def seed_dw_staging_layer(context, snowpipes_result) -> DbtCliOutput:
    context.log.info(f"Seeding the data warehouse")
    return context.resources.dbt.seed()

@solid(required_resource_keys={"dbt"})
def build_dw_staging_layer(context, seed_dw_staging_layer_result: DbtCliOutput) -> DbtCliOutput:
    context.log.info(f"Building the staging layer")
    return context.resources.dbt.run(models=["staging.*"])

@solid(required_resource_keys={"dbt"})
def test_dw_staging_layer(context, build_dw_staging_layer_result: DbtCliOutput):
    context.log.info(f"Testing the staging layer")
    context.resources.dbt.test(models=["staging.*"], schema=True, data=False)

@solid(required_resource_keys={"dbt"})
def build_dw_integration_layer(context, test_dw_staging_layer_result) -> DbtCliOutput:
    context.log.info(f"Building the integration layer")
    return context.resources.dbt.run(models=["integration.*"])

@solid(required_resource_keys={"dbt"})
def test_dw_integration_layer(context, build_dw_integration_layer_result: DbtCliOutput):
    context.log.info(f"Testing the integration layer")
    context.resources.dbt.test(models=["integration.*"], schema=True, data=False)

@solid(required_resource_keys={"dbt"})
def build_dw_warehouse_layer(context, test_dw_integration_layer_result) -> DbtCliOutput:
    context.log.info(f"Building the warehouse layer")
    return context.resources.dbt.run(models=["warehouse.*"])

@solid(required_resource_keys={"dbt"})
def test_dw_warehouse_layer(context, build_dw_warehouse_layer_result: DbtCliOutput):
    context.log.info(f"Testing the warehouse layer")
    context.resources.dbt.test(models=["warehouse.*"], schema=True, data=False)

@solid(required_resource_keys={"dbt"})
def data_test_warehouse(context, test_dw_warehouse_layer_result):
    context.log.info(f"Data tests")
    context.resources.dbt.test(schema=False, data=True)
from dagster import (
    op, 
    file_relative_path
)
from dagster_dbt import DbtCliOutput

DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "../dw")


@op(required_resource_keys = {"snowflake"})
def launch_snowpipes(context, enhance_mentions_result):
    q_load_gdelt_events = "alter pipe gdelt_events_pipe refresh;"
    q_load_enhanced_mentions_events = "alter pipe gdelt_enhanced_mentions_pipe refresh;"

    context.resources.snowflake.execute_query(q_load_gdelt_events)
    context.resources.snowflake.execute_query(q_load_enhanced_mentions_events)

@op(required_resource_keys={"dbt"})
def seed_dw_staging_layer(context) -> DbtCliOutput:
    context.log.info(f"Seeding the data warehouse")
    return context.resources.dbt.seed()

@op(required_resource_keys={"dbt"})
def build_dw_staging_layer(context, seed_dw_staging_layer_result: DbtCliOutput) -> DbtCliOutput:
    context.log.info(f"Building the staging layer")
    return context.resources.dbt.run(models=["staging.*"])

@op(required_resource_keys={"dbt"})
def test_dw_staging_layer(context, build_dw_staging_layer_result: DbtCliOutput):
    context.log.info(f"Testing the staging layer")
    context.resources.dbt.test(models=["staging.*"], schema=True, data=False)

@op(required_resource_keys={"dbt"})
def build_dw_integration_layer(context, test_dw_staging_layer_result) -> DbtCliOutput:
    context.log.info(f"Building the integration layer")
    return context.resources.dbt.run(models=["integration.*"])

@op(required_resource_keys={"dbt"})
def test_dw_integration_layer(context, build_dw_integration_layer_result: DbtCliOutput):
    context.log.info(f"Testing the integration layer")
    context.resources.dbt.test(models=["integration.*"], schema=True, data=False)

@op(required_resource_keys={"dbt"})
def build_dw_warehouse_layer(context, test_dw_integration_layer_result) -> DbtCliOutput:
    context.log.info(f"Building the warehouse layer")
    return context.resources.dbt.run(models=["warehouse.*"])

@op(required_resource_keys={"dbt"})
def test_dw_warehouse_layer(context, build_dw_warehouse_layer_result: DbtCliOutput):
    context.log.info(f"Testing the warehouse layer")
    context.resources.dbt.test(models=["warehouse.*"], schema=True, data=False)

@op(required_resource_keys={"dbt"})
def data_test_warehouse(context, test_dw_warehouse_layer_result):
    context.log.info(f"Data tests")
    context.resources.dbt.test(schema=False, data=True)
from dagster import (
    asset, 
    Output, 
    MetadataValue,
    Field,
    Noneable
)
import resources.my_resources
from dagster_hex.types import HexOutput
from dagster_hex.resources import DEFAULT_POLL_INTERVAL


@asset(
    description = "Data warehouse seeds",
    group_name = "data_warehouse",
    resource_defs = {
        'dbt_resource': resources.my_resources.my_dbt_resource
    }
)
def dw_seeds(context):
    context.resources.dbt_resource.seed()

    return Output(1)


@asset(
    non_argument_deps = {"dw_seeds"},
    description = "Data warehouse staging models",
    group_name = "data_warehouse",
    resource_defs = {
        'dbt_resource': resources.my_resources.my_dbt_resource
    },
    config_schema={"full_refresh_flag": bool}
)
def dw_staging_layer(context):
    full_refresh_flag = context.op_config["full_refresh_flag"] if context.op_config["full_refresh_flag"] else False
    context.resources.dbt_resource.run(select=["staging"], full_refresh=full_refresh_flag)
    context.resources.dbt_resource.test(select=["staging,test_type:generic"])

    return Output(1)


@asset(
    non_argument_deps = {"dw_staging_layer"},
    description = "Data warehouse integration models",
    group_name = "data_warehouse",
    resource_defs = {
        'dbt_resource': resources.my_resources.my_dbt_resource
    },
    config_schema={"full_refresh_flag": bool}
)
def dw_integration_layer(context):
    full_refresh_flag = context.op_config["full_refresh_flag"] if context.op_config["full_refresh_flag"] else False
    context.resources.dbt_resource.run(select=["integration"], full_refresh=full_refresh_flag)
    context.resources.dbt_resource.test(select=["integration,test_type:generic"])

    return Output(1)


@asset(
    non_argument_deps = {"dw_integration_layer"},
    description = "Data warehouse entity layer",
    group_name = "data_warehouse",
    resource_defs = {
        'dbt_resource': resources.my_resources.my_dbt_resource
    },
    config_schema={"full_refresh_flag": bool}
)
def dw_entity_layer(context):
    full_refresh_flag = context.op_config["full_refresh_flag"] if context.op_config["full_refresh_flag"] else False
    context.resources.dbt_resource.run(select=["warehouse"], full_refresh=full_refresh_flag)
    context.resources.dbt_resource.test(select=["warehouse,test_type:generic"])

    return Output(1)


@asset(
    non_argument_deps = {"dw_entity_layer"},
    description = "Data warehouse data tests",
    group_name = "data_warehouse",
    resource_defs = {
        'dbt_resource': resources.my_resources.my_dbt_resource
    }
)
def dw_data_tests(context):
    context.resources.dbt_resource.test(models=["test_type:singular"])
    
    return Output(1)


@asset(
    non_argument_deps = {"dw_data_tests"},
    description = "Data warehouse clean up",
    group_name = "data_warehouse",
    resource_defs = {
        'dbt_resource': resources.my_resources.my_dbt_resource
    }
)
def dw_clean_up(context):
    context.resources.dbt_resource.run_operation(macro="drop_old_relations")
    
    return Output(1)


@asset(
    non_argument_deps = {"dw_data_tests"},
    description = "Hex project refresh",
    group_name = "data_apps",
    resource_defs = {
        'hex_resource': resources.my_resources.my_hex_resource
    },
)
def hex_project_refresh(context):
    hex_output: HexOutput = context.resources.hex_resource.run_and_poll(
        project_id = "d6824152-38b4-4f39-8f5e-c3a963cc48c8",
        inputs = {},
        update_cache = False,
        kill_on_timeout = True,
        poll_interval = DEFAULT_POLL_INTERVAL,
        poll_timeout = None,
    )
    asset_name = ["hex", hex_output.run_response["projectId"]]

    return Output(
        value = asset_name, 
        metadata = {
            "run_url": MetadataValue.url(hex_output.run_response["runUrl"]),
            "run_status_url": MetadataValue.url(
                hex_output.run_response["runStatusUrl"]
            ),
            "trace_id": MetadataValue.text(hex_output.run_response["traceId"]),
            "run_id": MetadataValue.text(hex_output.run_response["runId"]),
            "elapsed_time": MetadataValue.int(
                hex_output.status_response["elapsedTime"]
            ),
        },
    )
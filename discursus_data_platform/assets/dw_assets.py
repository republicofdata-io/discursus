from dagster import asset, file_relative_path, Output
import resources.my_resources


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
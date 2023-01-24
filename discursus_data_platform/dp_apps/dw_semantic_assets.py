from dagster import asset, AssetKey, FreshnessPolicy, Output
from discursus_data_platform.utils.resources import my_resources


@asset(
    non_argument_deps = {
        AssetKey(["data_warehouse", "events_fct"]), 
        AssetKey(["data_warehouse", "observations_fct"]),
        AssetKey(["data_warehouse", "movements_dim"])
    },
    description = "Data warehouse clean up",
    group_name = "data_apps",
    resource_defs = {
        'dbt_resource': my_resources.my_dbt_resource
    },
    freshness_policy = FreshnessPolicy(
        maximum_lag_minutes = 60 * 4, 
        cron_schedule = "15 3,15 * * *"
    )
)
def semantic_definitions(context):
    # clean up the data warehouse
    context.resources.dbt_resource.run_operation(macro="drop_old_relations")
    
    return Output(1)
from dagster import asset, AssetKey, Output, MetadataValue, FreshnessPolicy, AutoMaterializePolicy
from dagster_hex.types import HexOutput
from dagster_hex.resources import DEFAULT_POLL_INTERVAL

from discursus_data_platform.utils.resources import my_resources


@asset(
    non_argument_deps = {
        AssetKey(["data_warehouse", "events_fct"]), 
        AssetKey(["data_warehouse", "observations_fct"]),
        AssetKey(["data_warehouse", "movements_dim"])
    },
    description = "Hex main dashboard refresh",
    group_name = "data_apps",
    key_prefix = ["hex"],
    resource_defs = {
        'hex_resource': my_resources.my_hex_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.lazy(),
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=60*4),
)
def hex_main_dashboard_refresh(context):
    hex_output: HexOutput = context.resources.hex_resource.run_and_poll(
        project_id = "d6824152-38b4-4f39-8f5e-c3a963cc48c8",
        inputs = None,
        update_cache = True,
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
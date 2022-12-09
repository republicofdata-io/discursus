from dagster import (
    asset, 
    Output, 
    MetadataValue
)
import resources.my_resources

from dagster_hex.types import HexOutput
from dagster_hex.resources import DEFAULT_POLL_INTERVAL


@asset(
    non_argument_deps = {"dw_data_tests"},
    description = "Hex main dashboard refresh",
    group_name = "data_apps",
    resource_defs = {
        'hex_resource': resources.my_resources.my_hex_resource
    },
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
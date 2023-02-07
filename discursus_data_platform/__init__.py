from dagster import (
    AssetSelection, 
    build_asset_reconciliation_sensor,
    file_relative_path,
    load_assets_from_package_module,
    repository, 
    with_resources
)
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

from discursus_data_platform import (
    dp_gdelt,
    dp_movement_groupings,
    dp_data_warehouse,
    dp_apps
)

DBT_PROJECT_DIR = dp_data_warehouse.__path__[0]

my_assets = with_resources(
    load_assets_from_dbt_project(
        project_dir = DBT_PROJECT_DIR, 
        profiles_dir = DBT_PROJECT_DIR, 
        key_prefix = ["data_warehouse"],
        use_build_command = True
    ) + 
    load_assets_from_package_module(dp_gdelt) +
    load_assets_from_package_module(dp_movement_groupings) +
    load_assets_from_package_module(dp_apps),
    resource_defs = {
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_DIR,
                "profiles_dir": DBT_PROJECT_DIR,
            },
        ),
        "io_manager": s3_pickle_io_manager.configured(
            {"s3_bucket": "discursus-io", "s3_prefix": "platform"}
        ),
        "s3": s3_resource,
    },
)

asset_sensor = [
    build_asset_reconciliation_sensor(
        asset_selection=AssetSelection.all(),
        name="asset_reconciliation_sensor",
        minimum_interval_seconds = 60 * 5
    )
]


@repository
def discursus_repo():
    return my_assets + asset_sensor

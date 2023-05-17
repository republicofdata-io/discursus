from dagster import (
    AssetSelection, 
    file_relative_path,
    load_assets_from_package_module,
    repository, 
    with_resources
)
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

from discursus_data_platform import (
    dp_gdelt,
    dp_apps
)

dbt_project_dir = file_relative_path(__file__, "./dp_data_warehouse/")
dbt_profile_dir = file_relative_path(__file__, "./dp_data_warehouse/config/")

my_assets = with_resources(
    load_assets_from_dbt_project(
        project_dir = dbt_project_dir, 
        profiles_dir = dbt_profile_dir, 
        key_prefix = ["data_warehouse"],
        use_build_command = False
    ) + 
    load_assets_from_package_module(dp_gdelt) + # type: ignore
    load_assets_from_package_module(dp_apps),
    resource_defs = {
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": dbt_project_dir,
                "profiles_dir": dbt_profile_dir,
            },
        ),
        "io_manager": s3_pickle_io_manager.configured(
            {"s3_bucket": "discursus-io", "s3_prefix": "platform"}
        ),
        "s3": s3_resource,
    },
)

@repository
def discursus_repo():
    return my_assets

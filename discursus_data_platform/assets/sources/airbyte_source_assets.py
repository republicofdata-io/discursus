from dagster import asset, Output


@asset(
    description = "Protest movement groupings",
    key_prefix = ["airbyte"],
    group_name = "sources"
)
def protest_groupings(context):
    pass
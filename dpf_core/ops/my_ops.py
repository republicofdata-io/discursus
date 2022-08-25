from dagster import (
    op
)


@op(
    required_resource_keys = {
        "aws_client"
    }
)
def get_enhanced_mentions_source_path(context, df_gdelt_enhanced_mentions):
    enhanced_mentions_source_path = context.op_config["asset_materialization_path"].split("s3://discursus-io/")[1].split(".CSV")[0] + ".enhanced.csv"
    context.log.info("Saving enhanced mentions to: " + str(enhanced_mentions_source_path))

    return enhanced_mentions_source_path
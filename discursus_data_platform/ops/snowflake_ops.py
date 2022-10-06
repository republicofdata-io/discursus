from dagster import (
    op, 
    file_relative_path,
    Output
)

DBT_PROFILES_DIR = file_relative_path(__file__, "./dw")
DBT_PROJECT_DIR = file_relative_path(__file__, "../dw")


@op(required_resource_keys = {"snowflake"})
def launch_gdelt_events_snowpipe(context):
    q_load_gdelt_events = "alter pipe gdelt_events_pipe refresh;"
    context.resources.snowflake.execute_query(q_load_gdelt_events)

@op(required_resource_keys = {"snowflake"})
def launch_gdelt_mentions_snowpipe(context, launch_gdelt_events_snowpipe_result):
    q_load_gdelt_mentions_events = "alter pipe gdelt_mentions_pipe refresh;"
    context.resources.snowflake.execute_query(q_load_gdelt_mentions_events)

@op(required_resource_keys = {"snowflake"})
def launch_gdelt_enhanced_mentions_snowpipe(context, launch_gdelt_mentions_snowpipe_result):
    q_load_gdelt_enhanced_mentions_events = "alter pipe gdelt_enhanced_mentions_pipe refresh;"
    context.resources.snowflake.execute_query(q_load_gdelt_enhanced_mentions_events)

@op(required_resource_keys = {"snowflake"})
def launch_ml_enriched_articles_snowpipe(context):
    q_load_ml_enriched_mentions = "alter pipe gdelt_ml_enriched_mentions_pipe refresh;"
    context.resources.snowflake.execute_query(q_load_ml_enriched_mentions)
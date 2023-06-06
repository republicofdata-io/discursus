from dagster import (
    DynamicPartitionsDefinition,
    job,
    op,
    ScheduleDefinition,
)
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

from discursus_data_platform.utils.resources import my_resources


#####
# Op, job and schedule to fetch new GDELT partitions
#####
gdelt_partitions_def = DynamicPartitionsDefinition(name="dagster_partition_id")


@op(
    description = "Populates list of GDELT partitions",
    required_resource_keys = {
        'bigquery_resource',
    },
)
def gdelt_partitions(context):
    # Get latest record id
    try:
        gdelt_dagster_partitions = context.instance.get_dynamic_partitions("dagster_partition_id")
        latest_gdelt_quarter_hour_partition = gdelt_dagster_partitions[-1]
        latest_gdelt_daily_partition = latest_gdelt_quarter_hour_partition[:8] + '000000'
    except:
        latest_gdelt_quarter_hour_partition = '20230602000000'
        latest_gdelt_daily_partition = latest_gdelt_quarter_hour_partition[:8] + '000000'

    context.log.info("Latest gdelt 15 min partition: " + latest_gdelt_quarter_hour_partition)
    context.log.info("Latest gdelt hourly partition: " + latest_gdelt_daily_partition)

    # Get list of partitions
    query = f"""
        with new_articles as (

            select distinct `DATE` as dagster_partition_id
            from `gdelt-bq.gdeltv2.gkg_partitioned`
            where _PARTITIONTIME >= parse_timestamp('%Y%m%d%H%M%S', '{latest_gdelt_daily_partition}')
            and `DATE` > {latest_gdelt_quarter_hour_partition}
            order by 1

        ),

        latest_partition as (

            select max(dagster_partition_id) as latest_dagster_partition_id
            from new_articles

        )

        select cast(dagster_partition_id as string) as dagster_partition_id
        from new_articles
        where dagster_partition_id != (select latest_dagster_partition_id from latest_partition)
        order by dagster_partition_id
    """

    gdelt_partitions_df = context.resources.bigquery_resource.query(query)

    # Convert gdelt_partitions to list
    gdelt_partitions_ls = gdelt_partitions_df["dagster_partition_id"].tolist()
    context.instance.add_dynamic_partitions("dagster_partition_id", gdelt_partitions_ls)

    # Return number of new partitions
    context.log.info("Number of new gdelt partitions: " + str(gdelt_partitions_df.index.size))
    return gdelt_partitions_df.index.size


@job(
    description="Populates list of GDELT partitions",
    resource_defs={
        'bigquery_resource': my_resources.my_bigquery_resource,
    },
)
def gdelt_partitions_job():
   gdelt_partitions()


gdelt_partitions_schedule = ScheduleDefinition(job=gdelt_partitions_job, cron_schedule="00,15,30,45 * * * *")



#####
# Op, job and schedule to refresh Snowflake pipes
#####
@op(
    description = "Refresh Snowflake pipes",
    required_resource_keys = {
        'snowflake_resource',
    },
)
def snowflake_pipes(context):
    # Transfer latest GDELT articles
    q_load_gdelt_articles = "alter pipe gdelt_articles_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_articles)

    # Transfer latest enhanced GDELT articles
    q_load_gdelt_mentions_enhanced_events = "alter pipe gdelt_enhanced_articles_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_mentions_enhanced_events)

    # Transfer latest GDELT article summaries
    q_load_gdelt_article_summaries_events = "alter pipe gdelt_article_summaries_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_article_summaries_events)

    return None


@job(
    description="Refresh Snowflake pipes",
    resource_defs={
        'snowflake_resource': my_resources.my_snowflake_resource,
    },
)
def snowflake_pipes_job():
   snowflake_pipes()


snowflake_pipes_schedule = ScheduleDefinition(job=snowflake_pipes_job, cron_schedule="15 3,9,15,21 * * *")
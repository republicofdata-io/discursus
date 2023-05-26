from dagster import (
    asset,
    AssetKey,
    AutoMaterializePolicy,
    DynamicPartitionsDefinition,
    FreshnessPolicy,
    Output
)
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

import pandas as pd

from discursus_data_platform.utils.resources import my_resources


# Dynamic partition for GDELT articles
gdelt_partitions_def = DynamicPartitionsDefinition(name="dagster_partition_id")


@asset(
    description = "List of new GDELT partitions to fill",
    key_prefix = ["gdelt"],
    group_name = "sources",
    resource_defs = {
        'bigquery_resource': my_resources.my_bigquery_resource,
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=60),
)
def gdelt_partitions(context):
    # Get list of partitions
    query = """
        with new_articles as (

            select distinct `DATE` as dagster_partition_id
            from `gdelt-bq.gdeltv2.gkg_partitioned`
            where _PARTITIONTIME >= parse_timestamp('%Y%m%d%H%M%S', '20230523100000')
            and GKGRECORDID > '20230523100000'
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

    context.log.info(gdelt_partitions)
    
    # Return asset
    return Output(
        value = gdelt_partitions_df, 
        metadata = {
            "rows": gdelt_partitions_df.index.size
        },
    )


@asset(
    non_argument_deps = {AssetKey(["gdelt", "gdelt_partitions"]),},
    description = "List of gkg articles mined on GDELT",
    key_prefix = ["gdelt"],
    group_name = "sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'bigquery_resource': my_resources.my_bigquery_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=None),
    partitions_def=gdelt_partitions_def,
)
def gdelt_gkg_articles(context):
    # Get partition
    dagster_partition_id = context.partition_key

    # Define S3 path
    dagster_partition_date = dagster_partition_id[:8]
    gdelt_asset_source_path = 'sources/gdelt/' + dagster_partition_date + '/' + dagster_partition_id + '.articles.csv'

    # Fetch articles for partition
    query = f"""
        with s_articles as (

        select
            GKGRECORDID as gdelt_gkg_article_id,
            Documentidentifier as article_url,
            SourceCollectionIdentifier as source_collection_id,
            lower(Themes) as themes,
            lower(Locations) as locations,
            lower(Persons) as persons,
            lower(Organizations) as organizations,
            SocialImageEmbeds as social_image_url,
            SocialVideoEmbeds as social_video_url,
            parse_timestamp('%Y%m%d%H%M%S', cast(`DATE` as string)) as creation_ts,
            DATE as dagster_partition_id,
            _PARTITIONTIME as bq_partition_id
        
        from `gdelt-bq.gdeltv2.gkg_partitioned`

        where DATE = {dagster_partition_id}

        ),

        filter_source_collections as (

            select * from s_articles
            where source_collection_id = 1

        ),

        primary_locations as (

            select
                gdelt_gkg_article_id,
                article_url,
                source_collection_id,
                themes,
                locations,
                (
                    select split_location 
                    from unnest(split(locations, ';')) as split_location
                    order by 
                        case substr(split_location, 1, 1)
                        when '3' then 1
                        when '4' then 2
                        when '2' then 3
                        when '5' then 4
                        when '1' then 5
                        else 6
                        end 
                    limit 1
                ) as primary_location,
                persons,
                organizations,
                social_image_url,
                social_video_url,
                creation_ts,
                dagster_partition_id,
                bq_partition_id

            from filter_source_collections

        ),

        filter_locations as (

            select * from primary_locations
            where primary_location like '%canada%'
            or primary_location like '%united states%'

        ),

        filter_themes as (

            select * from filter_locations
            where (
                select true 
                from unnest(split(themes, ';')) theme with offset
                where offset < 10
                and theme = 'protest'
                limit 1
            ) is not null

        )

        select *
        from filter_themes
        order by gdelt_gkg_article_id
    """
    gdelt_gkg_articles_df = context.resources.bigquery_resource.query(query)

    # Save data to S3
    context.resources.aws_resource.s3_put(gdelt_gkg_articles_df, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    # q_load_gdelt_articles = "alter pipe gdelt_articles_pipe refresh;"
    # context.resources.snowflake_resource.execute_query(q_load_gdelt_articles)
    
    # Return asset
    return Output(
        value = gdelt_gkg_articles_df, 
        metadata = {
            "s3_path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": gdelt_gkg_articles_df.index.size,
            "min_gdelt_gkg_article_id": gdelt_gkg_articles_df["gdelt_gkg_article_id"].min(),
            "max_gdelt_gkg_article_id": gdelt_gkg_articles_df["gdelt_gkg_article_id"].max(),
        },
    )

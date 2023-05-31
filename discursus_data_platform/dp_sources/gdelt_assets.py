from dagster import (
    asset,
    AssetIn,
    AssetKey,
    AutoMaterializePolicy,
    DynamicPartitionsDefinition,
    FreshnessPolicy,
    Output
)
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

import openai.error
import pandas as pd
import time

from discursus_data_platform.utils.resources import my_resources


# Dynamic partition for GDELT articles
gdelt_partitions_def = DynamicPartitionsDefinition(name="dagster_partition_id")


@asset(
    description = "List of new GDELT partitions to fill",
    key_prefix = ["gdelt"],
    group_name = "sources",
    resource_defs = {
        'bigquery_resource': my_resources.my_bigquery_resource,
        'snowflake_resource': my_resources.my_snowflake_resource,
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=15),
)
def gdelt_partitions(context):
    # Get latest record id
    try:
        gdelt_dagster_partitions = context.instance.get_dynamic_partitions("dagster_partition_id")
        latest_gdelt_quarter_hour_partition = gdelt_dagster_partitions[-1]
        latest_gdelt_daily_partition = latest_gdelt_quarter_hour_partition[:8] + '000000'
    except:
        latest_gdelt_quarter_hour_partition = '20230530120000'
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

    # Return asset
    return Output(
        value = gdelt_partitions_df, 
        metadata = {
            "rows": gdelt_partitions_df.index.size
        },
    )


@asset(
    description = "List of gkg articles mined on GDELT",
    key_prefix = ["gdelt"],
    group_name = "sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'bigquery_resource': my_resources.my_bigquery_resource,
        'snowflake_resource': my_resources.my_snowflake_resource,
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
    q_load_gdelt_articles = "alter pipe gdelt_articles_pipe refresh;"
    context.resources.snowflake_resource.execute_query(q_load_gdelt_articles)
    
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


@asset(
    ins = {"gdelt_gkg_articles": AssetIn(key_prefix = "gdelt")},
    description = "List of enhanced articles mined from GDELT",
    key_prefix = ["gdelt"],
    group_name = "prepared_sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'web_scraper_resource': my_resources.my_web_scraper_resource,
        'snowflake_resource': my_resources.my_snowflake_resource,
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=None),
    partitions_def=gdelt_partitions_def,
)
def gdelt_articles_enhanced(context, gdelt_gkg_articles):
    # Get partition
    dagster_partition_id = context.partition_key

    # Define S3 path
    dagster_partition_date = dagster_partition_id[:8]
    gdelt_asset_source_path = 'sources/gdelt/' + dagster_partition_date + '/' + dagster_partition_id + '.articles.enhanced.csv'

    # Dedup articles based on article_url field
    df_articles = gdelt_gkg_articles.drop_duplicates(subset=["article_url"], keep='first')

    # Create dataframe
    column_names = ['article_url', 'file_name', 'title', 'description', 'keywords', 'content']
    gdelt_articles_enhanced_df = pd.DataFrame(columns = column_names)

    for _, row in df_articles.iterrows():
        try:
            scraped_article = context.resources.web_scraper_resource.scrape_article(row["article_url"])
        except IndexError:
            continue

        if scraped_article is None:
            continue
    
        # Use get method with default value (empty string) for each element in scraped_row
        scraped_row = [
            scraped_article.get('url', ''),
            scraped_article.get('filename', ''),
            scraped_article.get('title', ''),
            scraped_article.get('description', ''),
            scraped_article.get('keywords', ''),
            scraped_article.get('content', '')
        ]
        
        df_length = len(gdelt_articles_enhanced_df)
        gdelt_articles_enhanced_df.loc[df_length] = scraped_row # type: ignore
    
    # Save data to S3
    context.resources.aws_resource.s3_put(gdelt_articles_enhanced_df, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_mentions_enhanced_events = "alter pipe gdelt_enhanced_articles_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_mentions_enhanced_events)

    # Return asset
    return Output(
        value = gdelt_articles_enhanced_df, 
        metadata = {
            "s3_path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": gdelt_articles_enhanced_df.index.size
        }
    )


@asset(
    ins = {"gdelt_articles_enhanced": AssetIn(key_prefix = "gdelt")},
    description = "LLM-generated summary of GDELT articles",
    key_prefix = ["gdelt"],
    group_name = "prepared_sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'openai_resource': my_resources.my_openai_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=None),
    partitions_def=gdelt_partitions_def,
)
def gdelt_article_summaries(context, gdelt_articles_enhanced):
    # Get partition
    dagster_partition_id = context.partition_key

    # Define S3 path
    dagster_partition_date = dagster_partition_id[:8]
    gdelt_asset_source_path = 'sources/gdelt/' + dagster_partition_date + '/' + dagster_partition_id + '.articles.summary.csv'

    # Cycle through each article in gdelt_mentions_enhanced and generate a summary
    gdelt_article_summaries_df = pd.DataFrame(columns = ['article_url', 'summary'])
    for _, row in gdelt_articles_enhanced.iterrows():
        prompt = f"""Write a concise summary for the following article:
        
        Title: {row['title']}
        Description: {row['description']}
        Content: {row['content'][:2350]}

        CONCISE SUMMARY:"""
    
        # Keep retrying the request until it succeeds
        while True:
            try:
                completion_str = context.resources.openai_resource.chat_completion(model='gpt-3.5-turbo', prompt=prompt, max_tokens=1500)
                break
            except openai.error.RateLimitError as e:
                # Wait for 5 seconds before retrying
                time.sleep(5)
                continue

        df_length = len(gdelt_article_summaries_df)
        gdelt_article_summaries_df.loc[df_length] = [row['article_url'], completion_str] # type: ignore

     # Save data to S3
    context.resources.aws_resource.s3_put(gdelt_article_summaries_df, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_article_summaries_events = "alter pipe gdelt_article_summaries_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_article_summaries_events)

    # Return asset
    return Output(
        value = gdelt_article_summaries_df, 
        metadata = {
            "s3_path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": gdelt_article_summaries_df.index.size
        }
    )
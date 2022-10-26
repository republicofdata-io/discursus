from dagster import asset, AssetObservation, Output
import pandas as pd
import boto3
from io import StringIO
import resources.my_resources
from resources.ml_enrichment_tracker import MLEnrichmentJobTracker


@asset(
    description = "List of events mined on GDELT",
    group_name = "sources",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource
    }
)
def gdelt_events(context):
    # Build source path
    latest_events_url = context.resources.gdelt_resource.get_url_to_latest_asset("events")
    gdelt_asset_filename_zip = str(latest_events_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_events = context.resources.gdelt_resource.mine_latest_asset(latest_events_url)
    df_latest_events_filtered = context.resources.gdelt_resource.filter_latest_events(df_latest_events, 14, ['US', 'CA'])
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_events_filtered, 'discursus-io', gdelt_asset_source_path)

    # Return asset
    return Output(
        value = df_latest_events_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_latest_events_filtered.index.size
        }
    )


@asset(
    description = "List of mentions mined from GDELT",
    group_name = "sources",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource
    }
)
def gdelt_mentions(context, gdelt_events):
    # Build source path
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_mentions = context.resources.gdelt_resource.mine_latest_asset(latest_mentions_url)
    df_latest_mentions_filtered = context.resources.gdelt_resource.filter_latest_mentions(df_latest_mentions, gdelt_events)
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_mentions_filtered, 'discursus-io', gdelt_asset_source_path)

    # Return asset
    return Output(
        value = df_latest_mentions_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_latest_mentions_filtered.index.size
        }
    )


@asset(
    description = "List of enhanced mentions mined from GDELT",
    group_name = "prepared_sources",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource,
        'web_scraper_resource': resources.my_resources.my_web_scraper_resource
    }
)
def gdelt_mentions_enhanced(context, gdelt_mentions):
    # Build source path
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Dedup articles
    context.log.info(gdelt_mentions[5])
    df_articles = gdelt_mentions.drop_duplicates(subset=[5], keep='first')

    # Create dataframe
    column_names = ['mention_identifier', 'file_name', 'title', 'description', 'keywords']
    df_gdelt_mentions_enhanced = pd.DataFrame(columns = column_names)

    # Scrape urls and populate dataframe
    for index, row in df_articles.iterrows():
        scraped_article = context.resources.web_scraper_resource.scrape_url(row[5])
        scraped_row = [
            scraped_article['mention_identifier'][0], 
            scraped_article['file_name'][0],
            scraped_article['title'][0],
            scraped_article['description'][0],
            scraped_article['keywords'][0]
        ]
        df_length = len(df_gdelt_mentions_enhanced)
        df_gdelt_mentions_enhanced.loc[df_length] = scraped_row
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_gdelt_mentions_enhanced, 'discursus-io', gdelt_asset_source_path)

    # Return asset
    return Output(
        value = df_gdelt_mentions_enhanced, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_gdelt_mentions_enhanced.index.size
        }
    )


@asset(
    description = "Jobs to classify relevancy of GDELT mentions",
    group_name = "prepared_sources",
    resource_defs = {
        'gdelt_resource': resources.my_resources.my_gdelt_resource,
        'novacene_resource': resources.my_resources.my_novacene_resource
    }
)
def gdelt_mentions_relevancy_ml_jobs(context, gdelt_mentions_enhanced): 
    # Build source path
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()
    
    # Sending latest batch of articles to Novacene for relevancy classification
    if gdelt_mentions_enhanced.index.size > 0:
        context.log.info("Sending " + str(gdelt_mentions_enhanced.index.size) + " articles for relevancy classification")

        protest_classification_dataset_id = context.resources.novacene_resource.create_dataset("protest_events_" + gdelt_asset_source_path.split("/")[3], gdelt_mentions_enhanced)
        protest_classification_job = context.resources.novacene_resource.enrich_dataset(protest_classification_dataset_id['id'], 17, 4)

        # Update log of enrichment jobs
        my_ml_enrichment_jobs_tracker.add_new_job(protest_classification_job['id'], 'processing')
        my_ml_enrichment_jobs_tracker.upload_job_log()

        # Return asset
        return Output(
            value = protest_classification_job, 
            metadata = {
                "job id": protest_classification_job['id'],
                "dataset enriched": gdelt_asset_source_path,
                "dataset enrtries": gdelt_mentions_enhanced.index.size
            }
        )
    else:
        return Output(1)


@asset(
    non_argument_deps = {"gdelt_mentions_enhanced"},
    description = "Snowpipe transfers of GDELT assets to Snowflake",
    group_name = "prepared_sources",
    resource_defs = {
        'snowflake_resource': resources.my_resources.my_snowflake_resource
    }
)
def snowpipe_transfers_gdelt_assets(context):
    # Events
    q_load_gdelt_events = "alter pipe gdelt_events_pipe refresh;"
    context.resources.snowflake_resource.execute_query(q_load_gdelt_events)

    # Mentions
    q_load_gdelt_mentions_events = "alter pipe gdelt_mentions_pipe refresh;"
    context.resources.snowflake_resource.execute_query(q_load_gdelt_mentions_events)

    # Enhanced Mentions
    q_load_gdelt_mentions_enhanced_events = "alter pipe gdelt_enhanced_mentions_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_mentions_enhanced_events)

    return Output(
        value = snowpipe_result
    )
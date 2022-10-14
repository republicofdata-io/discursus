from dagster import asset, multi_asset, Output
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
    latest_events_url = context.resources.gdelt_resource.get_url_to_latest_asset("events")
    
    # Build source path
    gdelt_asset_filename_zip = str(latest_events_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    latest_events_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_events = context.resources.gdelt_resource.mine_latest_asset(latest_events_url)
    df_latest_events_filtered = context.resources.gdelt_resource.filter_latest_events(df_latest_events, 14, ['US', 'CA'])
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_events_filtered, 'discursus-io', latest_events_source_path)

    # Return asset
    return Output(
        df_latest_events_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + latest_events_source_path,
            "rows": df_latest_events_filtered.index.size
        }
    )


@asset(
    non_argument_deps = {"gdelt_events"},
    description = "List of mentions mined from GDELT",
    group_name = "sources",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource
    }
)
def gdelt_mentions(context):
    file_path = context.op_config["file_path"].split("s3://discursus-io/")[1]

    df_latest_events_filtered = context.resources.aws_resource.s3_get('discursus-io', file_path)
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")

    # Build source path
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    latest_mentions_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_mentions = context.resources.gdelt_resource.mine_latest_asset(latest_mentions_url)
    df_latest_mentions_filtered = context.resources.gdelt_resource.filter_latest_mentions(df_latest_mentions, df_latest_events_filtered)
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_mentions_filtered, 'discursus-io', latest_mentions_source_path)

    # Return asset
    return Output(
        df_latest_mentions_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + latest_mentions_source_path,
            "rows": df_latest_mentions_filtered.index.size
        }
    )


@asset(
    non_argument_deps = {"gdelt_mentions"},
    description = "List of enhanced mentions mined from GDELT",
    group_name = "preparation",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource,
        'web_scraper_resource': resources.my_resources.my_web_scraper_resource
    }
)
def gdelt_mentions_enhanced(context):
    file_path = context.op_config["file_path"].split("s3://discursus-io/")[1]
    enhanced_mentions_source_path = context.op_config["asset_materialization_path"].split("s3://discursus-io/")[1].split(".CSV")[0] + ".enhanced.csv"
    df_latest_mentions_filtered = context.resources.aws_resource.s3_get('discursus-io', file_path)

    # Dedup articles
    df_articles = df_latest_mentions_filtered.drop_duplicates(subset=["5"], keep='first')

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
    context.resources.aws_resource.s3_put(df_gdelt_mentions_enhanced, 'discursus-io', enhanced_mentions_source_path)

    # Return asset
    return Output(
        df_gdelt_mentions_enhanced, 
        metadata = {
            "path": "s3://discursus-io/" + enhanced_mentions_source_path,
            "rows": df_gdelt_mentions_enhanced.index.size
        }
    )


@asset(
    description = "Relevancy classification of GDELT mentions",
    group_name = "preparation",
    resource_defs = {
        'novacene_resource': resources.my_resources.my_novacene_resource
    }
)
def gdelt_mentions_relevant(context):
    # Empty dataframe of files to fetch
    df_relevancy_classifications = pd.DataFrame(None, columns = ['job_id', 'name', 'file_path'])

    # Lit of jobs to remove
    l_completed_job_indexes = []

    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()

    for index, row in my_ml_enrichment_jobs_tracker.df_ml_enrichment_jobs.iterrows():
        job_info = context.resources.novacene_resource.job_info(row['job_id'])

        if job_info['status'] == 'Completed':
            # Append new job to existing list
            data_ml_enrichment_file = [[row['job_id'], job_info['source']['name'], job_info['result']['path']]]
            df_ml_enrichment_file = pd.DataFrame(data_ml_enrichment_file, columns = ['job_id', 'name', 'file_path'])
            df_relevancy_classifications = df_relevancy_classifications.append(df_ml_enrichment_file)

            # Keep track of jobs to remove from tracking log
            l_completed_job_indexes.append(index)

    # Updating job from log of enrichment jobs
    my_ml_enrichment_jobs_tracker.remove_completed_job(l_completed_job_indexes)
    my_ml_enrichment_jobs_tracker.upload_job_log()

    s3 = boto3.resource('s3')

    for index, row in df_relevancy_classifications.iterrows():
        # Read csv as pandas
        df_ml_enrichment_file = context.resources.novacene_resource.get_file(row['file_path'])

        # Extract date from file name
        file_date = row['name'].split("_")[2].split(".")[0][0 : 8]

        # Save df as csv in S3
        csv_buffer = StringIO()
        df_ml_enrichment_file.to_csv(csv_buffer, index = False)
        s3.Object('discursus-io', 'sources/ml/' + file_date + '/ml_enriched_' + row['name']).put(Body=csv_buffer.getvalue())

        # Return asset
        yield Output(
            df_ml_enrichment_file, 
            metadata = {
                "path": "s3://discursus-io/" + 'sources/ml/' + file_date + '/ml_enriched_' + row['name'],
                "rows": df_ml_enrichment_file.index.size
            }
        )
    
    return Output(1)
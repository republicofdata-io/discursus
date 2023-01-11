from dagster import asset, AssetIn, AssetObservation, Output, FreshnessPolicy
import pandas as pd
import boto3
from io import StringIO

from discursus_data_platform.utils.resources import my_resources
from discursus_data_platform.utils.resources.ml_enrichment_tracker import MLEnrichmentJobTracker



@asset(
    ins = {"gdelt_mentions": AssetIn(key_prefix = "gdelt")},
    description = "List of enhanced mentions mined from GDELT",
    group_name = "prepared_sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'web_scraper_resource': my_resources.my_web_scraper_resource,
        'snowflake_resource': my_resources.my_snowflake_resource,
        'novacene_resource': my_resources.my_novacene_resource
    },
)
def gdelt_mentions_enhanced(context, gdelt_mentions):
    # Build source path
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv[0:14] + '.mentions.enhanced.csv'

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

    # Transfer to Snowflake
    q_load_gdelt_mentions_enhanced_events = "alter pipe gdelt_enhanced_mentions_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_mentions_enhanced_events)

    # Transfer to Novacene for relevancy classification
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()
    
    # Sending latest batch of articles to Novacene for relevancy classification
    if df_gdelt_mentions_enhanced.index.size > 0:
        context.log.info("Sending " + str(df_gdelt_mentions_enhanced.index.size) + " articles for relevancy classification")

        protest_classification_dataset_id = context.resources.novacene_resource.create_dataset("protest_events_" + gdelt_asset_source_path.split("/")[3], df_gdelt_mentions_enhanced)
        protest_classification_job = context.resources.novacene_resource.enrich_dataset(protest_classification_dataset_id['id'], 17, 4)
        context.log.info("Protest classification job id: " + str(protest_classification_job['id']))

        # Update log of enrichment jobs
        my_ml_enrichment_jobs_tracker.add_new_job(str(protest_classification_job['id']), 'relevancy', 'processing')
        my_ml_enrichment_jobs_tracker.upload_job_log()

    # Return asset
    return Output(
        value = df_gdelt_mentions_enhanced, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_gdelt_mentions_enhanced.index.size
        }
    )


@asset(
    non_argument_deps = {"gdelt_mentions_enhanced"},
    description = "ML enriched GDELT mentions",
    key_prefix = ["gdelt"],
    group_name = "prepared_sources",
    resource_defs = {
        'novacene_resource': my_resources.my_novacene_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    freshness_policy = FreshnessPolicy(
        maximum_lag_minutes = 60
    )
)
def gdelt_mentions_relevancy(context):
    # Empty dataframe of files to fetch
    df_relevancy_classifications = pd.DataFrame(None, columns = ['job_id', 'name', 'file_path'])

    # Lit of jobs to remove
    l_completed_job_indexes = []

    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()

    for index, row in my_ml_enrichment_jobs_tracker.df_ml_enrichment_jobs.iterrows():
        if row['type'] == 'relevancy':
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
        file_date_time = row['name'].split("_")[2].split(".")[0]

        # Save df as csv in S3
        csv_buffer = StringIO()
        df_ml_enrichment_file.to_csv(csv_buffer, index = False)
        s3.Object('discursus-io', 'sources/ml/' + file_date + '/' + file_date_time + '_relevancy_classification.csv').put(Body=csv_buffer.getvalue())

        # Only keep relevant articles
        df_relevant_articles = df_ml_enrichment_file[df_ml_enrichment_file['predict_relevantTECLM3_v2.sav'] == 1]
        
        # Sending latest batch of articles to Novacene for entity extraction
        if df_relevant_articles.index.size > 0:
            context.log.info("Sending " + str(df_relevant_articles.index.size) + " articles for entity extraction")

            entity_extraction_dataset_id = context.resources.novacene_resource.create_dataset("entity_extraction_" + file_date, df_relevant_articles)
            entity_extraction_job = context.resources.novacene_resource.named_entity_recognition(entity_extraction_dataset_id['id'], 5)
            context.log.info("Entity extraction job id: " + str(entity_extraction_job['id']))

            # Update log of enrichment jobs
            my_ml_enrichment_jobs_tracker.add_new_job(str(entity_extraction_job['id']), 'entity_extraction', 'processing')
            my_ml_enrichment_jobs_tracker.upload_job_log()

        # Get trace of asset metadata
        context.log_event(
            AssetObservation(
                asset_key = "gdelt_mentions_relevancy",
                metadata = {
                    "path": "s3://discursus-io/" + 'sources/ml/' + file_date + '/' + file_date_time + '_relevancy_classification.csv',
                    "rows": df_ml_enrichment_file.index.size
                }
            )
        )
    
    # Transfer to Snowflake
    q_load_ml_enriched_mentions = "alter pipe gdelt_mentions_relevancy_pipe refresh;"
    context.resources.snowflake_resource.execute_query(q_load_ml_enriched_mentions)


@asset(
    non_argument_deps = {"gdelt_mentions_relevancy"},
    description = "Entity extraction of GDELT mentions",
    key_prefix = ["gdelt"],
    group_name = "prepared_sources",
    resource_defs = {
        'novacene_resource': my_resources.my_novacene_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    freshness_policy = FreshnessPolicy(
        maximum_lag_minutes = 60
    )
)
def gdelt_mentions_entity_extraction(context):
    # Empty dataframe of files to fetch
    df_entity_extractions = pd.DataFrame(None, columns = ['job_id', 'name', 'file_path'])

    # Lit of jobs to remove
    l_completed_job_indexes = []

    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()

    for index, row in my_ml_enrichment_jobs_tracker.df_ml_enrichment_jobs.iterrows():
        if row['type'] == 'entity_extraction':
            job_info = context.resources.novacene_resource.job_info(row['job_id'])

            if job_info['status'] == 'Completed':
                # Append new job to existing list
                data_ml_enrichment_file = [[row['job_id'], job_info['source']['name'], job_info['result']['path']]]
                df_ml_enrichment_file = pd.DataFrame(data_ml_enrichment_file, columns = ['job_id', 'name', 'file_path'])
                df_entity_extractions = df_entity_extractions.append(df_ml_enrichment_file)

                # Keep track of jobs to remove from tracking log
                l_completed_job_indexes.append(index)

    # Updating job from log of enrichment jobs
    my_ml_enrichment_jobs_tracker.remove_completed_job(l_completed_job_indexes)
    my_ml_enrichment_jobs_tracker.upload_job_log()

    s3 = boto3.resource('s3')

    for index, row in df_entity_extractions.iterrows():
        # Read csv as pandas
        df_ml_enrichment_file = context.resources.novacene_resource.get_file(row['file_path'])

        # Extract date from file name
        file_date = row['name'].split("_")[2]
        file_date_time = row['file_path'].split("_")[4]

        # Save df as csv in S3
        csv_buffer = StringIO()
        df_ml_enrichment_file.to_csv(csv_buffer, index = False)
        s3.Object('discursus-io', 'sources/ml/' + file_date + '/' + file_date_time + '_entity_extraction.csv').put(Body=csv_buffer.getvalue())

        # Get trace of asset metadata
        context.log_event(
            AssetObservation(
                asset_key = "gdelt_mentions_entity_extraction",
                metadata = {
                    "path": "s3://discursus-io/" + 'sources/ml/' + file_date + '/' + file_date_time + '_entity_extraction.csv',
                    "rows": df_ml_enrichment_file.index.size
                }
            )
        )
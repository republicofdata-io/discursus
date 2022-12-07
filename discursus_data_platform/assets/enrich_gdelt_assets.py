from dagster import asset, AssetObservation, Output
import pandas as pd
import boto3
from io import StringIO
import resources.my_resources
from resources.ml_enrichment_tracker import MLEnrichmentJobTracker


@asset(
    non_argument_deps = {"gdelt_mentions_enhanced"},
    description = "Relevancy classification of GDELT mentions",
    group_name = "prepared_sources",
    resource_defs = {
        'novacene_resource': resources.my_resources.my_novacene_resource
    }
)
def gdelt_mentions_relevancy(context):
    # Empty dataframe of files to fetch
    df_relevancy_classifications = pd.DataFrame(None, columns = ['job_id', 'name', 'file_path'])

    # Lit of jobs to remove
    l_completed_job_indexes = []

    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()

    for index, row in my_ml_enrichment_jobs_tracker.df_ml_enrichment_jobs.iterrows():
        job_info = context.resources.novacene_resource.job_info(row['job_id'])

        if job_info['type'] == 'relevancy' and job_info['status'] == 'Completed':
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

        # Get trace of asset metadata
        context.log_event(
            AssetObservation(
                asset_key = "gdelt_mentions_relevancy",
                metadata = {
                    "path": "s3://discursus-io/" + 'sources/ml/' + file_date + '/ml_enriched_' + row['name'],
                    "rows": df_ml_enrichment_file.index.size
                }
            )
        )


@asset(
    non_argument_deps = {"gdelt_mentions_relevancy"},
    description = "Snowpipe transfers of classified GDELT mentions to Snowflake",
    group_name = "prepared_sources",
    resource_defs = {
        'snowflake_resource': resources.my_resources.my_snowflake_resource
    }
)
def snowpipe_transfers_classified_gdelt_mentions(context):
    q_load_ml_enriched_mentions = "alter pipe gdelt_ml_enriched_mentions_pipe refresh;"
    snowpipe_result =  context.resources.snowflake_resource.execute_query(q_load_ml_enriched_mentions)

    return Output(
        value = snowpipe_result
    )


@asset(
    description = "Entity extraction of relevant articles",
    group_name = "prepared_sources",
    resource_defs = {
        'novacene_resource': resources.my_resources.my_novacene_resource
    }
)
def article_entity_extraction_ml_jobs(context, gdelt_mentions_relevancy):
    context.log.info(gdelt_mentions_relevancy) 

    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()
    
    # Sending latest batch of articles to Novacene for entity extraction
    if gdelt_mentions_relevancy.index.size > 0:
        context.log.info("Sending " + str(gdelt_mentions_relevancy.index.size) + " articles for entity extraction")
        entity_extraction_job = context.resources.novacene_resource.named_entity_recognition(gdelt_mentions_relevancy.metadata['dataset_id'], 4)

        # Update log of enrichment jobs
        my_ml_enrichment_jobs_tracker.add_new_job(entity_extraction_job['id'], 'entity_extraction', 'processing')
        my_ml_enrichment_jobs_tracker.upload_job_log()

        # Return asset
        return Output(
            value = entity_extraction_job, 
            metadata = {
                "dataset_id": gdelt_mentions_relevancy.metadata['dataset_id'],
                "dataset_source_path": gdelt_mentions_relevancy.metadata['dataset_source_path'],
                "dataset_enrtries": gdelt_mentions_relevancy.index.size,
                "job_id": entity_extraction_job['id']
            }
        )
    else:
        return Output(1)

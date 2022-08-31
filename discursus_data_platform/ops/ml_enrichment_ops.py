from dagster import op, AssetMaterialization, Output

import boto3
from io import StringIO
import pandas as pd

class MLEnrichmentJobTracker:

    def __init__(self):
        self.s3 = boto3.resource('s3')

        try:
            obj = self.s3.Object('discursus-io', 'ops/ml_enrichment_jobs.csv')
            df_ml_enrichment_jobs = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))
        except:
            df_ml_enrichment_jobs = pd.DataFrame(None, columns = ['job_id', 'status'])
        
        self.df_ml_enrichment_jobs = df_ml_enrichment_jobs
    
    def add_new_job(self, job_id, job_status):
        # Append new job to existing list
        data_ml_enrichment_jobs = [[job_id, job_status]]
        df_new_ml_enrichment_job = pd.DataFrame(data_ml_enrichment_jobs, columns = ['job_id', 'status'])
        self.df_ml_enrichment_jobs = self.df_ml_enrichment_jobs.append(df_new_ml_enrichment_job)
    
    def remove_completed_job(self, l_job_indexes):
        # Append new job to existing list
        self.df_ml_enrichment_jobs = self.df_ml_enrichment_jobs.drop(l_job_indexes)
    
    def upload_job_log(self):
        csv_buffer = StringIO()
        self.df_ml_enrichment_jobs.to_csv(csv_buffer, index = False)
        self.s3.Object('discursus-io', 'ops/ml_enrichment_jobs.csv').put(Body=csv_buffer.getvalue())



@op(
    required_resource_keys = {"novacene_client"},
    config_schema = {
        "asset_key": list,
        "asset_materialization_path": str
    }
)
def classify_mentions_relevancy(context): 
    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()
    
    # Get latest asset of gdelt articles
    filename = context.op_config["asset_materialization_path"].split("s3://discursus-io/")[1]
    obj = my_ml_enrichment_jobs_tracker.s3.Object('discursus-io', filename)
    df_gdelt_articles = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))

    # Sending latest batch of articles to Novacene for relevancy classification
    if df_gdelt_articles.index.size > 0:
        context.log.info("Sending " + str(df_gdelt_articles.index.size) + " articles for relevancy classification")

        protest_classification_dataset_id = context.resources.novacene_client.create_dataset("protest_events_" + filename.split("/")[3], df_gdelt_articles)
        protest_classification_job = context.resources.novacene_client.enrich_dataset(protest_classification_dataset_id['id'])

        # Update log of enrichment jobs
        my_ml_enrichment_jobs_tracker.add_new_job(protest_classification_job['id'], 'processing')
        my_ml_enrichment_jobs_tracker.upload_job_log()

        # Materialize asset
        yield AssetMaterialization(
            asset_key=["sources", "ml_enrichment_jobs"],
            description="List of ml enrichment jobs",
            metadata={
                "job id": protest_classification_job['id'],
                "dataset enriched": filename,
                "dataset enrtries": df_gdelt_articles.index.size
            }
        )
        yield Output(protest_classification_job)


@op(
    required_resource_keys = {"novacene_client"}
)
def get_ml_enrichment_files(context):
    # Empty dataframe of files to fetch
    df_ml_enrichment_files = pd.DataFrame(None, columns = ['job_id', 'name', 'file_path'])

    # Lit of jobs to remove
    l_completed_job_indexes = []

    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()

    for index, row in my_ml_enrichment_jobs_tracker.df_ml_enrichment_jobs.iterrows():
        job_info = context.resources.novacene_client.job_info(row['job_id'])

        if job_info['status'] == 'Completed':
            # Append new job to existing list
            data_ml_enrichment_file = [[row['job_id'], job_info['source']['name'], job_info['result']['path']]]
            df_ml_enrichment_file = pd.DataFrame(data_ml_enrichment_file, columns = ['job_id', 'name', 'file_path'])
            df_ml_enrichment_files = df_ml_enrichment_files.append(df_ml_enrichment_file)

            # Keep track of jobs to remove from tracking log
            l_completed_job_indexes.append(index)

    # Updating job from log of enrichment jobs
    my_ml_enrichment_jobs_tracker.remove_completed_job(l_completed_job_indexes)
    my_ml_enrichment_jobs_tracker.upload_job_log()

    return df_ml_enrichment_files


@op(
    required_resource_keys = {"novacene_client"}
)
def store_ml_enrichment_files(context, df_ml_enrichment_files):
    s3 = boto3.resource('s3')

    for index, row in df_ml_enrichment_files.iterrows():
        # Read csv as pandas
        df_ml_enrichment_file = context.resources.novacene_client.get_file(row['file_path'])

        # Extract date from file name
        file_date = row['name'].split("_")[2].split(".")[0][0 : 8]

        # Save df as csv in S3
        csv_buffer = StringIO()
        df_ml_enrichment_file.to_csv(csv_buffer, index = False)
        s3.Object('discursus-io', 'sources/ml/' + file_date + '/ml_enriched_' + row['name']).put(Body=csv_buffer.getvalue())

        # Materialize and yield asset
        yield AssetMaterialization(
        asset_key=["sources", "ml_enrichment_files"],
        description="List of ml enrichment files",
        metadata={
            "path": "s3://discursus-io/" + 'sources/ml/' + file_date + '/ml_enriched_' + row['name'],
            "rows": df_ml_enrichment_file.index.size
        }
    )
    
    yield Output(1)
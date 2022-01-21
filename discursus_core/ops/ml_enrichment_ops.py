from dagster import op, AssetMaterialization, Output

import boto3
from io import StringIO
import pandas as pd

class MLEnrichmentJobTracker:

    def __init__(self):
        s3 = boto3.resource('s3')

        try:
            obj = s3.Object('discursus-io', 'ops/ml_enrichment_jobs.csv').get()
            df_ml_enrichment_jobs = pd.read_csv(obj)
        except:
            df_ml_enrichment_jobs = pd.DataFrame(None, columns = ['job_id', 'status'])
            csv_buffer = StringIO()
            df_ml_enrichment_jobs.to_csv(csv_buffer)
            s3.Object('discursus-io', 'ops/ml_enrichment_jobs.csv').put(Body=csv_buffer.getvalue())
        
        self.df_ml_enrichment_jobs = df_ml_enrichment_jobs


@op(
    required_resource_keys = {"novacene_client"},
    config_schema = {
        "asset_key": list,
        "asset_materialization_path": str
    }
)
def classify_protest_relevancy(context): 
    # Get latest asset of gdelt articles
    filename = context.op_config["asset_materialization_path"].split("s3://discursus-io/")[1]
    s3 = boto3.resource('s3')
    obj = s3.Object('discursus-io', filename)
    df_gdelt_articles = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))

    # Sending latest batch of articles to Novacene for relevancy classification
    context.log.info("Sending " + str(df_gdelt_articles.index.size) + " articles for relevancy classification")

    protest_classification_dataset_id = context.resources.novacene_client.create_dataset("protest_events_" + filename.split("/")[3], df_gdelt_articles)
    protest_classification_job_id = context.resources.novacene_client.enrich_dataset(protest_classification_dataset_id['id'])
    context.log.info(protest_classification_job_id)

    return protest_classification_job_id['id']


@op
def get_ml_enrichment_jobs(context):
     # Get a unique list of urls to enhance
    my_ml_enrichment_jobs = MLEnrichmentJobTracker()
    context.log.info(my_ml_enrichment_jobs.df_ml_enrichment_jobs)
    
    # Create Enrichments jobs dataframe
    data_ml_enrichment_jobs = [[1, 'complete'], [2, 'processing']]
    df_ml_enrichment_jobs = pd.DataFrame(data_ml_enrichment_jobs, columns = ['job_id', 'status'])

     # Materialize asset
    yield AssetMaterialization(
        asset_key="ml_enrichment_jobs",
        description="List of ml enrichment jobs and their statuses",
        metadata={
            "path": "s3://discursus-io/ops/ml_enrichment_jobs.csv",
            "jobs": df_ml_enrichment_jobs['job_id'].size
        }
    )
    yield Output(df_ml_enrichment_jobs)
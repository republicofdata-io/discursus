from dagster import op, AssetMaterialization, Output

import boto3
from io import StringIO
import pandas as pd
import time

@op(
    required_resource_keys = {"airtable_client"}
)
def get_latest_ml_enrichments(context): 
    # S3 instances
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
   
    # Get latest file of ML enrichments
    response = s3_client.list_objects_v2(Bucket='discursus-io', Prefix='sources/ml')
    all = response['Contents']        
    latest = max(all, key=lambda x: x['LastModified'])

    # Download and convert latest file to dataframe
    obj = s3_resource.Object('discursus-io', latest['Key'])
    df_latest_enriched_events = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))

    # Sample 20 rows from dataframe, equally distributed between relevant and irrelevant classifications
    df_latest_enriched_events_sample = df_latest_enriched_events.groupby("predict_relevantTECLM3.sav").sample(n=5, random_state=1)

    return df_latest_enriched_events_sample


@op(
    required_resource_keys = {"airtable_client"}
)
def create_records(context, df_latest_enriched_events_sample):
    for index, row in df_latest_enriched_events_sample.iterrows():
        response_json = context.resources.airtable_client.create_record(
            article_url= row['mention_identifier'], 
            title = row['page_title'], 
            description = row['page_description'], 
            relevancy = bool(row['predict_relevantTECLM3.sav'])
        )
        time.sleep(0.25)

    return None
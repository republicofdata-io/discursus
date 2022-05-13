from dagster import op, AssetMaterialization, Output

import boto3
from io import StringIO
import pandas as pd
import time
from datetime import datetime

@op(
    required_resource_keys = {"airtable_client"}
)
def get_latest_ml_enrichments(context): 
    # S3 instances
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
   
    # Get latest file of ML enrichments
    todays_date = datetime.today().strftime('%Y%m%d')
    
    try:
        response = s3_client.list_objects_v2(Bucket='discursus-io', Prefix='sources/ml/' + todays_date)
        all = response['Contents']        
        latest = max(all, key=lambda x: x['LastModified'])

        context.log.info("Pulling latest ML enriched file from S3 to feed ML training engine: " + str(latest))

        # Download and convert latest file to dataframe
        obj = s3_resource.Object('discursus-io', latest['Key'])
        df_latest_enriched_events = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))

        # Sample rows from dataframe, equally distributed between relevant and irrelevant classifications
        df_latest_enriched_events_sample = df_latest_enriched_events[df_latest_enriched_events["predict_relevantTECLM3_v2.sav"] == 1].sample(frac = 0.5, random_state = 1)
        
        return df_latest_enriched_events_sample
    except:
        return None


@op(
    required_resource_keys = {"airtable_client"}
)
def create_records(context, df_latest_enriched_events_sample):
    for index, row in df_latest_enriched_events_sample.iterrows():
        response_json = context.resources.airtable_client.create_record(
            article_url= row['mention_identifier'], 
            title = row['page_title'], 
            description = row['page_description'], 
            relevancy = bool(row['predict_relevantTECLM3_v2.sav'])
        )
        time.sleep(0.25)

    return None
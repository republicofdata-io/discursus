from dagster import resource
import os
import json
import base64
from google.oauth2.service_account import Credentials
from google.cloud import bigquery


class BigQueryClient:
    def __init__(self):
        # Initiative BigQuery resource
        credentials_base64 = os.environ['GOOGLE_APPLICATION_CREDENTIALS_BASE64']
        credentials_json = base64.b64decode(credentials_base64).decode('utf-8')
        credentials_info = json.loads(credentials_json)
        credentials = Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=credentials_info['project_id'])

    
    # Methods
    ######################
    def query(self, query):
        query_job = self.client.query(query)
        response_df = query_job.to_dataframe()
        
        return(response_df)



@resource(
    description="BigQuery client.",
)
def bigquery_client(context):
    return BigQueryClient()
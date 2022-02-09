import requests
import json
from dagster import resource, StringSource, IntSource
from dagster.builtins import String
import pandas as pd
from io import StringIO

class AirtableAPIClient:
    def __init__(self, host, api_key):
        self._conn_host = host
        self._conn_api_key = api_key

    
    # API methods
    ######################
    def create_record(self, article_url, title, description, relevancy):
        """
        Upload a record.
        """
        
        endpoint = self._conn_host + '/Articles'

        #Headers
        headers= {
            "Authorization": "Bearer " + self._conn_api_key,
            "Content-Type":"application/json"
            }
        #Dataframe to json data conversion 
        new_data = {
            "records": [
                {
                    "fields": {
                        "Article URL": article_url,
                        "Title": title,
                        "Description": description,
                        "Relevant": relevancy
                    }
                }
            ]
        }

        #post request
        response = requests.request("POST", endpoint, headers=headers, data=json.dumps(new_data))
        response_json = response.json()
        
        return(response_json)



@resource(
    config_schema={
        "resources": {
            "airtable_client": {
                "config": {
                    "host": StringSource,
                    "api_key": StringSource
                }
            }
        }
    },
    description="Airtable API client.",
)
def airtable_api_client(context):
    return AirtableAPIClient(
        host = context.resource_config["resources"]["airtable_client"]["config"]["host"],
        api_key = context.resource_config["resources"]["airtable_client"]["config"]["api_key"]
    )
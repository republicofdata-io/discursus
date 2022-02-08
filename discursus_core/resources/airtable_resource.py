import requests
from dagster import resource, StringSource, IntSource
from dagster.builtins import String
import pandas as pd
from io import StringIO

class AirtableAPIClient:
    def __init__(self, host, api_key):
        self._conn_host = host
        self._conn_api_key = api_key

        self._session = None
        self._base_url = None
    

    def get_conn(self):
        """
        Returns the connection used by the resource for querying data.
        Should in principle not be used directly.
        """

        if self._session is None:
            self._base_url = self._conn_host

            # Build our session instance, which we will use for any
            # requests to the API.
            self._session = requests.Session()

            self._session.auth = (self._conn_login, self._conn_password)

        return self._session, self._base_url


    def close(self):
        """Closes any active session."""
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None

    
    # API methods
    ######################
    def create_record(self, filename, df_gdelt_articles):
        """
        Upload a dataset.
        """
        
        endpoint = "/dataset/"
        
        session, base_url = self.get_conn()
        url = base_url + endpoint
        
        payload = {
            "name": filename,
            "set_type": "file",
            "file_type": "csv"
        }

        files = [
            ('path',(filename, df_gdelt_articles.to_csv(), 'csv'))
        ]
        
        response = session.post(
            url, data = payload, files = files
        )
        
        response.raise_for_status()
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
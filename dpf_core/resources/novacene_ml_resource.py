import requests
from dagster import resource, StringSource, IntSource
from dagster.builtins import String
import pandas as pd
from io import StringIO

class NovaceneAPIClient:
    def __init__(self, host, login, password, ml_relevancy_classification_model_id):
        self._conn_host = host
        self._conn_login = login
        self._conn_password = password
        self._ml_relevancy_classification_model_id = ml_relevancy_classification_model_id

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
    def create_dataset(self, filename, df_gdelt_articles):
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


    def enrich_dataset(self, dataset_id):
        """
        Enriches a dataset.
        """
        
        endpoint = "/studio/get_local_model_analysis/"
        
        session, base_url = self.get_conn()
        url = base_url + endpoint
        
        payload = {
            "datasetId": int(dataset_id),
            "methodIdx": int(self._ml_relevancy_classification_model_id),
            "colIdx": 5
        }

        response = session.get(
            url, params = payload
        )
        
        response.raise_for_status()
        response_json = response.json()
        
        return(response_json)
    

    def get_file(self, file_url):
        """
        Get file
        """
        
        session, base_url = self.get_conn()

        response = session.get(file_url)
        df_file = pd.read_csv(StringIO(response.text))
        
        return(df_file)



    def job_info(self, job_id):
        """
        Get job information.
        """
        
        endpoint = "/job/" + str(job_id) + "/"
        
        session, base_url = self.get_conn()
        url = base_url + endpoint

        response = session.get(url)
        
        response.raise_for_status()
        response_json = response.json()
        
        return(response_json)



@resource(
    config_schema={
        "resources": {
            "novacene_client": {
                "config": {
                    "host": StringSource,
                    "login": StringSource,
                    "password": StringSource,
                    "ml_relevancy_classification_model_id": IntSource
                }
            }
        }
    },
    description="A Novacene API client.",
)
def novacene_ml_api_client(context):
    return NovaceneAPIClient(
        host = context.resource_config["resources"]["novacene_client"]["config"]["host"],
        login = context.resource_config["resources"]["novacene_client"]["config"]["login"],
        password = context.resource_config["resources"]["novacene_client"]["config"]["password"],
        ml_relevancy_classification_model_id = context.resource_config["resources"]["novacene_client"]["config"]["ml_relevancy_classification_model_id"]
    )
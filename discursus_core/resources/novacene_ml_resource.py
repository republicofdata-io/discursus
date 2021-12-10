from dagster import resource
from dagster.builtins import String


class NovaceneAPIClient:
    def __init__(self, conn_id, retry=3):
        self._conn_id = conn_id
        self._retry = retry
    
    def get_conn(self):
        """
        Returns the connection used by the resource for querying data.
        Should in principle not be used directly.
        """

        if self._session is None:
            # Fetch config for the given connection (host, login, etc).
            config = self.get_connection(self._conn_id)

            if not config.host:
                raise ValueError(f"No host specified in connection {self._conn_id}")

            schema = config.schema or self.DEFAULT_SCHEMA
            port = config.port or self.DEFAULT_PORT

            self._base_url = f"{schema}://{config.host}"

            # Build our session instance, which we will use for any
            # requests to the API.
            self._session = requests.Session()

            if config.login:
                self._session.auth = (config.login, config.password)

        return self._session, self._base_url

    def close(self):
        """Closes any active session."""
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None

    
    # API methods
    ######################
    def list_jobs(self, jobId):
        """
        List all jobs and their information.
        """
        
        endpoint="/job/list/"
        
        session, base_url = self.get_conn()
        url = base_url + endpoint
        
        response = session.get(
            url
        )
        
        response.raise_for_status()
        response_json = response.json()
        
        return(response_json)



@resource(
    config_schema={
        "host": String,
        "login": String,
        "password": String
    },
    description="A Novacene API client.",
)
def novacene_ml_api_client(init_context):
    return NovaceneAPIClient()
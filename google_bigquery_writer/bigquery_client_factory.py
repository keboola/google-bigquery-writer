from google.oauth2.credentials import Credentials
from google.cloud.bigquery import Client


class BigqueryClientFactory(object):
    def __init__(self, project_name: str, credentials: Credentials):
        self.project_name = project_name
        self.credentials = credentials

    def create(self) -> Client:
        return Client(
            self.project_name,
            self.credentials
        )

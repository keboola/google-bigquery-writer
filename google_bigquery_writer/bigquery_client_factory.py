from google.cloud.bigquery import Client


class BigqueryClientFactory(object):
    def __init__(self, project_name, credentials):
        self.project_name = project_name
        self.credentials = credentials

    def create(self):
        return Client(
            self.project_name,
            self.credentials
        )

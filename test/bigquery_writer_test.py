from google.cloud import exceptions
import google.oauth2.credentials
import os
from google_bigquery_writer.bigquery_client_factory import BigqueryClientFactory


class GoogleBigQueryWriterTest(object):
    bigquery_client_factory = None
    bigquery_client = None

    def setup_method(self):
        self.bigquery_client_factory = BigqueryClientFactory(
            os.environ.get('BIGQUERY_PROJECT'),
            self.get_credentials()
        )
        self.bigquery_client = None

    def get_client(self):
        if self.bigquery_client is None:
            self.bigquery_client = self.bigquery_client_factory.create()
        return self.bigquery_client

    def get_credentials(self): 
        return google.oauth2.credentials.Credentials(
            os.environ.get('OAUTH_ACCESS_TOKEN'),
            token_uri='https://accounts.google.com/o/oauth2/token',
            client_id=os.environ.get('OAUTH_CLIENT_ID'),
            client_secret=os.environ.get('OAUTH_CLIENT_SECRET'),
            refresh_token=os.environ.get('OAUTH_REFRESH_TOKEN')
        )

    def delete_dataset(self):
        client = self.get_client()
        dataset_reference = client.dataset(os.environ.get('BIGQUERY_DATASET'))

        try:
            dataset = client.get_dataset(dataset_reference)
        except exceptions.NotFound:
            return

        tables = list(client.list_tables(dataset_reference))
        for table in tables:
            client.delete_table(dataset.table(table.table_id))
        client.delete_dataset(dataset)

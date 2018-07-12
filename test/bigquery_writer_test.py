from google.cloud import bigquery, exceptions
import google.oauth2.credentials
import os


class GoogleBigQueryWriterTest(object):

    def get_client(self):
        return bigquery.Client(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )

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

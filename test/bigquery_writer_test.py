from google.cloud import bigquery
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
            token_uri=os.environ.get('OAUTH_TOKEN_URI'),
            client_id=os.environ.get('OAUTH_CLIENT_ID'),
            client_secret=os.environ.get('OAUTH_CLIENT_SECRET'),
            refresh_token=os.environ.get('OAUTH_REFRESH_TOKEN')
        )

    def delete_dataset(self):
        client = self.get_client()
        dataset = client.dataset(os.environ.get('BIGQUERY_DATASET'))
        if dataset.exists():
            tables = dataset.list_tables()
            for table in dataset.list_tables():
                table_obj = dataset.table(table.name)
                table_obj.delete()
            dataset.delete()
        if dataset.exists():
            raise Exception('Could not delete dataset')

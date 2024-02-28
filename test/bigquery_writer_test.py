from google.cloud import exceptions
from google.cloud.bigquery.dataset import DatasetReference
import os
from google_bigquery_writer.bigquery_client_factory \
    import BigqueryClientFactory
from google.oauth2 import service_account
import json


class GoogleBigQueryWriterTest(object):
    bigquery_client_factory = None
    bigquery_client = None

    def setup_method(self):
        self.bigquery_client = None

    def get_project(self):
        return json.loads(os.environ.get('SERVICE_ACCOUNT_MANAGE'))['project_id']

    def get_client(self, credentials_type='service_account'):
        if self.bigquery_client is None:
            if credentials_type == 'service_account':
                credentials = self.get_service_account_user_credentials()
            elif credentials_type == 'service_account_manage':
                credentials = self.get_service_account_manage_credentials()
            else:
                raise Exception('Unknown credentials type ' + credentials_type)

            self.bigquery_client_factory = BigqueryClientFactory(
                self.get_project(),
                credentials
            )
            self.bigquery_client = self.bigquery_client_factory.create()
        return self.bigquery_client

    def get_service_account_manage_credentials(self):
        service_account_info = json.loads(os.environ.get('SERVICE_ACCOUNT_MANAGE'))
        scopes = [
            'https://www.googleapis.com/auth/bigquery'
        ]
        return service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=scopes
        )

    def get_service_account_user_credentials(self):
        service_account_info = json.loads(os.environ.get('SERVICE_ACCOUNT_USER'))
        scopes = [
            'https://www.googleapis.com/auth/bigquery'
        ]
        return service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=scopes
        )

    def delete_dataset(self):
        client = self.get_client('service_account_manage')
        dataset_reference = DatasetReference(self.get_project(), os.environ.get('BIGQUERY_DATASET'))

        try:
            dataset = client.get_dataset(dataset_reference)
        except exceptions.NotFound:
            return

        tables = list(client.list_tables(dataset_reference))
        for table in tables:
            client.delete_table(dataset.table(table.table_id))
        client.delete_dataset(dataset)

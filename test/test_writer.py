import csv
import pytest
from google_bigquery_writer import writer, exceptions
from google.cloud import bigquery
import google.cloud.bigquery
import google.oauth2.credentials
import os
import sys

class TestWriter(object):

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
            print(tables)
            for table in dataset.list_tables():
                table_obj = dataset.table(table.name)
                table_obj.delete()
            dataset.delete()
        if dataset.exists():
            pytest.fail('Could not delete dataset')

    def teardown_method(self):
        self.delete_dataset()

    def setup_method(self):
        self.delete_dataset()

    def test_missing_credentials(self):
        try:
            writer.Writer()
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert str(err) == 'Cannot connect to BigQuery'
            pass

    def test_invalid_token(self, data_dir):
        credentials = google.oauth2.credentials.Credentials(
            'access_token',
            token_uri=os.environ.get('OAUTH_TOKEN_URI'),
            client_id=os.environ.get('OAUTH_CLIENT_ID'),
            client_secret=os.environ.get('OAUTH_CLIENT_SECRET')
        )
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=credentials
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('col1', 'STRING'),
            bigquery.schema.SchemaField('col2', 'INTEGER')
        ]
        try:
            my_writer.write_table(
                csv_file,
                os.environ.get('BIGQUERY_DATASET'),
                os.environ.get('BIGQUERY_TABLE'),
                schema)
        except exceptions.UserException as err:
            assert str(err) == 'Cannot connect to BigQuery.' \
                ' Check your access token or refresh token.'
            pass

    def test_write_table_async(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('col1', 'STRING'),
            bigquery.schema.SchemaField('col2', 'INTEGER')
        ]
        job = my_writer.write_table(
            csv_file,
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE'),
            schema
        )
        assert job.state == 'RUNNING'
        client = self.get_client()
        dataset = client.dataset(os.environ.get('BIGQUERY_DATASET'))
        assert dataset.exists()
        table = dataset.table(os.environ.get('BIGQUERY_TABLE'))
        assert table.exists()

    def test_write_table_sync(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('col1', 'STRING'),
            bigquery.schema.SchemaField('col2', 'INTEGER')
        ]
        my_writer.write_table_sync(
            csv_file,
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE'),
            schema
        )
        query = 'SELECT * FROM %s.%s' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )
        client = self.get_client()
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 2
        assert row_data[0] == ('val1', 1)
        assert row_data[1] == ('val2', 2)

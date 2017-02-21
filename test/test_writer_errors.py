import csv
import pytest
from google_bigquery_writer import writer, exceptions
from google.cloud import bigquery
import google.cloud.bigquery
import google.oauth2.credentials
import os
import sys
from test.bigquery_writer_test import GoogleBigQueryWriterTest

class TestWriterErrors(GoogleBigQueryWriterTest):

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
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert str(err) == 'Cannot connect to BigQuery.' \
                ' Check your access token or refresh token.'
            pass

    def test_write_table_sync_error_too_many_values(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('column_unknown', 'INTEGER'),
        ]
        try:
            my_writer.write_table_sync(
                csv_file,
                os.environ.get('BIGQUERY_DATASET'),
                os.environ.get('BIGQUERY_TABLE'),
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Too many values in row' in str(err)
            pass

    def test_create_dataset_invalid_name(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = []
        try:
            my_writer.write_table_sync(
                csv_file,
                os.environ.get('BIGQUERY_DATASET') + ' INVALID',
                os.environ.get('BIGQUERY_TABLE'),
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Cannot create dataset' in str(err)
            pass

    def test_create_table_invalid_name(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('column', 'ANYTHING'),
        ]
        try:
            my_writer.write_table_sync(
                csv_file,
                os.environ.get('BIGQUERY_DATASET'),
                os.environ.get('BIGQUERY_TABLE') + ' INVALID',
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Cannot create table' in str(err)
            pass

    def test_create_table_invalid_schema_datatype(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('column', 'ANYTHING'),
        ]
        try:
            my_writer.write_table_sync(
                csv_file,
                os.environ.get('BIGQUERY_DATASET'),
                os.environ.get('BIGQUERY_TABLE'),
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'ANYTHING is not a valid value' in str(err)
            pass
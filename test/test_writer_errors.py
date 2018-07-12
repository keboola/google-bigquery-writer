import pytest
from google_bigquery_writer import writer, exceptions
from google.cloud import bigquery
import google.cloud.bigquery
import google.oauth2.credentials
import os
from test.bigquery_writer_test import GoogleBigQueryWriterTest


class TestWriterErrors(GoogleBigQueryWriterTest):

    def teardown_method(self):
        self.delete_dataset()

    def setup_method(self):
        self.delete_dataset()

    def test_invalid_token(self, data_dir):
        credentials = google.oauth2.credentials.Credentials(
            'access_token',
            token_uri='https://accounts.google.com/o/oauth2/token',
            client_id=os.environ.get('OAUTH_CLIENT_ID'),
            client_secret=os.environ.get('OAUTH_CLIENT_SECRET')
        )
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=credentials
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('dummy', 'INTEGER')
        ]
        try:
            my_writer.write_table(
                csv_file,
                os.environ.get('BIGQUERY_DATASET'),
                {"dbName": os.environ.get('BIGQUERY_TABLE')},
                schema)
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert str(err) == 'Cannot connect to BigQuery.' \
                ' Check your access token or refresh token.'

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
                {"dbName": os.environ.get('BIGQUERY_TABLE')},
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Too many values in row' in str(err)

    def test_write_table_sync_error_missing_values(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('column_unknown1', 'INTEGER'),
            bigquery.schema.SchemaField('column_unknown2', 'INTEGER'),
            bigquery.schema.SchemaField('column_unknown3', 'INTEGER')
        ]
        try:
            my_writer.write_table_sync(
                csv_file,
                os.environ.get('BIGQUERY_DATASET'),
                {"dbName": os.environ.get('BIGQUERY_TABLE')},
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'contains only 2 columns' in str(err)

    def test_write_table_sync_error_invalid_datatype(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('col1', 'INTEGER'),
            bigquery.schema.SchemaField('col2', 'INTEGER'),
        ]
        try:
            my_writer.write_table_sync(
                csv_file,
                os.environ.get('BIGQUERY_DATASET'),
                {"dbName": os.environ.get('BIGQUERY_TABLE')},
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Could not parse \'val1\' as int for field col1' in str(err)

    def test_create_dataset_invalid_name(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('dummy', 'INTEGER')
        ]
        try:
            my_writer.write_table_sync(
                csv_file,
                os.environ.get('BIGQUERY_DATASET') + ' INVALID',
                {"dbName": os.environ.get('BIGQUERY_TABLE')},
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Cannot create dataset' in str(err)

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
                {"dbName": os.environ.get('BIGQUERY_TABLE') + ' INVALID'},
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Cannot create table' in str(err)

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
                {"dbName": os.environ.get('BIGQUERY_TABLE')},
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'ANYTHING is not a valid value' in str(err)

    def test_write_table_sync_with_invalid_column_order(self, data_dir):
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
            {"dbName": os.environ.get('BIGQUERY_TABLE')},
            schema
        )

        invalid_schema = [
            bigquery.schema.SchemaField('col2', 'INTEGER'),
            bigquery.schema.SchemaField('col1', 'STRING')
        ]

        invalid_csv_file = open(data_dir + 'simple_csv_invalid_column_order/in/tables/table.csv')
        try:
            my_writer.write_table_sync(
                invalid_csv_file,
                os.environ.get('BIGQUERY_DATASET'),
                {"dbName": os.environ.get('BIGQUERY_TABLE')},
                invalid_schema,
                True
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Column order mismatch. Actual configuration: col2, col1, BigQuery expected: col1, col2.' in str(err)

    def test_invalid_project(self, data_dir):
        my_writer = writer.Writer(
            project='invalid-project',
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('col1', 'STRING'),
            bigquery.schema.SchemaField('col2', 'INTEGER')
        ]
        try: 
            job = my_writer.write_table_sync(
                csv_file,
                os.environ.get('BIGQUERY_DATASET'),
                {"dbName": os.environ.get('BIGQUERY_TABLE')},
                schema
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Cannot create dataset ds_bigquery. Project invalid-project was not found.' in str(err)

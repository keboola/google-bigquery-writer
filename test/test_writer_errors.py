import pytest
from google_bigquery_writer import writer, exceptions
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
import os
from test.bigquery_writer_test import GoogleBigQueryWriterTest
from test import fixtures


class TestWriterErrors(GoogleBigQueryWriterTest):

    def teardown_method(self):
        self.delete_dataset()

    def setup_method(self):
        super(TestWriterErrors, self).setup_method()
        self.delete_dataset()

    def test_invalid_token(self, data_dir):
        invalid_credentials = Credentials(
            'access-token',
            token_uri='https://accounts.google.com/o/oauth2/token',
            client_id=os.environ.get('OAUTH_CLIENT_ID'),
            client_secret=os.environ.get('OAUTH_CLIENT_SECRET')
        )
        bigquery_client = bigquery.Client(
            os.environ.get('BIGQUERY_PROJECT'),
            invalid_credentials
        )
        my_writer = writer.Writer(bigquery_client)

        try:
            my_writer.write_table(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_simple_csv_table_configuration()
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert str(err) == 'Cannot connect to BigQuery.' \
                ' Check your access token or refresh token.'

    def test_write_table_sync_error_too_many_values(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        my_writer.write_table_sync(
            data_dir + 'simple_csv/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_simple_csv_table_configuration()
        )
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv_with_extra_column/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_simple_csv_with_extra_column_table_configuration(),
                True
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Too many values in row' in str(err)

    def test_write_table_sync_error_missing_values(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        my_writer.write_table_sync(
            data_dir + 'simple_csv_with_extra_column/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_simple_csv_with_extra_column_table_configuration()
        )
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_simple_csv_table_configuration(),
                True
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'contains only 2 columns' in str(err)

    def test_write_table_sync_error_invalid_datatype(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        my_writer.write_table_sync(
            data_dir + 'simple_csv/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_simple_csv_table_configuration()
        )
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv_invalid_data_types/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_simple_csv_invalid_data_types_table_configuration(),
                True
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Could not parse \'val1\' as int for field col2' in str(err)

    def test_create_dataset_invalid_name(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET') + ' INVALID',
                fixtures.get_simple_csv_table_configuration()
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Cannot create dataset' in str(err)

    def test_create_table_invalid_name(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_simple_csv_table_configuration_with_invalid_table_name()
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Cannot create table' in str(err)

    def test_create_table_invalid_schema_datatype(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_simple_csv_table_configuration_with_invalid_datatype()
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'INVALID-DATATYPE is not a valid value' in str(err)

    def test_write_table_sync_with_invalid_column_order(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        my_writer.write_table_sync(
            data_dir + 'simple_csv/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_simple_csv_table_configuration()
        )
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv_invalid_column_order/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_simple_csv_invalid_column_order_table_configuration(),
                True
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Column order mismatch. Actual configuration: col2, col1. Expected BigQuery: col1, col2.' in str(err)

    def test_invalid_project(self, data_dir):
        bigquery_client = bigquery.Client(
            'invalid-project',
            self.get_credentials()
        )

        my_writer = writer.Writer(bigquery_client)
        try: 
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_simple_csv_table_configuration()
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Project invalid-project was not found.' in str(err)

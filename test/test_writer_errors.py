import os
import pytest
import requests
from unittest.mock import MagicMock
from google_bigquery_writer import writer, exceptions
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
from google.auth.exceptions import RefreshError
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
            self.get_project(),
            invalid_credentials
        )
        my_writer = writer.Writer(bigquery_client)

        try:
            my_writer.write_table(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_table_configuration()
            )
            pytest.fail('Must raise exception.')
        except RefreshError as err:
            assert str(err) == 'The credentials do not contain the necessary' \
                               ' fields need to refresh the access token.' \
                               ' You must specify refresh_token, token_uri,' \
                               ' client_id, and client_secret.'

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_write_table_sync_error_too_many_values(self, data_dir, credentials_type):
        my_writer = writer.Writer(self.get_client(credentials_type=credentials_type))
        my_writer.write_table_sync(
            data_dir + 'simple_csv/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration()
        )
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv_with_extra_column/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_table_configuration_with_extra_column(),
                True
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Too many values in line' in str(err)

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_write_table_sync_error_missing_values(self, data_dir, credentials_type):
        my_writer = writer.Writer(self.get_client(credentials_type=credentials_type))
        my_writer.write_table_sync(
            data_dir + 'simple_csv_with_extra_column/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration_with_extra_column()
        )
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_table_configuration(),
                True
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'contains only 2 columns' in str(err)

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_write_table_sync_error_invalid_datatype(self, data_dir, credentials_type):
        my_writer = writer.Writer(self.get_client(credentials_type=credentials_type))
        my_writer.write_table_sync(
            data_dir + 'simple_csv/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration()
        )
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv_invalid_data_types/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_table_configuration_with_invalid_data_type(),
                True
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'column_index: 1 column_name: "col2" column_type: INT64 value: "val2"' in str(err)

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_create_dataset_invalid_name(self, data_dir, credentials_type):
        my_writer = writer.Writer(self.get_client(credentials_type=credentials_type))
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET') + ' INVALID',
                fixtures.get_table_configuration()
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Invalid dataset ID "writer_gh_actions INVALID"' in str(err)

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_create_table_invalid_name(self, data_dir, credentials_type):
        my_writer = writer.Writer(self.get_client(credentials_type=credentials_type))
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_table_configuration_with_invalid_table_name()
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Cannot create table' in str(err)

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_create_table_invalid_schema_datatype(self, data_dir, credentials_type):
        my_writer = writer.Writer(self.get_client(credentials_type=credentials_type))
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_table_configuration_with_invalid_datatype()
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'INVALID-DATATYPE is not a valid value' in str(err)

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_write_table_sync_with_invalid_column_order(self, data_dir, credentials_type):
        my_writer = writer.Writer(self.get_client(credentials_type=credentials_type))
        my_writer.write_table_sync(
            data_dir + 'simple_csv/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration()
        )
        try:
            my_writer.write_table_sync(
                '%ssimple_csv_invalid_column_order/in/tables/table.csv'
                % data_dir,
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_table_configuration_with_invalid_column_order(),
                True
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Column order mismatch. Actual configuration: '\
                   'col2, col1. Expected BigQuery: col1, col2.'\
                   in str(err)

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_invalid_project(self, data_dir, credentials_type):
        if (credentials_type == 'service_account'):
            credentials = self.get_service_account_user_credentials()
        else:
            credentials = self.get_oauth_credentials()

        bigquery_client = bigquery.Client(
            'invalid-project',
            credentials
        )

        my_writer = writer.Writer(bigquery_client)
        try:
            my_writer.write_table_sync(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_table_configuration()
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert 'Project invalid-project was not found.' in str(err)

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_write_table_connection_error(self, data_dir, credentials_type):
        client = self.get_client(credentials_type=credentials_type)
        error_msg = "Some connection error!"
        client.load_table_from_file = MagicMock(side_effect=requests.exceptions.ConnectionError(error_msg))
        my_writer = writer.Writer(client)
        try:
            my_writer.write_table(
                data_dir + 'simple_csv/in/tables/table.csv',
                os.environ.get('BIGQUERY_DATASET'),
                fixtures.get_table_configuration()
            )
        except exceptions.UserException as err:
            msg = 'Loading data into table {}.{} failed: {}'\
                .format(os.environ.get('BIGQUERY_DATASET'), os.environ.get('BIGQUERY_TABLE'), error_msg)
            assert msg in str(err)

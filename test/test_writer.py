import csv
import pytest
import google_auth_httplib2
import google.auth.credentials
import oauth2client.client
from google_bigquery_extractor import writer, exceptions
from google.cloud import bigquery
from google import auth


class TestWriter(object):

    def test_missing_credentials(self):
        try:
            writer.Writer()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == "Cannot connect to BigQuery"
            pass

    def test_invalid_token(self):
        credentials = oauth2client.client.GoogleCredentials('mytoken', 'client_id', 'client_secret', 'refresh_token', 'expiry', 'token_uri', 'user_agent')
        writer.Writer(project='big-silo-88312', credentials=credentials)

'''
    def test_write_table(self, data_dir):
        credentials = oauth2client.client.AccessTokenCredentials('mytoken', 'KBC Test')
        my_writer = writer.Writer(project='big-silo-88312')
        csv_file = open(data_dir + 'simple_csv/in/tables/file.csv')
        schema = [
            bigquery.schema.SchemaField('col1', 'STRING'),
            bigquery.schema.SchemaField('col2', 'INTEGER')
        ]
        my_writer.write_table(csv_file, 'my_dataset', 'my_table', schema)
'''
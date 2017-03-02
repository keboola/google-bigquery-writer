import pytest
from google_bigquery_writer import app, exceptions
from test.bigquery_writer_test import GoogleBigQueryWriterTest
import os
import json
import shutil
from datetime import datetime
from datetime import timezone


class TestApp(GoogleBigQueryWriterTest):

    def teardown_method(self):
        self.delete_dataset()

    def setup_method(self):
        self.delete_dataset()

    def prepare(self, action="run", data_dir=None):
        if os.path.exists(data_dir + "sample_populated"):
            shutil.rmtree(data_dir + "sample_populated")

        # populate config file
        source_config_file_path = data_dir + "sample/config.json"
        dst_config_file_path = data_dir + "sample_populated/config.json"
        os.makedirs(data_dir + "sample_populated/")
        with open(source_config_file_path) as source_config_file:
            data = json.load(source_config_file)

        oauth = {
            "access_token": os.environ.get('OAUTH_ACCESS_TOKEN'),
            "expires_in": 3600,
            "refresh_token": os.environ.get('OAUTH_REFRESH_TOKEN'),
            "token_type": "Bearer"
        }

        data['authorization'] = {
            'oauth_api': {
                'credentials': {
                    'appKey': os.environ.get('OAUTH_CLIENT_ID'),
                    '#appSecret': os.environ.get('OAUTH_CLIENT_SECRET'),
                    '#data': json.dumps(oauth)
                }
            }
        }

        data['parameters']['project'] = os.environ.get('BIGQUERY_PROJECT')
        data['parameters']['dataset'] = os.environ.get('BIGQUERY_DATASET')
        data['action'] = action

        with open(dst_config_file_path, 'w+') as dst_config_file:
            json.dump(data, dst_config_file)

        # copy data tables
        dst_dir = data_dir + "sample_populated/in/tables"
        src_dir = data_dir + "sample/in/tables"
        os.makedirs(dst_dir)
        shutil.copyfile(
            src_dir + '/in.c-bucket.table1.csv',
            dst_dir + '/in.c-bucket.table1.csv'
        )
        shutil.copyfile(
            src_dir + '/in.c-bucket.table2.csv',
            dst_dir + '/in.c-bucket.table2.csv'
        )

    def test_successful_run(self, data_dir, capsys):
        self.prepare(action="run", data_dir=data_dir)
        # run app
        application = app.App(data_dir + "sample_populated/")
        application.run()

        # assertions
        out, err = capsys.readouterr()
        assert err == ''
        assert out == 'Loading table in.c-bucket.table1 into BigQuery ' \
            'as %s.table1\n' \
            'Loading table in.c-bucket.table2 into BigQuery as %s.table2\n' \
            'BigQuery Writer finished\n' % (
                os.environ.get('BIGQUERY_DATASET'),
                os.environ.get('BIGQUERY_DATASET')
            )

        client = self.get_client()

        # check for only the testing dataset
        datasets = list(client.list_datasets())
        assert len(datasets) >= 1
        # todo find the required dataset
        matching_datasets = list(filter(
            lambda dataset: dataset.name == os.environ.get('BIGQUERY_DATASET'), datasets
        ))

        assert len(matching_datasets) == 1
        assert matching_datasets[0].name == os.environ.get('BIGQUERY_DATASET')

        tables = list(matching_datasets[0].list_tables())
        assert len(tables) == 2
        assert tables[0].name == 'table1'
        assert tables[1].name == 'table2'

        table = matching_datasets[0].table('table1')
        table.reload()
        rcvd_schema = table.schema
        assert rcvd_schema[0].field_type == 'STRING'
        assert rcvd_schema[0].fields is None
        assert rcvd_schema[0].mode == 'NULLABLE'
        assert rcvd_schema[0].name == 'string'
        assert rcvd_schema[1].field_type == 'INTEGER'
        assert rcvd_schema[1].fields is None
        assert rcvd_schema[1].mode == 'NULLABLE'
        assert rcvd_schema[1].name == 'integer'
        assert rcvd_schema[2].field_type == 'FLOAT'
        assert rcvd_schema[2].fields is None
        assert rcvd_schema[2].mode == 'NULLABLE'
        assert rcvd_schema[2].name == 'float'
        assert rcvd_schema[3].field_type == 'BOOLEAN'
        assert rcvd_schema[3].fields is None
        assert rcvd_schema[3].mode == 'NULLABLE'
        assert rcvd_schema[3].name == 'boolean'
        assert rcvd_schema[4].field_type == 'TIMESTAMP'
        assert rcvd_schema[4].fields is None
        assert rcvd_schema[4].mode == 'NULLABLE'
        assert rcvd_schema[4].name == 'timestamp'

        query = 'SELECT * FROM %s.%s' % (
            os.environ.get('BIGQUERY_DATASET'),
            'table1'
        )
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 3
        assert row_data[0][0] == 'MyString'
        assert row_data[0][1] == 123456
        assert row_data[0][2] == 123.456
        assert row_data[0][3] is True
        assert row_data[0][4] == datetime(2014, 8, 19, 12, 41, 35, 220000,
                                          tzinfo=timezone.utc)
        assert row_data[1] == ('', 0, 0, False, None)
        assert row_data[2] == (None, None, None, None, None)

        query = 'SELECT * FROM %s.%s' % (
            os.environ.get('BIGQUERY_DATASET'),
            'table2'
        )
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 3

        # run app second time (increments)
        application = app.App(data_dir + "sample_populated/")
        application.run()

        query = 'SELECT * FROM %s.%s' % (
            os.environ.get('BIGQUERY_DATASET'),
            'table1'
        )
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 3

        query = 'SELECT * FROM %s.%s' % (
            os.environ.get('BIGQUERY_DATASET'),
            'table2'
        )
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 6

        out, err = capsys.readouterr()
        assert err == ''
        assert out == 'Loading table in.c-bucket.table1 into BigQuery ' \
            'as %s.table1\n' \
            'Loading table in.c-bucket.table2 into BigQuery as %s.table2\n' \
            'BigQuery Writer finished\n' % (
                os.environ.get('BIGQUERY_DATASET'),
                os.environ.get('BIGQUERY_DATASET')
            )

    def test_list_projects(self, data_dir, capsys):
        self.prepare(action="listProjects", data_dir=data_dir)
        application = app.App(data_dir + "sample_populated/")
        application.run()
        out, err = capsys.readouterr()
        assert err == ''
        data = json.loads(out)
        assert os.environ.get('BIGQUERY_PROJECT') in map(
            lambda project: project['id'],
            data
        )

    def test_list_datasets(self, data_dir, capsys):
        client = self.get_client()
        dataset = client.dataset(os.environ.get('BIGQUERY_DATASET'))
        dataset.create()

        self.prepare(action="listDatasets", data_dir=data_dir)
        application = app.App(data_dir + "sample_populated/")
        application.run()
        out, err = capsys.readouterr()
        assert err == ''
        data = json.loads(out)
        assert 1 == len(data)
        assert os.environ.get('BIGQUERY_DATASET') in map(
            lambda dataset: dataset['id'],
            data
        )

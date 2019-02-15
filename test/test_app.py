from google_bigquery_writer import app
from test.bigquery_writer_test import GoogleBigQueryWriterTest
import os
import json
import shutil
from datetime import datetime
from datetime import timezone
from google_bigquery_writer.exceptions import UserException
import pytest
from google.cloud import bigquery


class TestApp(GoogleBigQueryWriterTest):
    def teardown_method(self):
        self.delete_dataset()

    def setup_method(self):
        super(TestApp, self).setup_method()
        self.delete_dataset()

    def prepare(self, action='run', data_dir=None, credentials_type='service_account'):
        if os.path.exists(data_dir + 'sample_populated'):
            shutil.rmtree(data_dir + 'sample_populated')

        # populate config file
        source_config_file_path = data_dir + 'sample/config.json'
        dst_config_file_path = data_dir + 'sample_populated/config.json'
        os.makedirs(data_dir + 'sample_populated/')
        with open(source_config_file_path) as source_config_file:
            data = json.load(source_config_file)

        if credentials_type == 'service_account':
            service_account_info = json.loads(os.environ.get('SERVICE_ACCOUNT_USER'))
            data['parameters']['service_account'] = {
                '#private_key': service_account_info['private_key'],
                'client_email': service_account_info['client_email'],
                'token_uri': service_account_info['token_uri'],
                'project_id': service_account_info['project_id']
            }
        elif credentials_type == 'oauth':
            oauth = {
                'access_token': os.environ.get('OAUTH_ACCESS_TOKEN'),
                'expires_in': 3600,
                'refresh_token': os.environ.get('OAUTH_REFRESH_TOKEN'),
                'token_type': 'Bearer'
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

        else:
            raise Exception('Unknown credentials type ' + credentials_type)

        data['parameters']['dataset'] = os.environ.get('BIGQUERY_DATASET')
        data['action'] = action

        with open(dst_config_file_path, 'w+') as dst_config_file:
            json.dump(data, dst_config_file)

        # copy data tables
        dst_dir = data_dir + 'sample_populated/in/tables'
        src_dir = data_dir + 'sample/in/tables'
        os.makedirs(dst_dir)
        shutil.copyfile(
            src_dir + '/in.c-bucket.table1.csv',
            dst_dir + '/in.c-bucket.table1.csv'
        )
        shutil.copyfile(
            src_dir + '/in.c-bucket.table2.csv',
            dst_dir + '/in.c-bucket.table2.csv'
        )
        shutil.copyfile(
            src_dir + '/in.c-bucket.table1.csv.manifest',
            dst_dir + '/in.c-bucket.table1.csv.manifest'
        )
        shutil.copyfile(
            src_dir + '/in.c-bucket.table2.csv.manifest',
            dst_dir + '/in.c-bucket.table2.csv.manifest'
        )

    def test_run_authorization_missing_user_exception(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + 'missing_authorization/'
        application = app.App()
        try:
            application.run()
            pytest.fail('Must raise exception')
        except UserException as err:
            assert str(err) ==\
                   'Authorization missing.'

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_successful_run(self, data_dir, capsys, credentials_type):
        os.environ['KBC_DATADIR'] = '%ssample_populated/'\
                                    % data_dir
        self.prepare(action='run', data_dir=data_dir, credentials_type=credentials_type)
        # run app
        application = app.App()
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

        client = self.get_client('service_account_manage')

        # check for only the testing dataset
        datasets = list(client.list_datasets())
        assert len(datasets) >= 1
        # todo find the required dataset
        matching_datasets = list(filter(
            lambda dataset:
                dataset.dataset_id == os.environ.get(
                    'BIGQUERY_DATASET'
                ),
            datasets
        ))

        assert len(matching_datasets) == 1
        assert matching_datasets[0].dataset_id == \
            os.environ.get('BIGQUERY_DATASET')

        tables = list(client.list_tables(matching_datasets[0].reference))
        assert len(tables) == 2
        assert tables[0].reference.table_id == 'table1'
        assert tables[1].reference.table_id == 'table2'

        table_reference = matching_datasets[0].table('table1')
        table = client.get_table(table_reference)

        rcvd_schema = table.schema
        assert rcvd_schema[0].field_type == 'STRING'
        assert rcvd_schema[0].fields == ()
        assert rcvd_schema[0].mode == 'NULLABLE'
        assert rcvd_schema[0].name == 'string'
        assert rcvd_schema[1].field_type == 'INTEGER'
        assert rcvd_schema[1].fields == ()
        assert rcvd_schema[1].mode == 'NULLABLE'
        assert rcvd_schema[1].name == 'integer'
        assert rcvd_schema[2].field_type == 'FLOAT'
        assert rcvd_schema[2].fields == ()
        assert rcvd_schema[2].mode == 'NULLABLE'
        assert rcvd_schema[2].name == 'float'
        assert rcvd_schema[3].field_type == 'BOOLEAN'
        assert rcvd_schema[3].fields == ()
        assert rcvd_schema[3].mode == 'NULLABLE'
        assert rcvd_schema[3].name == 'boolean'
        assert rcvd_schema[4].field_type == 'TIMESTAMP'
        assert rcvd_schema[4].fields == ()
        assert rcvd_schema[4].mode == 'NULLABLE'
        assert rcvd_schema[4].name == 'timestamp'

        query = 'SELECT * FROM %s.%s ORDER BY 1 DESC' % (
            os.environ.get('BIGQUERY_DATASET'),
            'table1'
        )

        query_job = client.query(query)
        row_data = list(query_job)

        assert len(row_data) == 3
        assert row_data[0][0] == 'MyString'
        assert row_data[0][1] == 123456
        assert row_data[0][2] == 123.456
        assert row_data[0][3] is True
        assert row_data[0][4] == datetime(2014, 8, 19, 12, 41, 35, 220000,
                                          tzinfo=timezone.utc)
        assert row_data[1][0] == ''
        assert row_data[1][1] == 0
        assert row_data[1][2] == 0
        assert row_data[1][3] is False
        assert row_data[1][4] is None

        assert row_data[2][0] is None
        assert row_data[2][1] is None
        assert row_data[2][2] is None
        assert row_data[2][3] is None
        assert row_data[2][4] is None

        query = 'SELECT * FROM %s.%s' % (
            os.environ.get('BIGQUERY_DATASET'),
            'table2'
        )
        query_job = client.query(query)

        row_data = list(query_job)
        assert len(row_data) == 3

        # run app second time (increments)
        application = app.App()
        application.run()

        query = 'SELECT * FROM %s.%s' % (
            os.environ.get('BIGQUERY_DATASET'),
            'table1'
        )
        query_job = client.query(query)
        row_data = list(query_job)
        assert len(row_data) == 3

        query = 'SELECT * FROM %s.%s' % (
            os.environ.get('BIGQUERY_DATASET'),
            'table2'
        )
        query_job = client.query(query)
        row_data = list(query_job)
        assert len(row_data) == 6

        out, err = capsys.readouterr()
        assert err == ''
        assert out == 'Loading table in.c-bucket.table1 into BigQuery ' \
            'as %s.table1\n' \
            'Loading table in.c-bucket.table2 into BigQuery as %s.table2\n' \
            'BigQuery Writer finished\n' % (
                os.environ.get('BIGQUERY_DATASET'),
                os.environ.get('BIGQUERY_DATASET')
            )

    @pytest.mark.parametrize('credentials_type', ['oauth', 'service_account'])
    def test_list(self, data_dir, capsys, credentials_type):
        client = self.get_client('service_account_manage')
        dataset_reference = bigquery.DatasetReference(
            self.get_project(),
            os.environ.get('BIGQUERY_DATASET')
        )
        dataset = bigquery.Dataset(dataset_reference)
        client.create_dataset(dataset)

        os.environ['KBC_DATADIR'] = data_dir + 'sample_populated/'
        self.prepare(action='list', data_dir=data_dir, credentials_type=credentials_type)
        application = app.App()
        application.run()
        out, err = capsys.readouterr()
        assert err == ''
        data = json.loads(out)
        assert 'projects' in data.keys()
        assert self.get_project() in map(
            lambda project: project['id'],
            data['projects']
        )
        project = list(filter(
            lambda project: project['id'] == self.get_project(),
            data['projects']
        ))[0]
        assert os.environ.get('BIGQUERY_DATASET') in map(
            lambda dataset: dataset['id'],
            project['datasets']
        )

from test.bigquery_writer_test import GoogleBigQueryWriterTest
import os
import json
import shutil
from google_bigquery_writer.exceptions import UserException
import pytest
from google_bigquery_writer import app


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

        else:
            raise Exception('Unknown credentials type ' + credentials_type)

        data['parameters']['dataset'] = 'invalid-dataset-name'
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

    def test_invalid_dataset_name(self, data_dir, capsys):
        os.environ['KBC_DATADIR'] = '%ssample_populated/'\
                                    % data_dir
        self.prepare(action='run', data_dir=data_dir)
        application = app.App()

        with pytest.raises(UserException, match=r'Invalid dataset ID'):
            application.run()

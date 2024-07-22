from google_bigquery_writer import writer
from google.cloud import exceptions
import os
from test.bigquery_writer_test import GoogleBigQueryWriterTest
import pytest
from test import fixtures


class TestWriter(GoogleBigQueryWriterTest):

    def teardown_method(self):
        self.delete_dataset()

    def setup_method(self):
        super(TestWriter, self).setup_method()
        self.delete_dataset()

    def test_write_table_async(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        job = my_writer.write_table(
            data_dir + 'simple_csv/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration()
        )
        assert job[0].state == 'RUNNING'

        client = self.get_client('service_account_manage')
        dataset_reference = client.get_dataset(os.environ.get('BIGQUERY_DATASET'))
        try:
            dataset = client.get_dataset(dataset_reference)
        except exceptions.NotFound:
            pytest.fail('Must not raise an exception.')

        table_reference = dataset.table(os.environ.get('BIGQUERY_TABLE'))
        try:
            client.get_table(table_reference)
        except exceptions.NotFound:
            pytest.fail('Must not raise an exception.')

    def test_write_table_sync(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        my_writer.write_table_sync(
            data_dir + 'simple_csv/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration(),
        )
        query = 'SELECT * FROM %s.%s ORDER BY 1 ASC' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )
        client = self.get_client('service_account_manage')
        query_job = client.query(query)
        row_data = list(query_job)

        assert len(row_data) == 2
        assert row_data[0].col1 == 'val1'
        assert row_data[0].col2 == 1
        assert row_data[1].col1 == 'val2'
        assert row_data[1].col2 == 2

    def test_write_table_schema(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        my_writer.write_table_sync(
            data_dir + 'simple_csv/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration()
        )

        client = self.get_client('service_account_manage')
        dataset = client.get_dataset(os.environ.get('BIGQUERY_DATASET'))
        table_reference = dataset.table(os.environ.get('BIGQUERY_TABLE'))
        table = client.get_table(table_reference)

        assert table.schema[0].field_type == 'STRING'
        assert table.schema[0].fields == ()
        assert table.schema[0].mode == 'NULLABLE'
        assert table.schema[0].name == 'col1'
        assert table.schema[1].field_type == 'INTEGER'
        assert table.schema[1].fields == ()
        assert table.schema[1].mode == 'NULLABLE'
        assert table.schema[1].name == 'col2'

    def test_write_table_sync_overwrite(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        csv_file_path = data_dir + 'simple_csv/in/tables/table.csv'
        my_writer.write_table_sync(
            csv_file_path,
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration()
        )
        my_writer.write_table_sync(
            csv_file_path,
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration()
        )

        query = 'SELECT * FROM %s.%s ORDER BY 1 ASC' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )

        client = self.get_client('service_account_manage')
        query_job = client.query(query)

        row_data = list(query_job)
        assert len(row_data) == 2
        assert row_data[0].col1 == 'val1'
        assert row_data[0].col2 == 1
        assert row_data[1].col1 == 'val2'
        assert row_data[1].col2 == 2

    def test_write_table_sync_append(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        csv_file_path = data_dir + 'simple_csv/in/tables/table.csv'
        my_writer.write_table_sync(
            csv_file_path,
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration()
        )
        my_writer.write_table_sync(
            csv_file_path,
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration(),
            incremental=True
        )
        query = 'SELECT * FROM %s.%s ORDER BY 1 ASC, 2 ASC' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )

        client = self.get_client('service_account_manage')
        query_job = client.query(query)

        row_data = list(query_job)
        assert len(row_data) == 4
        assert row_data[0].col1 == 'val1'
        assert row_data[0].col2 == 1
        assert row_data[1].col1 == 'val1'
        assert row_data[1].col2 == 1
        assert row_data[2].col1 == 'val2'
        assert row_data[2].col2 == 2
        assert row_data[3].col1 == 'val2'
        assert row_data[3].col2 == 2

    def test_write_table_sync_newlines(self, data_dir):
        my_writer = writer.Writer(self.get_client())
        my_writer.write_table_sync(
            data_dir + 'newlines/in/tables/table.csv',
            os.environ.get('BIGQUERY_DATASET'),
            fixtures.get_table_configuration()
        )
        query = 'SELECT * FROM %s.%s ORDER BY 1 ASC' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )

        client = self.get_client('service_account_manage')
        query_job = client.query(query)

        row_data = list(query_job)
        assert len(row_data) == 2
        assert row_data[0].col1 == 'val1\non new line'
        assert row_data[0].col2 == 1
        assert row_data[1].col1 == 'val2'
        assert row_data[1].col2 == 2

from google_bigquery_writer import writer
from google.cloud import bigquery, exceptions
import os
from test.bigquery_writer_test import GoogleBigQueryWriterTest
import pytest


class TestWriter(GoogleBigQueryWriterTest):

    def teardown_method(self):
        self.delete_dataset()

    def setup_method(self):
        self.delete_dataset()

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
            {"dbName": os.environ.get('BIGQUERY_TABLE')},
            schema
        )
        assert job.state == 'RUNNING'

        client = self.get_client()
        dataset_reference = client.dataset(os.environ.get('BIGQUERY_DATASET'))
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
        query = 'SELECT * FROM %s.%s ORDER BY 1 ASC' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )
        client = self.get_client()
        query_job = client.query(query)
        row_data = list(query_job)

        assert len(row_data) == 2
        assert row_data[0].col1 == 'val1'
        assert row_data[0].col2 == 1
        assert row_data[1].col1 == 'val2'
        assert row_data[1].col2 == 2

    def test_write_table_sync_different_columns(self, data_dir):
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'simple_csv/in/tables/table.csv')
        schema = [
            bigquery.schema.SchemaField('anything1', 'STRING'),
            bigquery.schema.SchemaField('anything2', 'INTEGER')
        ]
        my_writer.write_table_sync(
            csv_file,
            os.environ.get('BIGQUERY_DATASET'),
            {"dbName": os.environ.get('BIGQUERY_TABLE')},
            schema
        )
        query = 'SELECT * FROM %s.%s ORDER BY 1 ASC' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )
        client = self.get_client()
        query_job = client.query(query)

        row_data = list(query_job)
        assert len(row_data) == 2
        assert row_data[0].anything1 == 'val1'
        assert row_data[0].anything2 == 1
        assert row_data[1].anything1 == 'val2'
        assert row_data[1].anything2 == 2

    def test_write_table_schema(self, data_dir):
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
        client = self.get_client()
        dataset = client.dataset(os.environ.get('BIGQUERY_DATASET'))
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
        my_writer.write_table_sync(
            csv_file,
            os.environ.get('BIGQUERY_DATASET'),
            {"dbName": os.environ.get('BIGQUERY_TABLE')},
            schema
        )
        query = 'SELECT * FROM %s.%s ORDER BY 1 ASC' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )
        client = self.get_client()
        query_job = client.query(query)

        row_data = list(query_job)
        assert len(row_data) == 2
        assert row_data[0].col1 == 'val1'
        assert row_data[0].col2 == 1
        assert row_data[1].col1 == 'val2'
        assert row_data[1].col2 == 2

    def test_write_table_sync_append(self, data_dir):
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
        my_writer.write_table_sync(
            csv_file,
            os.environ.get('BIGQUERY_DATASET'),
            {"dbName": os.environ.get('BIGQUERY_TABLE')},
            schema,
            incremental=True
        )
        query = 'SELECT * FROM %s.%s ORDER BY 1 ASC, 2 ASC' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )
        client = self.get_client()
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
        my_writer = writer.Writer(
            project=os.environ.get('BIGQUERY_PROJECT'),
            credentials=self.get_credentials()
        )
        csv_file = open(data_dir + 'newlines/in/tables/table.csv')
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
        query = 'SELECT * FROM %s.%s ORDER BY 1 ASC' % (
            os.environ.get('BIGQUERY_DATASET'),
            os.environ.get('BIGQUERY_TABLE')
        )
        client = self.get_client()
        query_job = client.query(query)

        row_data = list(query_job)
        assert len(row_data) == 2
        assert row_data[0].col1 == 'val1\non new line'
        assert row_data[0].col2 == 1
        assert row_data[1].col1 == 'val2'
        assert row_data[1].col2 == 2

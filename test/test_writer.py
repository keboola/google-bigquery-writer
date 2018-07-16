from google_bigquery_writer import writer
from google.cloud import bigquery
import os
from test.bigquery_writer_test import GoogleBigQueryWriterTest


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
        dataset = client.dataset(os.environ.get('BIGQUERY_DATASET'))
        assert dataset.exists()
        table = dataset.table(os.environ.get('BIGQUERY_TABLE'))
        assert table.exists()

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
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 2
        assert row_data[0] == ('val1', 1)
        assert row_data[1] == ('val2', 2)

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
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 2
        assert row_data[0] == ('val1', 1)
        assert row_data[1] == ('val2', 2)

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
        table = dataset.table(os.environ.get('BIGQUERY_TABLE'))
        table.reload()
        rcvd_schema = table.schema
        assert rcvd_schema[0].field_type == 'STRING'
        assert rcvd_schema[0].fields is None
        assert rcvd_schema[0].mode == 'NULLABLE'
        assert rcvd_schema[0].name == 'col1'
        assert rcvd_schema[1].field_type == 'INTEGER'
        assert rcvd_schema[1].fields is None
        assert rcvd_schema[1].mode == 'NULLABLE'
        assert rcvd_schema[1].name == 'col2'

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
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 2
        assert row_data[0] == ('val1', 1)
        assert row_data[1] == ('val2', 2)

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
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 4
        assert row_data[0] == ('val1', 1)
        assert row_data[1] == ('val1', 1)
        assert row_data[2] == ('val2', 2)
        assert row_data[3] == ('val2', 2)

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
        query_obj = client.run_sync_query(query)
        query_obj.run()
        (row_data, total_rows, page_token) = query_obj.fetch_data()
        assert total_rows == 2
        assert row_data[0] == ('val1\non new line', 1)
        assert row_data[1] == ('val2', 2)

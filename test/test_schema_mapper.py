import pytest
from google_bigquery_writer import schema_mapper, exceptions
from google.cloud import bigquery


class TestSchema(object):

    def test_get_schema_field(self):
        column_definition = {'dbName': 'test', 'type': 'STRING'}
        schema_field = schema_mapper.get_schema_field(column_definition)
        expected = bigquery.schema.SchemaField('test', 'STRING', mode='NULLABLE')
        assert schema_field == expected

    def test_get_schema(self):
        table_definition = {
            'items': [
                {'dbName': 'test', 'type': 'STRING'},
                {'dbName': 'test2', 'type': 'INTEGER'}
            ]
        }
        table_schema = schema_mapper.get_schema(table_definition)
        assert len(table_schema) == 2

    def test_is_table_definition_in_match_with_bigquery(self):
        dataset_reference = bigquery.DatasetReference('project', 'dataset')
        table_reference = bigquery.TableReference(dataset_reference, 'table')
        schema = [
            bigquery.SchemaField('col1', 'INTEGER'),
            bigquery.SchemaField('col2', 'STRING')
            ]
        table = bigquery.Table(table_reference, schema)

        assert schema_mapper.is_table_definition_in_match_with_bigquery(schema, table)

    def test_is_table_definition_in_match_with_bigquery_throw_user_exception(self):
        dataset_reference = bigquery.DatasetReference('project', 'dataset')
        table_reference = bigquery.TableReference(dataset_reference, 'table')
        schema = [
            bigquery.SchemaField('col1', 'INTEGER'),
            bigquery.SchemaField('col2', 'STRING')
        ]
        table = bigquery.Table(table_reference, schema)

        invalid_schema = [
            bigquery.SchemaField('col2', 'STRING'),
            bigquery.SchemaField('col1', 'INTEGER')
        ]
        try:
            schema_mapper.is_table_definition_in_match_with_bigquery(invalid_schema, table)
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert str(err) == "Column order mismatch. Actual configuration: col2, col1. Expected BigQuery: col1, col2."

    def test_is_csv_file_in_match_with_definition(self):
        csv_columns = ['col1', 'col2']
        table_definition = {
            'items': [
                {'dbName': 'col1', "name": "col1", 'type': 'INTEGER'},
                {'dbName': 'col2', "name": "col2", 'type': 'STRING'}
            ]
        }

        assert schema_mapper.is_csv_in_match_with_table_definition(table_definition, csv_columns)

    def test_is_csv_file_in_match_with_definition_throws_user_exception(self):
        csv_columns = ['col2', 'col1']
        table_definition = {
            'items': [
                {'dbName': 'col1', "name": "col1", 'type': 'INTEGER'},
                {'dbName': 'col2', "name": "col2", 'type': 'STRING'}
            ]
        }

        try:
            schema_mapper.is_csv_in_match_with_table_definition(table_definition, csv_columns)
            pytest.fail("Must raise exception")
        except exceptions.UserException as err:
            assert str(err) == "Column order mismatch. Actual configuration: col1, col2. Expected csv: col2, col1."

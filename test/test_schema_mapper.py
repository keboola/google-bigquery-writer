import pytest
from google_bigquery_writer import schema_mapper
from google.cloud import bigquery
from google_bigquery_writer import exceptions


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
            assert str(err) == "Unexpected column order in configuration. Actual: col1, col2, expected: col2, col1."

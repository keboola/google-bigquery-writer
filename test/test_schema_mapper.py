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

    def test_get_schema_missing_items(self):
        table_definition = {
            'items__x': [
            ]
        }
        try:
            schema_mapper.get_schema(table_definition)
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == "Key 'items' not defined in table definition"
            pass
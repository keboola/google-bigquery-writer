import json
import pytest
from google_bigquery_writer import schema_mapper, exceptions
from google.cloud import bigquery


class TestSchema(object):

    def test_get_schema_field(self):
        column_definition = {'dbName': 'test', 'type': 'STRING'}
        schema_field = schema_mapper.get_schema_field(column_definition)
        expected = bigquery.schema.SchemaField(
            'test',
            'STRING',
            mode='NULLABLE'
        )
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

        assert schema_mapper.is_table_definition_in_match_with_bigquery(
            schema,
            table
        )

    def test_is_table_definition_in_match_with_bigquery_throw_user_exception(
            self
    ):
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
            schema_mapper.is_table_definition_in_match_with_bigquery(
                invalid_schema,
                table
            )
            pytest.fail('Must raise exception.')
        except exceptions.UserException as err:
            assert str(err) == "Column order mismatch. " \
                               "Actual configuration: col2, col1. " \
                               "Expected BigQuery: col1, col2."

    def test_is_csv_file_in_match_with_definition(self):
        csv_columns = ['col1', 'col2']
        columns_configuration = [
            {'dbName': 'col1', "name": "col1", 'type': 'INTEGER'},
            {'dbName': 'col2', "name": "col2", 'type': 'STRING'}
        ]

        assert schema_mapper.is_csv_in_match_with_table_definition(
            columns_configuration,
            csv_columns
        )

    def test_is_csv_file_in_match_with_definition_throws_user_exception(self):
        csv_columns = ['col2', 'col1']
        columns_configuration = [
            {'dbName': 'col1', "name": "col1", 'type': 'INTEGER'},
            {'dbName': 'col2', "name": "col2", 'type': 'STRING'}
        ]

        try:
            schema_mapper.is_csv_in_match_with_table_definition(
                columns_configuration,
                csv_columns
            )
            pytest.fail("Must raise exception")
        except exceptions.UserException as err:
            assert str(err) == "Column order mismatch. " \
                               "Actual configuration: col1, col2. " \
                               "Expected csv: col2, col1."


def _write_manifest(data_dir, manifest: dict) -> str:
    tables_dir = data_dir / 'in' / 'tables'
    tables_dir.mkdir(parents=True)
    (data_dir / 'config.json').write_text('{}')
    (tables_dir / 'table.csv.manifest').write_text(json.dumps(manifest))
    return str(tables_dir / 'table.csv')


class TestTableDescriptions(object):
    table_definition = {
        'items': [
            {'name': 'col1', 'dbName': 'col1', 'type': 'STRING'},
            {'name': 'col2', 'dbName': 'renamed_col2', 'type': 'INTEGER'},
        ]
    }

    def test_manifest_without_descriptions(self, tmp_path):
        file_path = _write_manifest(tmp_path, {'columns': ['col1', 'col2']})

        assert schema_mapper.get_table_descriptions(file_path, self.table_definition) == (None, {})

    def test_empty_descriptions_are_ignored(self, tmp_path):
        file_path = _write_manifest(tmp_path, {
            'columns': ['col1', 'col2'],
            'description': '',
            'schema': [
                {'name': 'col1', 'description': ''},
                {'name': 'col2'},
            ],
        })

        assert schema_mapper.get_table_descriptions(file_path, self.table_definition) == (None, {})

    def test_columns_are_keyed_by_db_name(self, tmp_path):
        file_path = _write_manifest(tmp_path, {
            'columns': ['col1', 'col2'],
            'description': 'Table description',
            'schema': [
                {'name': 'col1', 'description': 'First column', 'data_type': {}},
                {'name': 'col2', 'description': 'Second column', 'nullable': True},
            ],
        })

        assert schema_mapper.get_table_descriptions(file_path, self.table_definition) == (
            'Table description',
            {'col1': 'First column', 'renamed_col2': 'Second column'},
        )

    def test_schema_column_not_in_items_is_skipped(self, tmp_path):
        file_path = _write_manifest(tmp_path, {
            'columns': ['col1', 'col2', 'col3'],
            'schema': [
                {'name': 'col1', 'description': 'First column'},
                {'name': 'col3', 'description': 'Not mapped'},
            ],
        })

        assert schema_mapper.get_table_descriptions(file_path, self.table_definition) == (
            None,
            {'col1': 'First column'},
        )

    def test_table_description_only(self, tmp_path):
        file_path = _write_manifest(tmp_path, {
            'columns': ['col1', 'col2'],
            'description': 'Table description',
            'schema': [{'name': 'col1'}, {'name': 'col2'}],
        })

        assert schema_mapper.get_table_descriptions(file_path, self.table_definition) == ('Table description', {})

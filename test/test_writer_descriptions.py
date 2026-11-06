import logging
from unittest.mock import MagicMock
from google.api_core.exceptions import Forbidden
from google.cloud import bigquery
from google_bigquery_writer import writer


def _get_writer(table: bigquery.Table) -> writer.Writer:
    client = MagicMock()
    client.project = 'project'
    client.get_table.return_value = table
    return writer.Writer(client)


def _get_table() -> bigquery.Table:
    table_reference = bigquery.TableReference(bigquery.DatasetReference('project', 'dataset'), 'table')
    return bigquery.Table(table_reference, [
        bigquery.SchemaField('col1', 'STRING', description='Existing col1'),
        bigquery.SchemaField('col2', 'INTEGER', description='Existing col2'),
        bigquery.SchemaField('col3', 'STRING'),
    ])


class TestApplyDescriptions(object):

    def test_sets_table_and_mapped_field_descriptions(self):
        my_writer = _get_writer(_get_table())

        my_writer.apply_descriptions('dataset', {'dbName': 'table'}, 'Table description', {'col1': 'New col1'})

        client = my_writer.bigquery_client
        table_reference = client.get_table.call_args.args[0]
        assert (table_reference.dataset_id, table_reference.table_id) == ('dataset', 'table')
        updated_table, fields = client.update_table.call_args.args
        assert fields == ['schema', 'description']
        assert updated_table.description == 'Table description'
        assert [(field.name, field.field_type, field.description) for field in updated_table.schema] == [
            ('col1', 'STRING', 'New col1'),
            ('col2', 'INTEGER', 'Existing col2'),
            ('col3', 'STRING', None),
        ]

    def test_field_descriptions_only_keep_table_description(self):
        table = _get_table()
        table.description = 'Existing table description'
        my_writer = _get_writer(table)

        my_writer.apply_descriptions('dataset', {'dbName': 'table'}, None, {'col3': 'New col3'})

        updated_table, fields = my_writer.bigquery_client.update_table.call_args.args
        assert fields == ['schema']
        assert updated_table.description == 'Existing table description'
        assert [field.description for field in updated_table.schema] == ['Existing col1', 'Existing col2', 'New col3']

    def test_api_error_is_logged_as_warning(self, caplog):
        my_writer = _get_writer(_get_table())
        my_writer.bigquery_client.update_table.side_effect = Forbidden('Access denied')

        with caplog.at_level(logging.WARNING):
            my_writer.apply_descriptions('dataset', {'dbName': 'table'}, 'Table description', {})

        assert 'Cannot set descriptions on table dataset.table' in caplog.text
        assert 'Access denied' in caplog.text

    def test_nothing_to_apply_skips_api_calls(self):
        my_writer = _get_writer(_get_table())

        my_writer.apply_descriptions('dataset', {'dbName': 'table'}, None, {})

        my_writer.bigquery_client.get_table.assert_not_called()
        my_writer.bigquery_client.update_table.assert_not_called()

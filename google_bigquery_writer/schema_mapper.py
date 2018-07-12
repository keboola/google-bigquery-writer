from google.cloud import bigquery
from google_bigquery_writer.exceptions import UserException
import json

def get_schema_field(item_definition):
    schema_field = bigquery.schema.SchemaField(
        item_definition['dbName'],
        item_definition['type']
    )
    return schema_field


def get_schema(table_definition):
    return list(map(get_schema_field, table_definition['items']))


def get_csv_schema(manifest_file_path):
    manifest_file = open(manifest_file_path)
    return json.load(manifest_file)['columns']


def is_csv_in_match_with_table_definition(table_definition, csv_header_schema):
    actual_columns = []
    expected_columns = []
    fail = False
    i = 0

    for csv_column in csv_header_schema:
        actual_columns.append(table_definition['items'][i]['name'])
        expected_columns.append(csv_column)
        if csv_column != table_definition['items'][i]['name']:
            fail = True
        i += 1

    if fail:
        raise UserException(
            'Unexpected column order in configuration. Actual: ' + ', '.join(actual_columns) + ', expected: ' + ', '.join(expected_columns) + '.')
    return True


def is_table_definition_in_match_with_bigquery(table_schema, table):
    table.reload()
    actual_columns = []
    expected_columns = []
    fail = False
    i = 0
    for column in table.schema:
        actual_columns.append(table_schema[i].name)
        expected_columns.append(column.name)
        if column.name != table_schema[i].name:
            fail = True
        i += 1
    if fail:
        raise UserException('Unexpected column order in configuration. Actual: ' + ', '.join(actual_columns) + ', expected: ' + ', '.join(expected_columns) + '.')
    return True

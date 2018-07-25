from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from google_bigquery_writer.exceptions import UserException
from keboola import docker
from os import path


def get_schema_field(item_definition: dict) -> SchemaField:
    schema_field = SchemaField(
        item_definition['dbName'],
        item_definition['type']
    )
    return schema_field


def get_schema(table_definition: dict) -> list:
    return list(map(get_schema_field, table_definition['items']))


def get_csv_schema(file_path: str) -> list:
    data_dir = path.realpath(path.join(
        path.dirname(file_path),
        '../..'
    )) + path.sep
    return docker.Config(data_dir).get_file_manifest(file_path)['columns']


def get_input_table_mapping(input_tables_mapping: list, table_id: str) -> dict:
    input_tables = list(filter(
        lambda input_table: input_table['source'] == table_id,
        input_tables_mapping
    ))

    # check for missing data tables
    if len(input_tables) == 0:
        message = 'Missing input mapping for table %s.' % table_id
        raise UserException(message)
    return input_tables[0]


def is_csv_in_match_with_table_definition(
        columns_configuration: list,
        csv_header_schema: list
) -> bool:
    expected_schema = ColumnsSchema('csv', csv_header_schema)
    actual_schema = ColumnsSchema(
        'configuration',
        list(map(lambda col: col['name'], columns_configuration))
    )
    return compare_schema(expected_schema, actual_schema)


def is_table_definition_in_match_with_bigquery(
        table_schema: list,
        table: bigquery.Table
) -> bool:
    expected_schema = ColumnsSchema(
        'BigQuery',
        list(map(lambda col: col.name, table.schema))
    )
    actual_schema = ColumnsSchema(
        'configuration',
        list(map(lambda col: col.name, table_schema))
    )

    return compare_schema(expected_schema, actual_schema)


class ColumnsSchema:
    def __init__(self, source: str, columns: list):
        self.source = source
        self.columns = columns


def compare_schema(
        expected_schema: ColumnsSchema,
        actual_schema: ColumnsSchema
) -> bool:
    actual_columns = actual_schema.columns.copy()
    for expected_column in expected_schema.columns:
        if expected_column != actual_columns.pop(0):
            raise UserException(
                'Column order mismatch. Actual %s: %s. Expected %s: %s.' %
                (
                    actual_schema.source,
                    ', '.join(actual_schema.columns),
                    expected_schema.source,
                    ', '.join(expected_schema.columns)
                )
            )
        return True

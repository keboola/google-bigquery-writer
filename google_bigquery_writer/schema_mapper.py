from google.cloud import bigquery
from google_bigquery_writer.exceptions import UserException
import csv


def get_schema_field(item_definition):
    schema_field = bigquery.schema.SchemaField(
        item_definition['dbName'],
        item_definition['type']
    )
    return schema_field


def get_schema(table_definition):
    if 'items' not in table_definition:
        message = 'Key \'items\' not defined in table definition'
        raise UserException(message)

    return list(map(get_schema_field, table_definition['items']))


def get_schema_sorted_properly(table_definition, csv_header_schema):
    items = []
    for column in csv_header_schema:
        for column_definition in table_definition['items']:
            if column == column_definition['name']:
                items.append(column_definition)
    table_definition['items'] = items
    return get_schema(table_definition)


def get_csv_schema_header(csvfile):
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    return next(csv_reader)

from google.cloud import bigquery
from google_bigquery_writer.exceptions import UserException


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

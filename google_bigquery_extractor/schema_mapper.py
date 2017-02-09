from google.cloud import bigquery

def get_schema_field(item_definition):
    schema_field = bigquery.schema.SchemaField(
        item_definition['dbName'],
        item_definition['type']
    )
    return schema_field

def get_schema(table_definition):
    return list(map(get_schema_field, table_definition['items']))
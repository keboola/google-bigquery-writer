import os


def get_table_configuration() -> dict:
    return {
        "dbName": os.environ.get('BIGQUERY_TABLE'),
        "items": [
            {
                "name": "col1",
                "dbName": "col1",
                "type": "STRING"
            },
            {
                "name": "col2",
                "dbName": "col2",
                "type": "INTEGER"
            }
        ]
    }


def get_table_configuration_with_invalid_column_order() -> dict:
    return {
        "dbName": os.environ.get('BIGQUERY_TABLE'),
        "items": [
            {
                "name": "col2",
                "dbName": "col2",
                "type": "INTEGER"
            },
            {
                "name": "col1",
                "dbName": "col1",
                "type": "STRING"
            }
        ]
    }


def get_table_configuration_with_invalid_datatype() -> dict:
    return {
        "dbName": os.environ.get('BIGQUERY_TABLE'),
        "items": [
            {
                "name": "col1",
                "dbName": "col1",
                "type": "INVALID-DATATYPE"
            },
            {
                "name": "col2",
                "dbName": "col2",
                "type": "INTEGER"
            }
        ]
    }


def get_table_configuration_with_invalid_table_name() -> dict:
    return {
        "dbName": 'invalid-table-name',
        "items": [
            {
                "name": "col1",
                "dbName": "col1",
                "type": "INVALID-DATATYPE"
            },
            {
                "name": "col2",
                "dbName": "col2",
                "type": "INTEGER"
            }
        ]
    }


def get_table_configuration_with_invalid_data_type() -> dict:
    return {
        "dbName": os.environ.get('BIGQUERY_TABLE'),
        "items": [
            {
                "name": "col1",
                "dbName": "col1",
                "type": "INTEGER"
            },
            {
                "name": "col2",
                "dbName": "col2",
                "type": "STRING"
            }
        ]
    }


def get_table_configuration_with_extra_column() -> dict:
    return {
        "dbName": os.environ.get('BIGQUERY_TABLE'),
        "items": [
            {
                "name": "col1",
                "dbName": "col1",
                "type": "STRING"
            },
            {
                "name": "col2",
                "dbName": "col2",
                "type": "INTEGER"
            },
            {
                "name": "col3",
                "dbName": "col3",
                "type": "STRING"
            }
        ]
    }

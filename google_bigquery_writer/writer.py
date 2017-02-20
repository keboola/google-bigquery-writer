from google.cloud import bigquery
from google.auth import exceptions
from google_bigquery_writer.exceptions import UserException


class Writer(object):
    def __init__(self, project=None, credentials=None):
        try:
            self.client = bigquery.Client(project, credentials)
        except exceptions.DefaultCredentialsError as err:
            raise UserException('Cannot connect to BigQuery')

    def write_table(self, csv_file, dataset_name, table_name, columns_schema):
        dataset = self.client.dataset(dataset_name)
        try:
            if not dataset.exists():
                dataset.create()
        except exceptions.RefreshError as err:
            message = 'Cannot connect to BigQuery.' \
                ' Check your access token or refresh token.'
            raise UserException(message)
        table = dataset.table(table_name, columns_schema)
        if not table.exists():
            table.create()

        with open(csv_file.name, 'rb') as readable:
            table.upload_from_file(
                readable,
                source_format='CSV',
                skip_leading_rows=1
            )

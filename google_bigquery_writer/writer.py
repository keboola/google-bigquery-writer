from google.cloud import bigquery
from google.auth import exceptions
from google_bigquery_writer.exceptions import UserException
import time

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
            job = table.upload_from_file(
                readable,
                source_format='CSV',
                skip_leading_rows=1
            )
            return job

    def write_table_sync(self, csv_file, dataset_name, table_name,
                         columns_schema, polling_max_retries=10):
        job = self.write_table(
            csv_file,
            dataset_name,
            table_name,
            columns_schema
        )
        retry_count = 0
        sleep_runsum = 0
        while retry_count < polling_max_retries and job.state != u'DONE':
            time.sleep(1.5**retry_count)
            sleep_runsum += 1.5**retry_count
            retry_count += 1
            job.reload()
        if job.state != u'DONE':
            message = 'Loading data into table %s.%s didn\'t finish in %s ' \
                'seconds' % (dataset_name, table_name, round(sleep_runsum))
            raise UserException(message)

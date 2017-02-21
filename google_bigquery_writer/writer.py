from google.cloud import bigquery
from google.auth import exceptions
from google_bigquery_writer.exceptions import UserException
import time
import google.cloud.exceptions

class Writer(object):
    def __init__(self, project=None, credentials=None):
        try:
            self.client = bigquery.Client(project, credentials)
        except exceptions.DefaultCredentialsError as err:
            raise UserException('Cannot connect to BigQuery')

    def write_table(self, csv_file, dataset_name, table_name, columns_schema,
                    incremental=False
                    ):
        dataset = self.client.dataset(dataset_name)
        try:
            if not dataset.exists():
                dataset.create()
        except exceptions.RefreshError as err:
            message = 'Cannot connect to BigQuery.' \
                ' Check your access token or refresh token.'
            raise UserException(message)
        except google.cloud.exceptions.BadRequest as err:
            message = 'Cannot create dataset %s: %s' % (
                dataset_name,
                str(err)
            )
            raise UserException(message)
        table = dataset.table(table_name, columns_schema)
        try:
            if not incremental and table.exists():
                table.delete()
            if not table.exists():
                table.create()
        except google.cloud.exceptions.BadRequest as err:
            message = 'Cannot create table %s: %s' % (
                table_name,
                str(err)
            )
            raise UserException(message)
        with open(csv_file.name, 'rb') as readable:
            job = table.upload_from_file(
                readable,
                source_format='CSV',
                skip_leading_rows=1
            )
            return job

    def write_table_sync(self, csv_file, dataset_name, table_name,
                         columns_schema, incremental=False, polling_max_retries=10):
        job = self.write_table(
            csv_file,
            dataset_name,
            table_name,
            columns_schema,
            incremental=incremental
        )
        retry_count = 0
        sleep_runsum = 0
        while retry_count < polling_max_retries and job.state != u'DONE':
            sleep_time = 1.5**retry_count
            time.sleep(sleep_time)
            sleep_runsum += sleep_time
            retry_count += 1
            job.reload()
        if job.state != u'DONE':
            message = 'Loading data into table %s.%s didn\'t finish in %s ' \
                'seconds' % (dataset_name, table_name, round(sleep_runsum))
            raise UserException(message)
        if job.errors:
            first_error = job.errors.pop()
            message = 'Loading data into table %s.%s failed: %s' % (
                dataset_name,
                table_name,
                first_error['message']
            )
            raise UserException(message)

from google.cloud import bigquery
from google.auth import exceptions
from google_bigquery_writer.exceptions import UserException
import time
import google.cloud.exceptions


class Writer(object):
    def __init__(self, project=None, credentials=None):
        self.client = bigquery.Client(project, credentials)

    def write_table(self, csv_file, dataset_name, table_name, columns_schema,
                    incremental=False
                    ):
        if dataset_name == '' or dataset_name is None:
            raise UserException('Dataset name not specified.')
        if table_name == '' or table_name is None:
            raise UserException('Table name not specified.')
        if columns_schema is None or len(columns_schema) == 0:
            raise UserException('Columns schema not specified.')
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
        except google.cloud.exceptions.NotFound as err:
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
                skip_leading_rows=1,
                allow_quoted_newlines=True
            )
            return job

    def write_table_sync(self, csv_file, dataset_name, table_name,
                         columns_schema, incremental=False,
                         polling_max_retries=360,
                         polling_delay=5):
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
            time.sleep(polling_delay)
            retry_count += 1
            job.reload()
        if job.state != u'DONE':
            message = 'Loading data into table %s.%s didn\'t finish in %s ' \
                'seconds' % (
                    dataset_name,
                    table_name,
                    polling_delay*polling_max_retries
                )
            raise UserException(message)
        if job.errors:
            first_error = job.errors.pop()
            message = 'Loading data into table %s.%s failed: %s' % (
                dataset_name,
                table_name,
                first_error['message']
            )
            raise UserException(message)

from google.cloud import bigquery, exceptions as BQExceptions
from google.auth import exceptions
from google_bigquery_writer.exceptions import UserException
from google_bigquery_writer import schema_mapper
import time


class Writer(object):
    def __init__(self, project=None, credentials=None):
        self.client = bigquery.Client(project, credentials)

    def write_table(self, csv_file, dataset_name, table_definition, columns_schema,
                    incremental=False
                    ):
        if dataset_name == '' or dataset_name is None:
            raise UserException('Dataset name not specified.')
        if table_definition['dbName'] == '' or table_definition['dbName'] is None:
            raise UserException('Table name not specified.')
        if columns_schema is None or len(columns_schema) == 0:
            raise UserException('Columns schema not specified.')

        # TODO list projects and validate, that the project exists

        dataset_reference = self.client.dataset(dataset_name)

        try:
            dataset = self.client.get_dataset(dataset_reference)
        except BQExceptions.NotFound:
            try:
                dataset_obj = bigquery.Dataset(dataset_reference)
                dataset = self.client.create_dataset(dataset_obj)
            except BQExceptions.NotFound:
                message = 'Cannot create dataset %s. Project %s was not found.' % (
                    dataset_name,
                    self.client.project
                )
                raise UserException(message)
        except exceptions.RefreshError as err:
            message = 'Cannot connect to BigQuery.' \
                ' Check your access token or refresh token.'
            raise UserException(message)
        except BQExceptions.BadRequest as err:
            message = 'Cannot create dataset %s: %s' % (
                dataset_name,
                str(err)
            )
            raise UserException(message)

        table_ref = dataset.table(table_definition['dbName'])
        table = bigquery.Table(table_ref, columns_schema)

        try:
            bq_table = self.client.get_table(table_ref)
            table_exist = True
            if incremental:
                schema_mapper.is_table_definition_in_match_with_bigquery(
                    columns_schema,
                    bq_table
                )
            else:
                self.client.delete_table(table_ref)
                table_exist = False
        except BQExceptions.NotFound:
            table_exist = False
        except BQExceptions.BadRequest as err:
            message = 'Cannot create table %s: %s' % (
                table_definition['dbName'],
                str(err)
            )
            raise UserException(message)

        if not table_exist:
            try:
                self.client.create_table(table)
            except BQExceptions.BadRequest as err:
                message = 'Cannot create table %s: %s' % (
                    table_definition['dbName'],
                    str(err)
                )
                raise UserException(message)

        with open(csv_file.name, 'rb') as readable:
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = 'CSV'
            job_config.skip_leading_rows = 1
            job_config.allow_quoted_newlines=True

            job = self.client.load_table_from_file(
                readable,
                table_ref,
                job_config=job_config
            )

            return job

    def write_table_sync(self, csv_file, dataset_name, table_definition,
                         columns_schema, incremental=False,
                         polling_max_retries=360,
                         polling_delay=5):
        job = self.write_table(
            csv_file,
            dataset_name,
            table_definition,
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
                    table_definition['dbName'],
                    polling_delay*polling_max_retries
                )
            raise UserException(message)
        if job.errors:
            first_error = job.errors.pop()
            message = 'Loading data into table %s.%s failed: %s' % (
                dataset_name,
                table_definition['dbName'],
                first_error['message']
            )
            raise UserException(message)

from google.cloud import bigquery, exceptions as bq_exceptions
from google_bigquery_writer.exceptions import UserException
from google_bigquery_writer import schema_mapper
from google.api_core.exceptions import BadRequest

import time


class Writer(object):
    def __init__(self, bigquery_client: bigquery.Client):
        self.bigquery_client = bigquery_client

    def obtain_dataset(self, dataset_name: str) -> bigquery.Dataset:
        dataset_reference = self.bigquery_client.dataset(dataset_name)

        try:
            return self.bigquery_client.get_dataset(dataset_reference)
        except bq_exceptions.NotFound:
            dataset_obj = bigquery.Dataset(dataset_reference)
            try:
                return self.bigquery_client.create_dataset(dataset_obj)
            except BadRequest as err:
                raise UserException(err.message)
        except bq_exceptions.BadRequest as err:
            message = 'Cannot create dataset %s: %s' % (
                dataset_name,
                str(err)
            )
            raise UserException(message)

    def verify_project(self) -> None:
        projects = self.bigquery_client.list_projects()
        project_list = list(map(
            lambda project: project.project_id,
            projects
        ))

        if self.bigquery_client.project not in project_list:
            message = 'Project %s was not found.' % (
                self.bigquery_client.project
            )
            raise UserException(message)

    def prepare_table(
            self,
            dataset: bigquery.Dataset,
            table_name: str,
            columns_schema: list,
            incremental: bool
    ) -> bigquery.TableReference:
        table_reference = dataset.table(table_name)
        table = bigquery.Table(table_reference, columns_schema)

        try:
            bq_table = self.bigquery_client.get_table(table_reference)
            table_exist = True
            if incremental:
                schema_mapper.is_table_definition_in_match_with_bigquery(
                    columns_schema,
                    bq_table
                )
            else:
                self.bigquery_client.delete_table(table_reference)
                table_exist = False
        except bq_exceptions.NotFound:
            table_exist = False
        except bq_exceptions.BadRequest as err:
            message = 'Cannot create table %s: %s' % (
                table_reference,
                str(err)
            )
            raise UserException(message)

        if not table_exist:
            try:
                self.bigquery_client.create_table(table)
            except bq_exceptions.BadRequest as err:
                message = 'Cannot create table %s: %s' % (
                    table_name,
                    str(err)
                )
                raise UserException(message)
        return table_reference

    def write_table(
            self,
            csv_file_path: str,
            dataset_name: str,
            table_definition: dict,
            incremental: bool = False
    ) -> bigquery.LoadJob:
        if dataset_name == '' or dataset_name is None:
            raise UserException('Dataset name not specified.')
        if table_definition['dbName'] == ''\
                or table_definition['dbName'] is None:
            raise UserException('Table name not specified.')

        self.verify_project()  # Verify that defined project exists

        columns_schema = schema_mapper.get_schema(table_definition)
        if columns_schema is None or len(columns_schema) == 0:
            raise UserException('Columns schema not specified.')

        csv_schema = schema_mapper.get_csv_schema(csv_file_path)
        schema_mapper.is_csv_in_match_with_table_definition(
            table_definition['items'],
            csv_schema
        )

        dataset = self.obtain_dataset(dataset_name)
        table_reference = self.prepare_table(
            dataset,
            table_definition['dbName'],
            columns_schema,
            incremental
        )

        with open(csv_file_path, 'rb') as readable:
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = 'CSV'
            job_config.skip_leading_rows = 1
            job_config.allow_quoted_newlines = True

            try:
                job = self.bigquery_client.load_table_from_file(
                    readable,
                    table_reference,
                    job_config=job_config
                )
            except bq_exceptions.BadRequest as err:
                raise UserException(str(err))
            return job

    def write_table_sync(
            self,
            csv_file_path: str,
            dataset_name: str,
            table_definition: dict,
            incremental: bool = False,
            polling_max_retries: int = 360,
            polling_delay: int = 5
    ) -> None:
        job = self.write_table(
            csv_file_path,
            dataset_name,
            table_definition,
            incremental=incremental
        )
        retry_count = 0
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

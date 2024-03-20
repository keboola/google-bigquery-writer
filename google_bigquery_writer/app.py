from keboola import docker
from google_bigquery_writer.exceptions import UserException
import google_bigquery_writer.writer
import google.api_core
from google.cloud import bigquery
from google.auth.exceptions import RefreshError
import json
import os
from google_bigquery_writer import schema_mapper
from google_bigquery_writer.bigquery_client_factory \
    import BigqueryClientFactory
from google.oauth2 import service_account


class App:
    def __init__(self):
        self.data_dir = os.environ.get('KBC_DATADIR')
        self.cfg = docker.Config(self.data_dir)
        self.writer = None

    def validate_credentials(self):
        parameters = (self.cfg.config_data.get('image_parameters', {}).get('service_account')
                      or self.cfg.get_parameters().get('service_account'))

        if not parameters:
            raise UserException('Authorization missing.')

        private_key = parameters.get('#private_key')
        if private_key == '' or private_key is None:
            raise UserException('Service account private key missing.')

        client_email = parameters.get('client_email')
        if client_email == '' or client_email is None:
            raise UserException('Service account client email missing.')

        token_uri = parameters.get('token_uri')
        if token_uri == '' or token_uri is None:
            raise UserException('Service account token URI missing.')

        project_id = parameters.get('project_id')
        if project_id == '' or project_id is None:
            raise UserException('Service account project id missing.')

    def get_credentials(self):
        credentials_json = (self.cfg.config_data.get('image_parameters', {}).get('service_account')
                            or self.cfg.get_parameters().get('service_account'))

        private_key = credentials_json.get('#private_key')
        client_email = credentials_json.get('client_email')
        token_uri = credentials_json.get('token_uri')

        service_account_info = {
            'private_key': private_key,
            'client_email': client_email,
            'token_uri': token_uri
        }

        scopes = [
            'https://www.googleapis.com/auth/bigquery'
        ]
        try:
            return service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=scopes
            )
        except ValueError as err:
            message = 'Cannot get credentials from service account %s. Reason "%s".' % (
                client_email,
                str(err)
            )
            raise UserException(message)

    def get_writer(self):
        """
        Late loading method
        """
        if self.writer:
            return self.writer

        config_data_service_account = self.cfg.config_data.get('image_parameters', {}).get('service_account', {})
        parameters_service_account = self.cfg.get_parameters().get('service_account')

        project = (
                self.cfg.get_parameters().get('project', None) or
                config_data_service_account.get('project_id', None) or
                parameters_service_account.get('project_id'))

        bigquery_client_factory = BigqueryClientFactory(
            project,
            self.get_credentials(),
            location=self.cfg.get_parameters().get('location', None)
        )

        bigquery_client = bigquery_client_factory.create()
        self.writer = google_bigquery_writer.writer.Writer(bigquery_client)
        return self.writer

    def run(self):
        action = self.cfg.get_action()

        parameters = self.cfg.get_parameters()
        if len(parameters) == 0:
            message = 'Configuration is empty.'
            raise UserException(message)

        self.validate_credentials()

        if parameters.get('dataset') is None or parameters.get('dataset') == '':
            message = 'Google BigQuery dataset not specified in the configuration.'
            raise UserException(message)

        if (
                not self.cfg.get_parameters().get('project') and
                (
                        not self.cfg.get_parameters().get('service_account') or
                        not self.cfg.get_parameters().get('service_account').get('project_id')
                ) and
                not self.cfg.config_data.get('image_parameters', {}).get('service_account', {}).get('project_id')
        ):
            message = 'Google BigQuery project not specified in the configuration.'
            raise UserException(message)

        if action == 'run' or action is None or action == '':
            self.action_run()
            return
        if action == 'list':
            self.action_list()
            return
        raise UserException('Action %s not defined' % action)

    def action_run(self):
        # validate application parameters
        parameters = self.cfg.get_parameters()
        # check for empty tables
        if isinstance(parameters, list):
            raise UserException('There are no tables specified in the configuration.')
        tables = parameters.get('tables')
        if not tables:
            raise UserException('There are no tables specified in the configuration.')

        for table in tables:
            # skip tables with export: false
            if 'export' in table.keys() and table['export'] is False:
                continue
            if 'items' not in table.keys():
                message = 'Key \'items\' not defined in ' \
                          '\'%s\' table definition.'\
                          % table['tableId']
                raise UserException(message)

            input_table_mapping = schema_mapper.get_input_table_mapping(
                self.cfg.get_input_tables(),
                table['tableId']
            )
            incremental = \
                'incremental' in table.keys()\
                and table['incremental'] is True
            csv_file_path = '%s/in/tables/%s' % (
                self.data_dir,
                input_table_mapping['destination']
            )

            print('Loading table %s into BigQuery as %s.%s' % (
                input_table_mapping['source'],
                parameters.get('dataset'),
                table['dbName']
            ))

            self._process_upload(csv_file_path, parameters, table, incremental)

        print('BigQuery Writer finished')

    def _process_upload(self, csv_file_path: str, parameters: dict, table: dict, incremental: bool):
        try:
            self.get_writer().write_table_sync(
                csv_file_path,
                parameters.get('dataset'),
                table,
                incremental=incremental
            )
        except RefreshError:
            message = 'Cannot connect to BigQuery.' \
                      ' Please try reauthorizing.'
            raise UserException(message)
        except google.api_core.exceptions.Forbidden as err:
            raise UserException(err.message)

    def action_list(self):
        client = bigquery.client.Client(
            credentials=self.get_credentials(),
            project='dummy'
        )
        response = {
            'projects': []
        }
        try:
            projects = list(client.list_projects())
            for project in projects:
                client = bigquery.client.Client(
                    credentials=self.get_credentials(),
                    project=project.project_id,
                )
                datasets = list(client.list_datasets())
                response['projects'].append({
                    'id': project.project_id,
                    'name': project.friendly_name,
                    'datasets': list(map(
                        lambda dataset: {
                            'id': dataset.dataset_id,
                            'name': dataset.dataset_id
                        },
                        datasets
                    ))
                })

        except RefreshError:
            message = 'Cannot connect to BigQuery.' \
                      ' Please try reauthorizing.'
            raise UserException(message)
        except google.api_core.exceptions.Forbidden as err:
            raise UserException(err.message)

        print(json.dumps(response))

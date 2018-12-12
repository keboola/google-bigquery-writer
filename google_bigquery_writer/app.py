from keboola import docker
from google_bigquery_writer.exceptions import UserException
import google_bigquery_writer.writer
import google.oauth2.credentials
from google.cloud import bigquery
from google.auth.exceptions import RefreshError
import json
import os
from google_bigquery_writer import schema_mapper
from google_bigquery_writer.bigquery_client_factory \
    import BigqueryClientFactory


class App:
    def __init__(self):
        self.data_dir = os.environ.get('KBC_DATADIR')
        self.cfg = docker.Config(self.data_dir)
        self.writer = None

    def get_credentials(self):
        oauthapi_data = self.cfg.get_oauthapi_data()
        if oauthapi_data == {}:
            raise UserException('Authorization missing.')
        return google.oauth2.credentials.Credentials(
            oauthapi_data.get('access_token'),
            token_uri='https://accounts.google.com/o/oauth2/token',
            client_id=self.cfg.get_oauthapi_appkey(),
            client_secret=self.cfg.get_oauthapi_appsecret(),
            refresh_token=oauthapi_data.get('refresh_token')
        )

    def get_writer(self):
        """
        Late loading method
        """
        if self.writer:
            return self.writer

        bigquery_client_factory = BigqueryClientFactory(
            self.cfg.get_parameters().get('project'),
            self.get_credentials()
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
        if parameters.get('dataset') is None or parameters.get('dataset') == '':
            message = 'Google BigQuery dataset not specified in the configuration.'
            raise UserException(message)
        if parameters.get('project') is None or parameters.get('project') == '':
            message = 'Google BigQuery project not specified in the configuration.'
            raise UserException(message)

        if action == 'run' or action is None or action == '':
            self.action_run()
            return
        if action == 'listProjects':
            self.action_list_projects()
            return
        if action == 'listDatasets':
            self.action_list_datasets()
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
            message = 'There are no tables specified in the configuration.'
            raise UserException(message)
        tables = parameters.get('tables')
        if tables is None:
            message = 'There are no tables specified in the configuration.'
            raise UserException(message)

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
        print('BigQuery Writer finished')

    def action_list(self):
        client = bigquery.client.Client(
            credentials=self.get_credentials(),
            project='dummy'
        )
        response = {
            'projects': []
        }
        projects = list(client.list_projects())
        for project in projects:
            client = bigquery.client.Client(
                credentials=self.get_credentials(),
                project=project.project_id
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

        print(json.dumps(response))

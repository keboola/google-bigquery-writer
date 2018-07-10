# coding=utf-8
from keboola import docker
from google_bigquery_writer.exceptions import UserException
import google_bigquery_writer.writer
import google.oauth2.credentials
import json
from google_bigquery_writer import schema_mapper


class App:
    def __init__(self, data_dir=None):
        self.data_dir = data_dir
        self.cfg = docker.Config(self.data_dir)
        self.writer = None

    def get_credentials(self):
        oauthapi_data = self.cfg.get_oauthapi_data()
        credentials = google.oauth2.credentials.Credentials(
            oauthapi_data.get('access_token'),
            token_uri='https://accounts.google.com/o/oauth2/token',
            client_id=self.cfg.get_oauthapi_appkey(),
            client_secret=self.cfg.get_oauthapi_appsecret(),
            refresh_token=oauthapi_data.get('refresh_token')
        )
        return credentials

    def get_writer(self):
        """
        Late loading method
        """
        if self.writer:
            return self.writer
        parameters = self.cfg.get_parameters()
        my_writer = google_bigquery_writer.writer.Writer(
            project=parameters.get('project'),
            credentials=self.get_credentials()
        )
        self.writer = my_writer
        return self.writer

    def run(self):
        parameters = self.cfg.get_parameters()
        action = self.cfg.get_action()
        if action == 'run' or action is None or action == '':
            self.action_run()
            return
        if action == 'listProjects':
            self.action_list_projects()
            return
        if action == 'listDatasets':
            self.action_list_datasets()
            return            
        raise UserException('Action %s not defined' % (action))

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

        input_tables = self.cfg.get_input_tables()
        for table in tables:
            # skip tables with export: false
            if 'export' in table.keys() and table['export'] is False:
                continue
            matching_inputs = list(filter(
                lambda input: input['source'] == table['tableId'], input_tables
            ))
            # check for missing data tables
            if len(matching_inputs) == 0:
                message = 'Missing input mapping for table %s.' % (
                    table['tableId']
                )
                raise UserException(message)

            input_mapping = matching_inputs[0]

            incremental = False
            if 'incremental' in table.keys():
                incremental = table['incremental']

            file_path = self.data_dir + '/in/tables/' + input_mapping['destination']
            csv_file = open(file_path)

            csv_header_schema = schema_mapper.get_csv_schema_header(csv_file)
            schema = schema_mapper.get_schema_sorted_properly(table, csv_header_schema)

            print('Loading table %s into BigQuery as %s.%s' % (
                input_mapping['source'],
                parameters.get('dataset'),
                table['dbName']
            ))
            self.get_writer().write_table_sync(
                csv_file,
                parameters.get('dataset'),
                table,
                schema,
                incremental=incremental
            )
        print('BigQuery Writer finished')

    def action_list_projects(self):
        client = google.cloud.bigquery.client.Client(
            credentials=self.get_credentials(),
            project='dummy'
        )
        projects = list(client.list_projects())
        print(json.dumps(
            list(map(
                lambda project: {
                    'id': project.project_id,
                    'name': project.friendly_name
                }, projects))
            )
        )

    def action_list_datasets(self):
        parameters = self.cfg.get_parameters()
        client = google.cloud.bigquery.client.Client(
            credentials=self.get_credentials(),
            project=parameters.get('project')
        )
        datasets = list(client.list_datasets())
        print(json.dumps(
            list(map(
                lambda dataset: {
                    'id': dataset.name,
                    'name': dataset.name
                }, datasets))
            )
        )
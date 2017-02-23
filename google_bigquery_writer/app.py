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

    def get_writer(self):
        """
        Late loading method
        """
        if self.writer:
            return self.writer
        parameters = self.cfg.get_parameters()
        oauthapi_data = self.cfg.get_oauthapi_data()
        credentials = google.oauth2.credentials.Credentials(
            oauthapi_data.get('access_token'),
            token_uri='https://accounts.google.com/o/oauth2/token',
            client_id=self.cfg.get_oauthapi_appkey(),
            client_secret=self.cfg.get_oauthapi_appsecret(),
            refresh_token=oauthapi_data.get('refresh_token')
        )
        my_writer = google_bigquery_writer.writer.Writer(
            project=parameters.get('project'),
            credentials=credentials
        )
        self.writer = my_writer
        return self.writer

    def run(self):
        # validate application parameters
        parameters = self.cfg.get_parameters()

        # check for empty tables
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

            schema = schema_mapper.get_schema(table)
            input_mapping = matching_inputs[0]

            incremental = False
            if 'incremental' in table.keys():
                incremental = table['incremental']

            file_path = self.data_dir + '/in/tables/' + input_mapping['destination']
            csv_file = open(file_path)

            self.get_writer().write_table_sync(
                csv_file,
                parameters.get('dataset'),
                table['dbName'],
                schema,
                incremental=incremental
            )


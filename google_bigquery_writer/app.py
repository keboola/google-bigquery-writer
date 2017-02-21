# coding=utf-8
from keboola import docker
from google_bigquery_writer.exceptions import UserException


class App:
    def __init__(self, data_dir=None):
        self.data_dir = data_dir

    def run(self):
        # initialize KBC configuration
        cfg = docker.Config(self.data_dir)
        # validate application parameters
        parameters = cfg.get_parameters()
        tables = parameters.get('tables')
        if tables is None:
            raise UserException('There are no tables specified in the configuration.')

        input_tables = cfg.get_input_tables()
        for table in tables:
            matching_inputs = filter(lambda input: input['source'] == table['tableId'], input_tables)
            if len(list(matching_inputs)) == 0:
                raise UserException('Missing input mapping for table %s.' % (table['tableId']))


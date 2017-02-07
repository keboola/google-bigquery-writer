# coding=utf-8
from keboola import docker

class App:
    def __init__(self, data_dir=None):
        self.data_dir = data_dir

    def run(self):
        # initialize KBC configuration
        cfg = docker.Config(self.data_dir)
        # validate application parameters
        parameters = cfg.get_parameters()
        try:
            input_mapping = parameters.get('storage').get('input').get('tables')
        except AttributeError as err:
            raise ValueError('There are no tables specified in the input mapping.')
            
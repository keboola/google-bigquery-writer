import pytest
import os
from google_bigquery_writer import app, exceptions


class TestAppErrors:

    def test_empty_config(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "empty_config/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == "There are no tables "\
                "specified in the configuration."
            pass

    def test_missing_input(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "missing_input/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == "Missing input mapping for " \
                "table in.c-main.table1."
            pass

    def test_action_not_defined(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "invalid_action/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == "Action invalid not defined"
            pass

    def test_invalid_parameters(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "invalid_parameters_type/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == \
                   "There are no tables specified in the configuration."
            pass

    def test_missing_authorization(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "missing_authorization/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == "Authorization missing."
            pass

    def test_invalid_authorization(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "invalid_authorization/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == 'Cannot connect to BigQuery.' \
                               ' Please try reauthorizing.'
            pass

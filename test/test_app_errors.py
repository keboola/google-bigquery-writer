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
            assert str(err) == "Configuration is empty."
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

    def test_missing_project(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "missing_project/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == \
                   "Google BigQuery project not specified in the configuration."
            pass

    def test_empty_project(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "empty_project/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == \
                   "Google BigQuery project not specified in the configuration."
            pass

    def test_missing_dataset(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "missing_dataset/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == \
                   "Google BigQuery dataset not specified in the configuration."
            pass

    def test_empty_dataset(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "empty_dataset/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == \
                   "Google BigQuery dataset not specified in the configuration."
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

    def test_invalid_authorization_private_key(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "invalid_authorization_private_key/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == 'Cannot get credentials from service account ' \
                               'bigquery-writer-manage@syrup-components.iam.' \
                               'gserviceaccount.com. Reason ' \
                               '"No key could be detected.".'
            pass

    def test_service_account_missing_private_key(self, data_dir):
        os.environ['KBC_DATADIR'] = data_dir + "service_account_missing_private_key/"
        application = app.App()
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == 'Service account private key missing.'
            pass

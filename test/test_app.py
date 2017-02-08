import os
import csv
import pytest
from google_bigquery_extractor import app, exceptions

class TestGoogleBigQueryWriter():
    
    def test_empty_config(self, data_dir):
        application = app.App(data_dir + "empty_config/")
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == "There are no tables specified in the configuration."
            pass

    def test_missing_input(self, data_dir):
        application = app.App(data_dir + "missing_input/")
        try:
            application.run()
            pytest.fail("Must raise exception.")
        except exceptions.UserException as err:
            assert str(err) == "Missing input mapping for table in.c-main.table1."
            pass

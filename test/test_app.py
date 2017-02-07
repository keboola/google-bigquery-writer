import os
import csv
import pytest
from google_bigquery_extractor import app

class TestGoogleBigQueryWriter():
    def test_empty_config(self, data_dir):
        application = app.App(data_dir + "empty_config/")
        try:
            application.run()
            pytest.xfail("Must raise exception.")
        except ValueError as err:
            assert str(err) == "There are no tables specified in the input mapping."
            pass

#!/bin/sh

export KBC_DATA_DIR=/home/test/data
cd /home/

flake8 ./google_bigquery_writer/ ./test/
py.test --cov=google_bigquery_writer --cov-report term-missing
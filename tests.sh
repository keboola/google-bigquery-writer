#!/bin/sh
set -e

export KBC_DATA_DIR=/home/test/data
cd /home/

flake8 ./ --exclude=venv --statistics --max-line-length 120
#py.test --cov=google_bigquery_writer --cov-report term-missing -vv
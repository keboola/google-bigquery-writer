#!/bin/sh

export KBC_DATA_DIR=/home/test/data
cd /home/
py.test --cov=google_bigquery_writer --cov-report term-missing
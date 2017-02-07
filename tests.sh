#!/bin/sh

export KBC_DATA_DIR=/home/test/data
cd /home/
py.test --cov=google-bigquery-extractor --cov-report term-missing
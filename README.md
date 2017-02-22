[![Build Status](https://travis-ci.org/keboola/google-bigquery-writer.svg?branch=master)](https://travis-ci.org/keboola/google-bigquery-writer)

# Google BigQuery writer

## Set up

```
docker-compose Build
```

## Development

Create `.env` file, that contains required env variables
```
OAUTH_ACCESS_TOKEN=
OAUTH_CLIENT_ID=
OAUTH_CLIENT_SECRET=
OAUTH_REFRESH_TOKEN=
BIGQUERY_PROJECT=my_project
BIGQUERY_DATASET=my_dataset
BIGQUERY_TABLE=my_table
```

PyTest tests are located in `/test`, to start the testsuite, run 

```
docker-compose run --rm tests 
```

To run a specific test, run

```
docker-compose run --rm tests py.test -k my_test
```
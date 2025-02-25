[![Code Climate](https://codeclimate.com/github/keboola/google-bigquery-writer/badges/gpa.svg)](https://codeclimate.com/github/keboola/google-bigquery-writer)
[![Test Coverage](https://codeclimate.com/github/keboola/google-bigquery-writer/badges/coverage.svg)](https://codeclimate.com/github/keboola/google-bigquery-writer/coverage)

# Google BigQuery writer

## Set up

```
docker-compose build
```

## Development

Create `.env` file, that contains required env variables
```
OAUTH_CLIENT_ID=
OAUTH_CLIENT_SECRET=

OAUTH_ACCESS_TOKEN=
OAUTH_REFRESH_TOKEN=

SERVICE_ACCOUNT_USER=
SERVICE_ACCOUNT_MANAGE=

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

## Actions

### list

`list` action returns a list of projects and datasets in this format

```json
{
    "projects": [
        {
            "id": "bigquery-writer-158018",
            "name": "BigQuery Writer",
            "datasets": [
                {
                    "id": "travis_test",
                    "name": "travis_test"
                }
            ]
        }
    ]
}
```

## Development Credentials

### OAuth

- Create OAuth client IDs or obtain an existing client id (`OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`)
- Use these client credentials in (https://developers.google.com/oauthplayground)[Google OAuth playground] to create new authorization, use `https://www.googleapis.com/auth/bigquery` scope 

### Service Account

This app requires 2 sets of service account credentials - one for managing the tests (fixtures, etc), one for running the tests. They have different scopes.

#### Manage

- Create a new service account (IAM > Service accounts), e.g. `BigQuery Writer Manage`
- Add `BigQuery Data Owner` and `BigQuery Job User` roles to the user
- Create JSON key
- Remove newlines from the JSON and assign it to the `SERVICE_ACCOUNT_MANAGE` variable in the `.env` file  

#### User

- Create a new service account (IAM > Service accounts), e.g. `BigQuery Writer User`
- Add `BigQuery User` and `BigQuery Job User` roles to the user
- Create JSON key
- Remove newlines from the JSON and assign it to the `SERVICE_ACCOUNT_USER` variable in the `.env` file

## Configuration Example

```json
{
  "storage": {
    "input": {
      "tables": [
        {
          "source": "in.c-bucket.table1",
          "destination": "in.c-bucket.table1.csv",
          "columns": [
            "string",
            "integer",
            "float",
            "boolean",
            "timestamp"
          ]
        }
      ]
    }
  },
  "action": "run",
  "parameters": {
    "dataset": "bigquery_writer_test",
    "service_account": {
      "#private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
      "client_email": "keboola-connection-bigquery-wr@my_bigquery_project.iam.gserviceaccount.com",
      "token_uri": "https://oauth2.googleapis.com/token",
      "project_id": "my_bigquery_project"
    },    
    "tables": [
      {
        "dbName": "table1",
        "chunkSize": 10000, 
        "tableId": "in.c-bucket.table1", 
        "partitioning": "time",
        "partitioning_granularity": "DAY",
        "partitioning_column": "timestamp",
        "partition_expiration_ms": 7776000000,
        "require_partition_filter": true,
        "clustering": true,
        "clustering_columns": ["category", "category2"],
        "items": [
          {
            "name": "string",
            "dbName": "string",
            "type": "STRING"
          },
          {
            "name": "integer",
            "dbName": "integer",
            "type": "INTEGER"
          },
          {
            "name": "float",
            "dbName": "float",
            "type": "FLOAT"
          },
          {
            "name": "boolean",
            "dbName": "boolean",
            "type": "BOOLEAN"
          },
          {
            "name": "timestamp",
            "dbName": "timestamp",
            "type": "TIMESTAMP"
          },
          {
            "name": "category",
            "dbName": "category",
            "type": "INTEGER"
          },
          {
            "name": "category2",
            "dbName": "category2",
            "type": "STRING"
          }
        ]
      }
    ]
  }
}
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.

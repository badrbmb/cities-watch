import json
import os
import time

from google.cloud import bigquery, exceptions
from google.oauth2 import service_account
from tqdm import tqdm

from cities_watch import config

# Load service account credentials for BigQuery
scopes = ["https://www.googleapis.com/auth/cloud-platform"]
credentials = service_account.Credentials.from_service_account_file(filename=os.getenv("PATH_TO_CREDS"),
                                                                    scopes=scopes)


def get_table_schema(schema=None, path_to_schema=None):
    if schema is None:
        with open(path_to_schema, 'r') as f:
            schema = json.load(f)

    table_schema = []
    for s in schema:
        if s['mode'] != 'REPEATED':
            table_schema.append(bigquery.SchemaField(**s))
        else:
            fields = s.pop('fields')
            table_schema.append(bigquery.SchemaField(**s, fields=get_table_schema(schema=fields)))

    return table_schema


def load_table(table_id, client=None, path_to_schema=None, buffer_creation=10):
    if not client:
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    try:
        table = client.get_table(table_id)
        print(
            "Loaded table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    except exceptions.NotFound:
        if not path_to_schema:
            raise ValueError(f"Table:{table_id} for found, please specify a schema to create it.")

        table_schema = get_table_schema(path_to_schema=path_to_schema)
        table = bigquery.Table(table_id, schema=table_schema)
        table = client.create_table(table)
        # sleep for buffer between creation and potential push of records
        time.sleep(buffer_creation)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    return table


def push_records_to_bq(bq_records, table_id, batch_size=1000, client=None, verbose=config.VERBOSE, path_to_schema=None):
    if not client:
        # Construct a BigQuery client object.
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Check table exists or create one
    _ = load_table(table_id=table_id, client=client, path_to_schema=path_to_schema)

    # Insert rows in table
    failed_to_push = []
    if len(bq_records) < batch_size:
        errors = client.insert_rows_json(table_id, bq_records)
        if len(errors) != 0:
            print("Encountered errors while inserting rows: {}".format(errors))
            failed_to_push += bq_records
    else:
        for i in tqdm(range(0, len(bq_records), batch_size)):
            to_push = bq_records[i:i + batch_size]
            errors = client.insert_rows_json(table_id, to_push)
            if len(errors) != 0:
                print("Encountered errors while inserting rows: {}".format(errors))
                failed_to_push += to_push
    return failed_to_push

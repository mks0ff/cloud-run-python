# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from datetime import date
from unittest.mock import patch

import flask
from flask import json
from flask.testing import FlaskClient


def test_get_index(app: flask.app.Flask, client: FlaskClient) -> None:
    res = client.get("/")
    assert res.status_code == 200


def test_post_index(app: flask.app.Flask, client: FlaskClient) -> None:
    res = client.post("/")
    assert res.status_code == 405


@patch("app.compose_file")
@patch("app.bq_header")
@patch("app.list_file")
@patch("app.bq_export")
@patch("app.get_bigquery_client")
@patch("app.get_gcs_client")
def test_post_export(get_gcs_client, get_bigquery_client, bq_export, list_file, bq_header, compose_file,
                     app: flask.app.Flask,
                     client: FlaskClient) -> None:
    # initialise json input data
    json_input = {
        "project": "boom-1153",
        "bucket": "boom-bada-boom-bucket",
        "location": "us-central1",
        "with_header": "false"
    }

    # Random IDs for post /export/dataset_id/table_id
    dataset_id = "dataset_id"
    table_id = "table_id"

    # Create test variables
    file_name: str = "export-{}-{}".format(table_id, date.today().isoformat())
    temp_file_prefix: str = f'{dataset_id}/tmp/{table_id}/partition'
    temp_destination_uri: str = f'gs://{json_input["bucket"]}/{dataset_id}/tmp/{table_id}/partition*.csv'
    file_path: str = f'{dataset_id}/{file_name}.csv'
    file_uri: str = "gs://{}/{}".format(json_input["bucket"], file_path)
    list_file.return_value = [x for x in range(100)]
    bq_header.return_value = ['header1', 'header_2']

    # Get cloud clients
    storage_client = get_gcs_client.return_value
    bigquery_client = get_bigquery_client.return_value

    # Test POST API
    response = client.post(f"/export/{dataset_id}/{table_id}", json=json_input)
    data = json.loads(response.data)

    # Assertions
    get_gcs_client.assert_called_once_with()
    get_bigquery_client.assert_called_once_with()
    bq_export.assert_called_once_with(json_input["project"], dataset_id, table_id, json_input["location"],
                                      temp_destination_uri, bigquery_client)
    list_file.assert_called_once_with(json_input["bucket"], temp_file_prefix, storage_client)
    bq_header.assert_called_once_with(json_input["project"], dataset_id, table_id, bigquery_client)
    compose_file.assert_called_once_with(file_uri, list_file(), storage_client, bq_header())

    assert response.status_code == 200
    assert data["status"] == 200
    assert data["path"] == file_uri


def test_export_is_no_json(client: FlaskClient):
    table_id = "table_id"
    dataset_id = "dataset_id"
    response = client.post(f"/export/{dataset_id}/{table_id}")  # Missing Json
    data = json.loads(response.data)
    assert response.status_code == 400
    assert data["status"] == 400
    assert data["error"] == "Content-Type must be application/json"

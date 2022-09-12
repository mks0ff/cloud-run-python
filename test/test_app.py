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

from os import environ

import flask

from flask import json
from flask.testing import FlaskClient


def test_get_index(app: flask.app.Flask, client: FlaskClient) -> None:
    res = client.get("/")
    assert res.status_code == 200


def test_post_index(app: flask.app.Flask, client: FlaskClient) -> None:
    res = client.post("/")
    assert res.status_code == 405


def test_post_export(app: flask.app.Flask, client: FlaskClient) -> None:
    # TODO mock bigquery and storage clients
    res = client.post("/export/covid/covid19_open_data", json={
        "project" : "boom-1153",
        "bucket" : "boom-bada-boom-bucket",
        "location" : "us-central1",
        "with_header": "false"
    })
    data = json.loads(res.data)

    assert res.status_code == 200
    assert data["status"] == 200

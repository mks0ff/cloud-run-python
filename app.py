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

import signal
import sys
import traceback
from datetime import date
from types import FrameType

from flask import Flask, jsonify, make_response, request
from google.api_core.exceptions import BadRequest
from google.cloud.exceptions import NotFound

from utils.bigquery import bq_export, bq_header, get_bigquery_client
from utils.compose import compose_file, list_file, get_gcs_client
from utils.logging import logger

app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True


@app.errorhandler(NotFound)
def handle_exception(err):
    """Handler missing file for composing"""
    logger.error(f"ValueError: {str(err)}")
    logger.debug(''.join(traceback.format_exception(type(err), value=err, tb=err.__traceback__)))
    response = {"error": "URI,Location, or Project is wrong"}
    return jsonify(response), 404


@app.errorhandler(500)
def handle_exception(err):
    """Return JSON instead of HTML for any other server error"""
    logger.error(f"Unknown Exception: {str(err)}")
    logger.debug(''.join(traceback.format_exception(type(err), value=err, tb=err.__traceback__)))
    response = {"error": "Sorry, internal error, please check logs"}
    return jsonify(response), 500


@app.errorhandler(ValueError)
def handle_exception(err):
    """Handler missing file for composing"""
    logger.error(f"ValueError: {str(err)}")
    logger.debug(''.join(traceback.format_exception(type(err), value=err, tb=err.__traceback__)))
    response = {"error": "file uri not found"}
    return jsonify(response), 400


@app.errorhandler(BadRequest)
def handle_exception(err):
    """Handler missing file for composing"""
    logger.error(f"ValueError: {str(err)}")
    logger.debug(''.join(traceback.format_exception(type(err), value=err, tb=err.__traceback__)))
    response = {"error": str(err)}
    return jsonify(response), 400


@app.errorhandler(400)
@app.route("/export/<dataset_id>/<table_id>", methods=["POST"])
def export(dataset_id: str, table_id: str) -> str:
    """
    :param dataset_id:
    :param table_id:
    :return:
    """
    if not request.is_json:
        response = {
            "status": 400,
            "error": "Content-Type must be application/json",
        }
        return make_response(jsonify(response), 400)

    logger.info("Starting export bq:{}/{}".format(dataset_id, table_id))

    # config
    project: str = request.json.get("project", "cel-em-prj-dpf-shr-01-dev")
    bucket: str = request.json.get("bucket", "cel-em-gcs-dpf-shr-01-dev")
    location: str = request.json.get("location", "europe-west1")
    folder: str = request.json.get("output", dataset_id)
    file_name: str = request.json.get("file_name", "export-{}-{}".format(table_id, date.today().isoformat()))
    with_header: bool = request.json.get("with_header", False)
    temp_file_prefix: str = f'{folder}/tmp/{table_id}/partition'
    temp_destination_uri: str = f'gs://{bucket}/{folder}/tmp/{table_id}/partition*.csv'
    file_path: str = f'{folder}/{file_name}.csv'
    file_uri: str = "gs://{}/{}".format(bucket, file_path)

    logger.info("Payload : {}".format(request.json))
    logger.info("Filename : {}.csv".format(file_name))
    logger.info("With Header : {}".format(with_header))
    # Get Cloud Clients
    storage_client = get_gcs_client()
    bigquery_client = get_bigquery_client()

    bq_export(project, dataset_id, table_id, location, temp_destination_uri, bigquery_client)
    blobs = list_file(bucket, temp_file_prefix, storage_client)
    header = bq_header(project, dataset_id, table_id, bigquery_client)

    final_result = compose_file(file_uri, blobs, storage_client, header)

    logger.info("final result : {}".format(final_result.path))

    response = {
        "status": 200,
        "path": file_uri,
    }

    return jsonify(response)


@app.route("/")
def hello() -> str:
    # Use basic logging with custom fields
    logger.info(logField="custom-entry", arbitraryField="custom-entry")

    # https://cloud.google.com/run/docs/logging#correlate-logs
    logger.info("Child logger with trace Id.")

    return "Hello, World!"


def shutdown_handler(signal_int: int, frame: FrameType) -> None:
    logger.info(f"Caught Signal {signal.strsignal(signal_int)}")

    from utils.logging import flush

    flush()

    # Safely exit program
    sys.exit(0)


if __name__ == "__main__":
    # Running application locally, outside of a Google Cloud Environment

    # handles Ctrl-C termination
    signal.signal(signal.SIGINT, shutdown_handler)

    app.run(host="0.0.0.0", port=8080, debug=True)
else:
    # handles Cloud Run container termination
    signal.signal(signal.SIGTERM, shutdown_handler)

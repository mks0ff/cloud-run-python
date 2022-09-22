from unittest.mock import patch, PropertyMock

import pytest
from google.api_core.exceptions import NotFound, BadRequest
from google.cloud.bigquery import SchemaField

from utils.bigquery import bq_export, get_bigquery_client, bq_header


@patch("utils.bigquery.bigquery")
def test_get_bigquery_client(bigquery):
    return_client = bigquery.Client.return_value
    client = get_bigquery_client()
    bigquery.Client.assert_called_once()
    assert client == return_client


def test_get_gcs_client_exception():
    with pytest.raises(Exception) as e:
        get_bigquery_client()


@patch("utils.bigquery.bigquery")
def test_bq_export(bigquery):
    project = "project_id"
    dataset_id = "dataset_id"
    table_id = "table_id"
    location = "us"
    bucket = "bucket"
    bigquery_client = bigquery.Client()
    temp_destination_uri = f'gs://{bucket}/{dataset_id}/tmp/{table_id}/partition*.csv'
    bq_export(project, dataset_id, table_id, location, temp_destination_uri, bigquery_client)
    bigquery.DatasetReference.assert_called_once_with(project, dataset_id)
    dataset_ref = bigquery.DatasetReference.return_value
    dataset_ref.table.assert_called_once_with(table_id)
    table_ref = dataset_ref.table.return_value
    bigquery.job.ExtractJobConfig.assert_called_once()
    job_config = bigquery.job.ExtractJobConfig.return_value
    job_config.print_header = False
    bigquery.Client().extract_table.assert_called_once_with(table_ref, temp_destination_uri, location=location,
                                                            job_config=job_config)


@patch("utils.bigquery.bigquery")
def test_export_files_exception(bigquery):
    project = "project_id"
    dataset_id = "dataset_id"
    table_id = "table_id"
    location = "us"
    bucket = "bucket"
    temp_destination_uri = f'gs://{bucket}/{dataset_id}/tmp/{table_id}/partition*.csv'
    bigquery_client = bigquery.Client()
    # Assign our mock response as the result of our patched function
    bigquery_client.extract_table.side_effect = NotFound("NotFound")
    with pytest.raises(NotFound) as exception:
        bq_export(project, dataset_id, table_id, location, temp_destination_uri, bigquery_client)
    assert exception.value.message == "NotFound"


@patch("utils.bigquery.bigquery")
def test_export_bad_request_exception(bigquery):
    project = "project_id"
    dataset_id = "dataset_id"
    table_id = "table_id"
    location = "us"
    bucket = "bucket"
    temp_destination_uri = f'gs://{bucket}/{dataset_id}/tmp/{table_id}/partition*.csv'
    bigquery_client = bigquery.Client()
    # Assign our mock response as the result of our patched function
    bigquery_client.extract_table.side_effect = BadRequest("BadRequest")
    with pytest.raises(BadRequest) as exception:
        bq_export(project, dataset_id, table_id, location, temp_destination_uri, bigquery_client)
    assert exception.value.message == "BadRequest"


@patch("utils.bigquery.bigquery")
def test_bq_header(bigquery):
    project = "project_id"
    dataset_id = "dataset_id"
    table_id = "table_id"

    bigquery_client = bigquery.Client()

    dataset_ref = bigquery.DatasetReference.return_value
    table_ref = dataset_ref.table.return_value
    table = bigquery_client.get_table.return_value
    with patch('google.cloud.bigquery.SchemaField', new_callable=PropertyMock) as filed_test_1:
        filed_test_1 = SchemaField(name='header1', field_type="")
    with patch('google.cloud.bigquery.SchemaField', new_callable=PropertyMock) as filed_test_2:
        filed_test_2 = SchemaField(name='header2', field_type="")
    table.schema = [filed_test_1, filed_test_2]

    header = bq_header(project, dataset_id, table_id, bigquery_client)
    bigquery.DatasetReference.assert_called_once_with(project, dataset_id)
    dataset_ref = bigquery.DatasetReference.return_value
    dataset_ref.table.assert_called_once_with(table_id)
    bigquery_client.get_table.assert_called_once_with(table_ref)
    assert header == ["header1", "header2"]

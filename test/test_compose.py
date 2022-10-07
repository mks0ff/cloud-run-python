import concurrent.futures
import io
from unittest.mock import patch

import pytest

from configuration.exceptions import ValidationException
from utils.compose import get_gcs_client, list_file, generate_chunks, write_initial_file_with_header, \
    delete_objects_concurrent, compose_file


def test_get_gcs_client_exception():
    with pytest.raises(Exception) as exception:
        get_gcs_client()


@patch("utils.compose.storage")
def test_get_files_from_bucket(storage):
    client = storage.client()
    client.list_blobs.return_value = [1, 2]

    blob_list = list_file("bucket", "prefix", client)

    client.list_blobs.assert_called_with('bucket', prefix='prefix')

    assert blob_list == [1, 2]


@patch("utils.compose.storage")
def test_get_files_from_bucket_exception(storage):
    client = storage.Client.return_value
    with pytest.raises(ValidationException) as exception:
        list_file("bucket", "", client)

    client.list_blobs.not_assert_called_with('bucket', prefix='prefix')
    assert exception.value.__str__() == "File Prefix was not specified"


@patch("utils.compose.storage")
def test_get_gcs_client(storage):
    return_client = storage.Client.return_value
    client = get_gcs_client()
    storage.Client.assert_called_once()
    assert client == return_client


def test_generate_chunks():
    list_object_test_list = range(6)
    expected_generated_chunks = [range(3), range(3, 6)]
    generated_chunks = generate_chunks(list_object_test_list, 3)
    assert generated_chunks == expected_generated_chunks


@patch("utils.compose.io")
@patch("utils.compose.storage")
def test_write_initial_file_with_header(storage, io_string):
    storage_client = storage.Client.return_value
    file_uri = "gs://bucket/file.csv"
    header = ['header1', 'header2']
    final_blob = storage.Blob.from_string.return_value
    blob_with_header = io_string.StringIO.return_value
    blob = write_initial_file_with_header(file_uri, header, storage_client)
    storage.Blob.from_string.assert_called_once_with(file_uri)
    final_blob.upload_from_file.assert_called_once_with(blob_with_header, content_type='text/csv',
                                                        client=storage_client)
    assert blob == final_blob


@patch("utils.compose.io")
@patch("utils.compose.storage")
def test_write_initial_file_without_header(storage, io_bytes):
    storage_client = storage.Client.return_value
    file_uri = "gs://bucket/file.csv"
    header = list()
    final_blob = storage.Blob.from_string.return_value
    empty_file = io_bytes.BytesIO.return_value
    blob = write_initial_file_with_header(file_uri, header, storage_client)
    storage.Blob.from_string.assert_called_once_with(file_uri)
    final_blob.upload_from_file.assert_called_once_with(empty_file, client=storage_client)
    assert blob == final_blob


@patch("utils.compose.storage")
def test_write_initial_file__with_header_exception(storage):
    storage_client = storage.Client.return_value
    storage.Blob.from_string.side_effect = ValueError("URI scheme must be gs")

    file_uri = "gs://bucket/file.csv"
    header = ['header1', 'header2']
    final_blob = storage.Blob.from_string.return_value
    with pytest.raises(ValidationException) as exception:
        blob = write_initial_file_with_header(file_uri, header, storage_client)
    assert exception.value.__str__() == "Failed to upload blob to GCS"



@patch("utils.compose.storage")
def test_de_objects_concurrent(storage):
    blobs = [storage.Blob('file1'), storage.Blob('file2')]
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    storage_client = storage.Client.return_value
    delete_objects_concurrent(blobs, executor, storage_client)
    blobs[0].delete.assert_called_with(client=storage_client)
    blobs[1].delete.assert_called_with(client=storage_client)


@patch("utils.compose.write_initial_file_with_header")
@patch("utils.compose.storage")
def test_compose_file(storage, write_initial_file_with_header):
    file_uri = "gs://test/bucket/file.csv"
    list_object = [storage.Blob('file1'), storage.Blob('file2')]
    gcs_client = storage.Client.return_value
    header = ["header1", "header2"]
    final_blob_test = write_initial_file_with_header.return_value = storage.Blob()
    final_blob = compose_file(file_uri, list_object, gcs_client,
                              header)
    write_initial_file_with_header.assert_called_once_with(file_uri, header, gcs_client)
    final_blob_test.compose.assert_called_once()
    assert final_blob == final_blob_test


@patch("utils.compose.storage")
def test_compose_file_empty_list(storage):
    file_uri = "gs://test/bucket/file.csv"
    list_object = []
    gcs_client = storage.Client.return_value
    header = ["header1", "header2"]
    with pytest.raises(ValidationException) as exception:
        final_blob = compose_file(file_uri, list_object, gcs_client,
                                  header)
    assert exception.value.__str__() == "File not found"

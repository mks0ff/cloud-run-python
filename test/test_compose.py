from unittest.mock import patch

import pytest

from utils.compose import get_gcs_client, list_file


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
    client = storage.client()
    with pytest.raises(ValueError) as exception:

        list_file("bucket", "", client)

    client.list_blobs.not_assert_called_with('bucket', prefix='prefix')
    assert exception == "File Prefix was not specified"


@patch("utils.compose.storage")
def test_get_gcs_client(storage):
    return_client = storage.Client.return_value
    client = get_gcs_client()
    storage.Client.assert_called_once()
    assert client == return_client

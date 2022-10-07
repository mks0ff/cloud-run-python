import concurrent.futures
import io
from time import sleep
from typing import List

from google.cloud import storage

from configuration.exceptions import ValidationException
from utils.logging import logger


def get_gcs_client() -> storage.Client:
    try:
        client = storage.Client()
        return client
    except Exception as e:
        logger.error("Error creating client: \n\t{}".format(e))
        raise ValidationException("Error creating GCS Client")


def list_file(bucket: str, file_prefix: str, gcs_client: storage.Client) -> List[storage.Blob]:
    """
    :param bucket: Google Cloud Storage Bucket
    :param file_prefix: Prefix
    :param gcs_client: Google Cloud Storage Client
    :return: List of files in the specific bucket
    """

    logger.info('Listing files {} ...'.format(bucket))

    if file_prefix == "":
        raise ValidationException("File Prefix was not specified")

    return [blob for blob in gcs_client.list_blobs(bucket, prefix=file_prefix)]


def generate_chunks(list_object: List, max_partitions: int = 31) -> List:
    """
    :param list_object: list containing CSV files generated by bigquery
    :param max_partitions: the number of files in a chunk (default 31)
    :return: List of chunks
    """
    return [list_object[i:i + max_partitions] for i in range(0, len(list_object), max_partitions)]


def write_initial_file_with_header(file_uri: str, header: List, gcs_client: storage.Client) -> storage.Blob:
    """
    :param file_uri: the uri of the file
    :param header: headers of the bigquery table if any else empty list
    :param gcs_client: google cloud storage client
    :return: blob file
    """
    try:
        final_blob = storage.Blob.from_string(file_uri)
        if not header:
            final_blob.upload_from_file(io.BytesIO(b''), client=gcs_client)
        else:
            header_string = f"{','.join(header)} \n"
            header_string_io = io.StringIO(header_string)
            final_blob.upload_from_file(header_string_io, content_type='text/csv', client=gcs_client)
        return final_blob
    except Exception as e:
        logger.error("Failed to upload blob : {}".format(e))
        raise ValidationException("Failed to upload blob to GCS")


def compose_file(file_uri: str, list_object: List[storage.Blob], gcs_client: storage.Client,
                 header: List[str] = None) -> storage.Blob:
    """
    :param file_uri: the path of file
    :param list_object: list containing chunks of files
    :param header: the header of the dataset
    :param gcs_client: Google Cloud Storage Client
    :return: the composed csv file with the header
    """
    logger.info("Start composing files...")

    if not list_object:
        raise ValidationException('File not found')

    chunks = generate_chunks(list_object=list_object)
    logger.info("Partitioned list of files into {} slices.".format(len(chunks)))
    final_blob = write_initial_file_with_header(file_uri, header, gcs_client)
    logger.info("Destination file {}.".format(final_blob.name))
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

    for i, chunk in enumerate(chunks):
        chunk.insert(0, final_blob)
        logger.info("chunk {} size : {}".format(i, len(chunk)))
        logger.info("Composing csv(s) to one csv...")
        final_blob.compose(chunk, client=gcs_client)
        delete_objects_concurrent(chunk[1:], executor, storage_client=gcs_client)
        sleep(1)
    logger.info("End composing files.")
    # cleanup and exit
    executor.shutdown(True)
    return final_blob


def delete_objects_concurrent(blobs: List[storage.Blob], executor: concurrent.futures.ThreadPoolExecutor,
                              storage_client: storage.Client) -> None:
    """
    Delete chunks of files asynchronously
    :param blobs:  List of csv files to delete
    :param executor: Multithread Pool Executor
    :param storage_client: Google Cloud Storage Client
    :return: None
    """
    for blob in blobs:
        logger.debug("Deleting slice {}".format(blob.name))
        executor.submit(blob.delete, client=storage_client)
        sleep(.005)

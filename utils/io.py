
from asyncio import sleep
import concurrent.futures
import io

from typing import List

from google.cloud import storage

from utils.logging import logger


GCS_CLIENT: storage.Client = storage.Client()


def list_file(bucket: str, file_prefix: str) -> List[storage.Blob]:

    logger.info('Listing files {} ...'.format(bucket))
    
    if file_prefix == "":
        return

    return [blob for blob in GCS_CLIENT.list_blobs(bucket, prefix=file_prefix)]


def compose_file(file_uri: str, list_object: List[storage.Blob], header: List[str] = []) -> storage.Blob:

    logger.info("Start composing files...")

    if list_object == []:
        return

    max_partitions = 31
    chunks = [list_object[i:i + max_partitions] for i in range(0, len(list_object), max_partitions)]

    logger.info("Partitioned list of files into {} slices.".format(len(chunks)))

    final_blob = storage.Blob.from_string(file_uri)
    if header == []:
        final_blob.upload_from_file(io.BytesIO(b''), client=GCS_CLIENT) 
    else:
        final_blob.upload_from_file(io.StringIO(", ".join(header) + "\n"), client=GCS_CLIENT) 

    logger.info("Destination file {}.".format(final_blob.name))
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

    for i, chunk in enumerate(chunks):
        chunk.insert(0, final_blob)
        logger.info("chunk {} size : {}".format(i, len(chunk)))
        logger.info("Composing csv(s) to one csv...")
        final_blob.compose(chunk, client=GCS_CLIENT)
        delete_objects_concurrent(chunk[1:], executor, client=GCS_CLIENT)

    logger.info("End composing files.")
    return final_blob


def delete_objects_concurrent(blobs: List[storage.Blob], executor: concurrent.futures.ThreadPoolExecutor, client: storage.Client) -> None:

    for blob in blobs:
        logger.debug("Deleting slice {}".format(blob.name))
        executor.submit(blob.delete, client=client)
        sleep(.005)

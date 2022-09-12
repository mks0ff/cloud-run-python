from email import header
from typing import List
from xmlrpc.client import Boolean
from google.cloud import bigquery

from utils.logging import logger

BQ_CLIENT: bigquery.Client = bigquery.Client()


def bq_export(project: str, dataset_id: str, table_id: str, location: str, destination_uri: str) -> None:

    logger.info("Start BQ table export...")

    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    job_config = bigquery.job.ExtractJobConfig()
    job_config.print_header = False

    logger.info("Extracting {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri))

    extract_job = BQ_CLIENT.extract_table(
        table_ref,
        destination_uri,
        location=location,
        job_config=job_config
    )

    extract_job.result()
    logger.info("End BQ table export.")


def bq_header(project: str, dataset_id: str, table_id: str) -> List[str]:

    logger.info("Start BQ get schema header...")

    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    table = BQ_CLIENT.get_table(table_ref)
    header = ["{}".format(schema.name) for schema in table.schema]

    logger.info("Schema header {}".format(header))
    return header


from typing import List

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from utils.logging import logger


def get_bigquery_client() -> bigquery.Client:
    try:
        return bigquery.Client()
    except Exception as e:
        logger.error("Error creating client: \n\t{}".format(e))
        raise


def bq_export(project: str, dataset_id: str, table_id: str, location: str, destination_uri: str,
              bq_client: bigquery.Client) -> None:
    """
    :param project: The id of the project
    :param dataset_id: the dataset id in bigquery
    :param table_id: the table id in the dataset
    :param location: the location of the dataset
    :param destination_uri: the uri of the bucket
    :return: None
    """
    logger.info("Start BQ table export...")

    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.job.ExtractJobConfig()
    job_config.print_header = False

    logger.info("Extracting {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri))
    try:
        extract_job = bq_client.extract_table(
            table_ref,
            destination_uri,
            location=location,
            job_config=job_config
        )
        extract_job.result()
        logger.info("End BQ table export.")
    except NotFound as e:
        logger.exception(e, exc_info=True)
        raise

    except BadRequest as e:
        logger.exception(e, exc_info=True)
        raise


def bq_header(project: str, dataset_id: str, table_id: str, bq_client: bigquery.Client) -> List[str]:
    """
    :param project: The id of the project
    :param dataset_id: the dataset id in bigquery
    :param table_id: the table id in the dataset
    :return: List containing the header of the table
    """

    logger.info("Start BQ get schema header...")

    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    table = bq_client.get_table(table_ref)
    header = ["{}".format(schema.name) for schema in table.schema]

    logger.info("Schema header {}".format(header))
    return header

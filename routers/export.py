from datetime import date

from fastapi import Depends, APIRouter

from schema.gcp_details import ProjectDetails, ApiResponse
from utils.bigquery import bq_export, bq_header, get_bigquery_client
from utils.compose import list_file, compose_file, get_gcs_client
from utils.logging import logger

router = APIRouter()

responses = {
    400: {"description": "Validation Exception",
          "content": {
              "application/json": {
                  "example": {"status": 400, "path": "Dataset not found,please check the input parameters"}
              }},
          },
    200: {"description": "Dataset exported correctly",
          "content": {
              "application/json": {
                  "example": {"status": 200, "path": "gs://bucket/dataset_id/export-table_id-2022-10-07.csv"}
              }},
          }}


@router.post("/export/{dataset_id}/{table_id}", responses={**responses}, name="export", response_model=ApiResponse)
def p(dataset_id: str, table_id: str, body: ProjectDetails

      ) -> ApiResponse:
    """
    """

    temp_file_prefix: str = f'{dataset_id}/tmp/{table_id}/partition'
    temp_destination_uri: str = f'gs://{body.bucket}/{dataset_id}/tmp/{table_id}/partition*.csv'
    file_name = "export-{}-{}".format(table_id, date.today().isoformat())
    file_path: str = f'{dataset_id}/{file_name}.csv'
    file_uri: str = "gs://{}/{}".format(body.bucket, file_path)

    logger.info("Payload : {}".format(body.json()))
    logger.info("Filename : {}.csv".format(file_name))
    logger.info("With Header : {}".format(body.with_header))
    # Get Cloud Clients
    storage_client = get_gcs_client()
    bigquery_client = get_bigquery_client()

    bq_export(body.project, dataset_id, table_id, body.location, temp_destination_uri, bigquery_client)
    blobs = list_file(body.bucket, temp_file_prefix, storage_client)
    header = bq_header(body.project, dataset_id, table_id, bigquery_client)

    final_result = compose_file(file_uri, blobs, storage_client, header)

    logger.info("final result : {}".format(final_result.path))
    return ApiResponse(status=200, path=file_uri)

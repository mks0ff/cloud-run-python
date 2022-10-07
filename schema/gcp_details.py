from pydantic import BaseModel, Field


class ProjectDetails(BaseModel):
    project: str = Field(description="The GCP Project")
    bucket: str = Field(description="The Target Bucket")
    location: str = Field("us", description="The location of the dataset")
    with_header: bool = Field(default=False,
                              description="False: Save without header, True add the header to every partition")


class ApiResponse(BaseModel):
    status: int = Field(default=200, description="Responde code. 200: Ok")
    path: str = Field(description="The file uri returned by the API")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette import status
from starlette.responses import RedirectResponse, JSONResponse
from starlette.requests import Request

from configuration.exceptions import ValidationException
from routers import export

app = FastAPI(
    title="FastAPI - Data Sharing",
    description=""
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)




@app.get("/", name="home")
def main():
    return RedirectResponse(url="/docs/")


app.include_router(
    export.router,
    tags=["export"],
)


@app.exception_handler(ValidationException)
async def validation_exception_handler(request: Request, exc: ValidationException) -> JSONResponse:
    """
    Custom exception handler for ValidationException
    This returns a JSON format message.
    :param request:
    :param exc:
    :return:
    """
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=str(exc),
    )


@app.exception_handler(Exception)
async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Default exception handler.
    This returns a JSON format message.
    :param request:
    :param exc:
    :return:
    """
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content="Sorry, internal error, please check logs",
    )

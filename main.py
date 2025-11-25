import os
from urllib.parse import urlparse, unquote

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
from azure.storage.blob import BlobServiceClient, ContentSettings

# -------------------------
# Config – set these in App Service app settings
# -------------------------
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "my-public-container")

if not AZURE_STORAGE_CONNECTION_STRING:
    raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set in environment variables.")

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_STORAGE_CONTAINER_NAME)

# -------------------------
# FastAPI app with Swagger / OpenAPI metadata
# -------------------------
app = FastAPI(
    title="CSV to Azure Blob Uploader",
    description="""
API that accepts a CSV file URL, downloads the CSV, and uploads it to a public Azure Blob Storage container.

**How it works:**
- Provide a public `csv_url` (HTTP/HTTPS)
- The API downloads the CSV
- Uploads it to the configured blob container using your connection string
- Returns the blob name and public blob URL

Use `/docs` for Swagger UI and `/redoc` for ReDoc.
""",
    version="1.0.0",
    contact={
        "name": "Starlink Business 360 - Tools",
        "email": "support@example.com",
    },
)


class CsvUploadRequest(BaseModel):
    csv_url: HttpUrl


class CsvUploadResponse(BaseModel):
    message: str
    blob_name: str
    blob_url: str


def get_blob_name_from_url(csv_url: str) -> str:
    """
    Derive a blob name from the CSV URL.
    Example: https://host/path/data/file1.csv -> file1.csv
    """
    parsed = urlparse(csv_url)
    filename = os.path.basename(parsed.path)
    filename = unquote(filename)  # handle %20 etc.
    if not filename:
        filename = "uploaded.csv"
    return filename


@app.post(
    "/upload-csv",
    response_model=CsvUploadResponse,
    summary="Upload CSV from URL to Blob",
    description="""
Downloads a CSV from the given URL and uploads it to the configured Azure Blob container.

- **csv_url**: Public URL of the CSV file (HTTP/HTTPS)
- Uploads with `content-type: text/csv`
- Overwrites existing blob with the same name
    """,
    tags=["CSV Upload"],
)
def upload_csv(body: CsvUploadRequest):
    csv_url = str(body.csv_url)

    # 1. Download the CSV
    try:
        resp = requests.get(csv_url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as ex:
        raise HTTPException(status_code=400, detail=f"Failed to download CSV from URL: {ex}")

    if not resp.content:
        raise HTTPException(status_code=400, detail="Downloaded CSV is empty.")

    csv_bytes = resp.content

    # 2. Choose blob name
    blob_name = get_blob_name_from_url(csv_url)

    # 3. Upload to blob container
    try:
        container_client.upload_blob(
            name=blob_name,
            data=csv_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type="text/csv"),
        )
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Failed to upload blob: {ex}")

    blob_url = f"{container_client.url}/{blob_name}"

    return CsvUploadResponse(
        message="CSV uploaded successfully.",
        blob_name=blob_name,
        blob_url=blob_url,
    )


# Local run (for testing). In Azure App Service, configure the startup command.
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000)

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

app = FastAPI(title="CSV Uploader to Blob")


class CsvUploadRequest(BaseModel):
    csv_url: HttpUrl


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


@app.post("/upload-csv")
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

    return {
        "message": "CSV uploaded successfully.",
        "blob_name": blob_name,
        "blob_url": blob_url,
    }


# Local run (for testing). In Azure App Service, you usually configure the startup command.
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000)

import os
import logging
import datetime
import azure.functions as func
from azure.storage.blob import BlobServiceClient

# Define your source accounts (name, env var for connection string, and container)
SOURCES = [
    {
        "name": os.getenv("STORAGE_ACCOUNT_1", "stdevtest053"),
        "conn_var": os.getenv("SOURCE_CONN_1"),
        "container": os.getenv("SOURCE_CONTAINER_1", "tfstate")
    },
    {
        "name": os.getenv("STORAGE_ACCOUNT_2", "stprodtest053"),
        "conn_var": os.getenv("SOURCE_CONN_2"),
        "container": os.getenv("SOURCE_CONTAINER_2", "tfstate")
    }
]

DEST_CONN = os.getenv("DEST_CONN")
DEST_CONTAINER = os.getenv("DEST_CONTAINER", "tfstate-backup")

def main(myTimer: func.TimerRequest) -> None:
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d")
    logging.info(f"Starting blob backup: {timestamp}")

    try:
        dest_service = BlobServiceClient.from_connection_string(DEST_CONN)
        dest_container_client = dest_service.get_container_client(DEST_CONTAINER)
    except Exception as e:
        logging.error(f"Failed to connect to destination storage: {str(e)}")
        return

    for source in SOURCES:
        source_conn = os.getenv(source["conn_var"])
        if not source_conn:
            logging.warning(f"Missing connection string for {source['name']}")
            continue

        try:
            source_service = BlobServiceClient.from_connection_string(source_conn)
            source_container = source_service.get_container_client(source["container"])
        except Exception as e:
            logging.error(f"Failed to connect to source {source['name']}: {str(e)}")
            continue

        for blob in source_container.list_blobs():
            if blob.name.endswith(".tfstate"):
                try:
                    source_blob = source_container.get_blob_client(blob.name)
                    new_blob_name = f"{source['name']}/{blob.name.replace('.tfstate', '')}-{timestamp}.tfstate"
                    dest_blob = dest_container_client.get_blob_client(new_blob_name)

                    dest_blob.start_copy_from_url(source_blob.url)
                    logging.info(f"Copied: {blob.name} → {new_blob_name}")
                except Exception as e:
                    logging.error(f"Copy failed for {blob.name}: {str(e)}")

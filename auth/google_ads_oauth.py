from google_auth_oauthlib.flow import InstalledAppFlow
from google.cloud import secretmanager
import json
import logging

SCOPES = ["https://www.googleapis.com/auth/adwords"]

def bootstrap_oauth(
    project_id: str,
    secret_id: str,
    client_secret_file: str
):
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secret_file,
        scopes=SCOPES
    )

    credentials = flow.run_local_server(port=8080)

    payload = {
        "developer_token": input("Developer Token: "),
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "refresh_token": credentials.refresh_token,
        "login_customer_id": input("Login Customer ID (MCC): ")
    }

    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}/secrets/{secret_id}"

    client.add_secret_version(
        parent=parent,
        payload={"data": json.dumps(payload).encode("utf-8")}
    )

    msg = ("✅ [AUTH] Google Ads OAuth credentials saved to Secret Manager.") 
    print(msg)
    logging.info(msg)